use super::*;
use crate::loader::find_method;

// 只用常驻库的静态 Descriptor 地址标识实际注册，不解引用整数。每张表内方法键唯一。
#[derive(PartialEq, Eq, Hash)]
pub(super) struct RegisteredRoute {
    sender: usize,
    receiver: usize,
    method: u32,
    raw_reply: bool,
}

#[derive(Clone, Copy)]
pub(super) enum Caller {
    Actor {
        key: u64,
        id: u64,
    },
    Port {
        key: u64,
        source: u64,
        target: u64,
        session: u64,
    },
}
pub(super) enum Stage {
    Open,
    Ready(Result<Payload, Status>),
    Offering,
    Delivered,
    Expired,
}
// 出口再次返回 Busy 可在提示回调内登记下一次等待，不能被旧提示的结算覆盖。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteInterest {
    None,
    Waiting,
    Ready,
    Offering,
}
impl WriteInterest {
    pub(super) fn notify(&mut self) {
        if *self == Self::Waiting {
            *self = Self::Ready;
        }
    }
}
pub(super) struct Pending {
    pub caller: Caller,
    // 实例与 peer 代次从同一不回绕的 ID 域预留；remote 非空时 callee 是链路键。
    pub callee: u64,
    pub remote: Option<peer::Binding>,
    pub method: Method,
    pub reply: ReplyBudget,
    pub deadline: Instant,
    pub reserve: Permit,
    pub stage: Stage,
    pub write: WriteInterest,
    pub reply_charge: u64,
    pub action: Option<LifecycleAction>,
}
impl Pending {
    pub(super) fn complete(
        &mut self,
        result: Result<Body<'_>, Status>,
        slot: &mut LocalSlot,
    ) -> Result<(), Status> {
        if !matches!(self.stage, Stage::Open) {
            return Err(Status::NOT_FOUND);
        }
        self.stage = Stage::Ready(match result {
            Err(e) => Err(e),
            Ok(body) => {
                let part = self.reserve.split(Payload::size(body, slot))?;
                Ok(Payload::take(body, slot, part, self.reply_charge)?)
            }
        });
        Ok(())
    }
    pub(super) fn owner(&self) -> Option<u64> {
        if let Caller::Actor { key, .. } = self.caller {
            Some(key)
        } else {
            None
        }
    }
}
fn ceiling(m: &Method, r: Representation) -> u64 {
    match r {
        Representation::Bytes => m.max_reply_bytes,
        Representation::LocalOwned => m.max_reply_local_bytes,
    }
}
impl Core {
    // 本机和远端共同的 caller 预留。token 只有在 mailbox 或 socket 出口接纳后才正式签发。
    pub(super) fn reserve_call(
        &self,
        s: &State,
        key: u64,
        callee: (u64, Method),
        r: Outbound<'_>,
        ids: &mut Ids,
    ) -> Result<Option<(u64, u64, Pending)>, Status> {
        if r.timeout_ms == 0 {
            return Ok(None);
        }
        let deadline = Instant::now() + self.duration(r.timeout_ms)?;
        if s.pending
            .values()
            .filter(|p| p.owner() == Some(key))
            .count()
            >= self.b.control_slots.get()
        {
            return Err(Status::BUSY);
        }
        let reserve = s
            .actor(key)?
            .budget
            .reserve(r.reply.ceiling, self.b.payload_bytes.get())?;
        let id = ids.issue()?;
        let token = ids.issue()?;
        Ok(Some((
            token,
            id,
            Pending {
                caller: Caller::Actor { key, id },
                callee: callee.0,
                method: callee.1,
                remote: None,
                reply: r.reply,
                deadline,
                reserve,
                stage: Stage::Open,
                write: WriteInterest::None,
                reply_charge: ids.charge(
                    r.reply.representation == Representation::Bytes && r.reply.ceiling != 0,
                )?,
                action: s.phase(key)?.action(),
            },
        )))
    }
    pub(super) fn select(
        &self,
        key: u64,
        dst: u64,
        wire: WireContract,
        mode: ReplyMode,
    ) -> Result<(Method, Method, [Representation; 2]), Status> {
        let s = lock(&self.state);
        if s.closing {
            return Err(Status::STOPPED);
        }
        if let Some(e) = s.phase(key)?.ending() {
            return Err(e);
        }
        if ServiceAddress::try_from(dst)
            .map_err(|_| Status::PROTOCOL)?
            .node()
            .get()
            != self.node
        {
            let method = self.peer_method(&s, key, dst, wire, mode)?;
            return Ok((method, method, [Representation::Bytes; 2]));
        }
        let dst = s.key(dst)?;
        s.running(dst)?;
        let src = s.actor(key)?;
        let sender = src.spec.implementation;
        let receiver = s.actor(dst)?.spec.implementation;
        let target = find_method(receiver, wire.key)?;
        if target.peer_control != PeerAccess::None {
            return Err(Status::DENIED);
        }
        let (from, route) = match find_method(sender, wire.key) {
            Ok(m) if m.directions & 2 != 0 => (
                m,
                Some(RegisteredRoute {
                    sender: std::ptr::from_ref(sender).addr(),
                    receiver: std::ptr::from_ref(receiver).addr(),
                    method: m.wire.key,
                    raw_reply: mode == ReplyMode::RawBytes,
                }),
            ),
            _ if src.spec.grants.player_entry
                && mode == ReplyMode::RawBytes
                && target.client_callable == 1 =>
            {
                // 通用玩家桥没有注册此发送方法；合成的纯字节声明不占资格表。
                (
                    Method {
                        directions: 2,
                        request_local: RNone,
                        response_local: RNone,
                        ..target
                    },
                    None,
                )
            }
            _ => return Err(Status::DENIED),
        };
        if wire != from.wire {
            return Err(Status::PROTOCOL);
        }
        if let Some(paths) = route.as_ref().and_then(|r| s.registered_routes.get(r)) {
            return Ok((from, target, *paths));
        }
        drop(s); // 布局兼容检查可以调用常驻库的布局访问器，不能占据宿主账本锁。
        #[cfg(test)]
        tests::trace(key, "select-declarations");
        let paths = from.select(&target, RouteScope::Local, mode)?;
        // 并发的首次比较可以重叠，不在库访问器外加初始化锁；只发布已核验的结果。
        let paths = if let Some(route) = route {
            *lock(&self.state)
                .registered_routes
                .entry(route)
                .or_insert(paths)
        } else {
            paths
        };
        Ok((from, target, paths))
    }
    pub(super) fn submit(
        &self,
        key: u64,
        r: Outbound<'_>,
        slot: &mut LocalSlot,
    ) -> Result<u64, Status> {
        if ServiceAddress::try_from(r.destination)
            .map_err(|_| Status::PROTOCOL)?
            .node()
            .get()
            != self.node
        {
            return self.peer_submit(key, r);
        }
        let raw = r.reply.representation == Representation::Bytes;
        let (from, target, paths) = self.select(
            key,
            r.destination,
            r.wire,
            if raw {
                ReplyMode::RawBytes
            } else {
                ReplyMode::Typed
            },
        )?;
        if let RSome(local) = slot
            && (local.is_charged()
                || !from
                    .request_local
                    .into_option()
                    .is_some_and(|t| t.matches(local.local_type())))
        {
            return Err(Status::PROTOCOL);
        }
        let mut s = lock(&self.state);
        // 快照之后可能已 stop；地址不可复用，复核两端后再预留、拿槽和提交。
        if s.closing {
            return Err(Status::STOPPED);
        }
        s.running(s.key(r.destination)?)?;
        let phase = s.phase(key)?;
        if let Some(e) = phase.ending() {
            return Err(e);
        }
        if phase != Phase::Running && phase.action().is_none() {
            return Err(Status::NOT_READY);
        }
        let a = s.actor(key)?;
        let src = a.address.ok_or(Status::NOT_READY)?.bits();
        if matches!(r.entry, Entry::Player { .. }) {
            if !a.spec.grants.player_entry || target.client_callable != 1 {
                return Err(Status::DENIED);
            }
        } else if !a
            .spec
            .implementation
            .methods
            .as_slice()
            .iter()
            .any(|m| m.wire == r.wire && m.directions & 2 != 0)
        {
            return Err(Status::DENIED);
        }
        if r.reply.representation != paths[1]
            || (r.body.representation != paths[0]
                && !(raw && r.body.representation == Representation::Bytes))
        {
            return Err(Status::PROTOCOL);
        }
        if r.reply.ceiling > ceiling(&from, paths[1]).min(ceiling(&target, paths[1])) {
            return Err(Status::TOO_SMALL);
        }
        let dst = s.key(r.destination)?;
        let account = a.budget.clone();
        let bytes = Payload::size(r.body, slot);
        let limit = self.b.payload_bytes.get();
        let mut ids = s.ids;
        let pending = self.reserve_call(&s, key, (dst, target), r, &mut ids)?;
        let mut permit = s.actor(dst)?.budget.reserve(bytes, limit)?;
        if dst != key {
            permit.sender = Some(Box::new(account.reserve(bytes, limit)?));
        }
        let token = pending.as_ref().map_or(0, |p| p.0);
        let charge = ids.charge(!r.body.bytes.is_empty())?;
        s.enqueue(
            dst,
            Envelope {
                source: src,
                token,
                wire: r.wire,
                entry: r.entry,
                reply: r.reply,
                timeout: r.timeout_ms,
                payload: Payload::queued(r.body, slot, permit, charge),
            },
            slot,
        )?;
        s.ids = ids;
        if let Some((token, id, p)) = pending {
            s.pending.insert(token, p);
            s.actor(key)?.signal.notify_one();
            Ok(id)
        } else {
            Ok(0)
        }
    }
    pub(super) fn reply(
        &self,
        key: u64,
        token: u64,
        r: Reply<'_>,
        slot: &mut LocalSlot,
    ) -> Result<(), Status> {
        if let RSome(local) = slot {
            let expected = {
                let s = lock(&self.state);
                let p = s.pending.get(&token).ok_or(Status::NOT_FOUND)?;
                if p.callee != key {
                    return Err(Status::DENIED);
                }
                p.method.response_local
            };
            if local.is_charged()
                || !expected
                    .into_option()
                    .is_some_and(|t| t.matches(local.local_type()))
            {
                return Err(Status::PROTOCOL);
            }
        }
        let mut s = lock(&self.state);
        if let Some(e) = s.phase(key)?.ending() {
            return Err(e);
        }
        let s = &mut *s;
        let p = s.pending.get_mut(&token).ok_or(Status::NOT_FOUND)?;
        if p.callee != key {
            return Err(Status::DENIED);
        }
        if !matches!(p.stage, Stage::Open) {
            return Err(Status::NOT_FOUND);
        }
        if p.deadline <= Instant::now() {
            s.finish_error(token, Status::TIMEOUT);
            return Err(Status::TIMEOUT);
        }
        let result = match r {
            Reply::Error(e) => Err(e),
            Reply::Success(b) => {
                if b.representation != p.reply.representation {
                    return Err(Status::PROTOCOL);
                }
                if Payload::size(b, slot) > p.reply.ceiling {
                    s.finish_error(token, Status::TOO_SMALL);
                    return Err(Status::TOO_SMALL);
                }
                Ok(b)
            }
        };
        match p.caller {
            Caller::Actor { key: caller, .. } => {
                p.complete(result, slot)?;
                s.actor(caller)?.signal.notify_one();
                Ok(())
            }
            Caller::Port {
                key: port,
                session,
                source,
                target,
            } => {
                let output = s.ports.get_mut(&port).ok_or(Status::UNREACHABLE)?;
                if !output.push(
                    target,
                    source,
                    session,
                    p.method.wire,
                    result.map(|b| b.bytes.as_slice()),
                ) {
                    #[cfg(test)]
                    super::tests::trace(key, "reply-busy");
                    p.write = WriteInterest::Waiting;
                    return Err(Status::BUSY);
                }
                s.pending.remove(&token);
                Ok(())
            }
        }
    }
}
impl State {
    pub(super) fn enqueue(
        &mut self,
        dst: u64,
        env: Envelope,
        slot: &mut LocalSlot,
    ) -> Result<(), Status> {
        let a = self.actor_mut(dst)?;
        // queued 包含因 task 满而保留在队首的一个 send；所有生产者与停止在此线性化。
        let pushed = if a.queued >= a.mailbox.capacity() {
            Err(env)
        } else {
            a.mailbox.push(env)
        };
        match pushed {
            Ok(()) => {
                a.queued += 1;
                a.signal.notify_one();
                Ok(())
            }
            Err(mut e) => {
                *slot = e.payload.local.take();
                Err(Status::BUSY)
            }
        }
    }
    pub(super) fn finish_error(&mut self, token: u64, e: Status) {
        let Some(p) = self.pending.get_mut(&token) else {
            return;
        };
        if !matches!(p.stage, Stage::Open) {
            return;
        }
        match p.caller {
            Caller::Actor { key, .. } => {
                p.stage = Stage::Ready(Err(e));
                if let Some(a) = self.actors.get(&key) {
                    a.signal.notify_one();
                }
            }
            Caller::Port {
                key,
                session,
                source,
                target,
            } => {
                if let Some(port) = self.ports.get_mut(&key) {
                    port.push(target, source, session, p.method.wire, Err(e));
                }
                self.pending.remove(&token);
            }
        }
    }
    pub(super) fn consume(&mut self, key: u64, id: u64) -> Result<(), Status> {
        let t = self
            .pending
            .iter()
            .find(|(_, p)| {
                matches!(p.caller,Caller::Actor{key:k,id:i} if k==key && i==id)
                    && matches!(p.stage, Stage::Offering | Stage::Delivered)
            })
            .map(|(&t, _)| t)
            .ok_or(Status::NOT_FOUND)?;
        self.pending.remove(&t);
        Ok(())
    }
}
