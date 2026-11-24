//! 已核定来源的有限字节入口/回复出口。节点 socket 与本机管理方使用同一接纳及背压账本。
use super::*;
use rpc::WriteInterest;
use std::collections::VecDeque;
/// 单个已绑定远端来源的字节调用；调用者必须是持有本机回复端口的管理适配器。
pub struct InboundCall<'a> {
    /// 远端地址，节点必须与端口绑定一致。
    pub source: ServiceAddress,
    /// 本机已发布实例。
    pub destination: ServiceAddress,
    /// 此端口代次上的非零会话。
    pub session: u64,
    /// 已经由适配器检查的字节契约，仍会复核目标声明。
    pub wire: WireContract,
    /// 输入字节，只在接纳前借用。
    pub bytes: &'a [u8],
    /// 按 wire 字节预留的最大成功回复。
    pub max_response_bytes: u64,
    /// 有限相对期限。
    pub timeout_ms: u64,
}
/// 一次被有限出口接纳的回复，不表示另一节点已经收到。
pub struct PortReply {
    /// 原目标实例，回包不能改作其它服务的响应。
    pub source: u64,
    /// 原调用方地址。
    pub destination: u64,
    /// 原端口会话。
    pub session: u64,
    /// 原方法。
    pub wire: WireContract,
    /// 成功字节或无体终态。
    pub body: Result<Box<[u8]>, Status>,
}
pub(super) struct Port {
    node: u16,
    entries: usize,
    limit: usize,
    used: usize,
    errors: usize,
    queue: VecDeque<PortReply>,
    socket: Option<Arc<Notify>>,
    pub(super) ready: Arc<std::sync::Condvar>,
}
impl Port {
    pub(super) fn new(
        node: u16,
        entries: usize,
        limit: usize,
        socket: Option<Arc<Notify>>,
    ) -> Self {
        Self {
            node,
            entries,
            limit,
            used: 0,
            errors: 0,
            queue: VecDeque::new(),
            ready: Arc::default(),
            socket,
        }
    }
    pub(super) fn push(
        &mut self,
        source: u64,
        destination: u64,
        session: u64,
        wire: WireContract,
        result: Result<&[u8], Status>,
    ) -> bool {
        let n = result.map_or(0, |b| b.len());
        if result.is_ok()
            && (self.queue.len() - self.errors >= self.entries || n > self.limit - self.used)
        {
            return false;
        }
        self.errors += usize::from(result.is_err());
        self.queue.push_back(PortReply {
            source,
            destination,
            session,
            wire,
            body: result.map(Box::from),
        });
        self.used += n;
        self.ready.notify_one();
        if let Some(n) = &self.socket {
            n.notify_one();
        }
        true
    }
}
/// 本机受管入口，拥有有限回复槽；释放即关闭本代次。不能取得本地服务的 HostApi 权限。
pub struct ReplyPort {
    core: Weak<Core>,
    key: u64,
}
impl Node {
    /// 分配与指定节点绑定的有限出口，供 transport 适配器或本机受控传输验证使用。
    /// # Errors
    /// 零/本地节点、非法预算、端口配额满或停止中时拒绝。
    pub fn reply_port(&self, node: u16, entries: usize, bytes: usize) -> Result<ReplyPort, Status> {
        if node == 0
            || node == self.core.node
            || entries == 0
            || entries > self.core.b.control_slots.get()
            || bytes as u64 > self.core.b.payload_bytes.get()
        {
            return Err(Status::PROTOCOL);
        }
        let mut s = lock(&self.core.state);
        if s.closing {
            return Err(Status::STOPPED);
        }
        if s.ports.values().filter(|p| p.socket.is_none()).count()
            >= self.core.b.control_slots.get()
        {
            return Err(Status::BUSY);
        }
        let key = s.ids.issue()?;
        s.ports.insert(key, Port::new(node, entries, bytes, None));
        Ok(ReplyPort {
            core: Arc::downgrade(&self.core),
            key,
        })
    }
}
impl ReplyPort {
    /// 按绑定来源、目标代次、方法、队列和成功响应额度接纳入站请求。不支持伪造玩家入口。
    /// # Errors
    /// 身份、契约、超时或配额不符均拒绝，不登记半请求。
    pub fn request(&self, r: InboundCall<'_>) -> Result<(), Status> {
        let core = self.core.upgrade().ok_or(Status::STOPPED)?;
        let mut s = lock(&core.state);
        core.inbound(&mut s, self.key, r, Entry::Service)
    }
    /// 取走一个出口结果并让等待中的回复有机会重试；同一 token 的提示合并。
    pub fn try_recv(&self) -> Option<PortReply> {
        lock(&self.core.upgrade()?.state).pop_port(self.key)
    }
    /// 在同步适配器线程上有限等待一次实际出队，不占 Actor worker。
    /// # Errors
    /// 没有结果直到截止时间返回 Timeout，已关闭返回 Stopped。
    pub fn recv(&self, timeout: Duration) -> Result<PortReply, Status> {
        let core = self.core.upgrade().ok_or(Status::STOPPED)?;
        let state = lock(&core.state);
        let ready = state
            .ports
            .get(&self.key)
            .ok_or(Status::STOPPED)?
            .ready
            .clone();
        let (mut state, _) = ready
            .wait_timeout_while(state, timeout, |s| {
                !s.closing && s.ports.get(&self.key).is_some_and(|p| p.queue.is_empty())
            })
            .unwrap_or_else(|_| std::process::abort());
        let absent = if state.closing {
            Status::STOPPED
        } else {
            Status::TIMEOUT
        };
        state.pop_port(self.key).ok_or(absent)
    }
}
impl Drop for ReplyPort {
    fn drop(&mut self) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        let mut s = lock(&core.state);
        s.close_port(self.key);
    }
}
impl State {
    pub(super) fn input_method(
        &self,
        dst: ServiceAddress,
        wire: WireContract,
        entry: Entry,
        ceiling: u64,
    ) -> Result<(u64, Method), Status> {
        let key = self.key(dst.bits())?;
        self.running(key)?;
        let m = crate::loader::find_method(self.actor(key)?.spec.implementation, wire.key)?;
        if m.wire != wire || m.directions & 1 == 0 {
            return Err(Status::PROTOCOL);
        }
        if m.peer_control != PeerAccess::None
            || matches!(entry, Entry::Player { .. }) && m.client_callable != 1
        {
            return Err(Status::DENIED);
        }
        if ceiling > m.max_reply_bytes {
            return Err(Status::TOO_SMALL);
        }
        Ok((key, m))
    }
    pub(super) fn has_inbound_session(&self, port: u64, caller: u64, id: u64) -> bool {
        self.ports.get(&port).is_some_and(|p|p.queue.iter().any(|r|r.destination==caller && r.session==id)) || self.pending.values().any(|p|matches!(p.caller,rpc::Caller::Port{key,source,session,..} if (key,source,session)==(port,caller,id)))
    }
    pub(super) fn inbound_room(&self, port: u64, limit: usize) -> bool {
        self.ports.get(&port).is_some_and(|p| {
            p.errors
                + self
                    .pending
                    .values()
                    .filter(|p| matches!(p.caller,rpc::Caller::Port{key,..} if key==port))
                    .count()
                < limit
        })
    }
    pub(super) fn pop_port(&mut self, key: u64) -> Option<PortReply> {
        let port = self.ports.get_mut(&key)?;
        let out = port.queue.pop_front()?;
        port.used -= out.body.as_ref().map_or(0, |b| b.len());
        if out.body.is_err() {
            port.errors -= 1;
        } else {
            let mut wake = Vec::new();
            for p in self.pending.values_mut() {
                if matches!(p.caller,rpc::Caller::Port{key:k,..} if k==key)
                    && p.write == WriteInterest::Waiting
                {
                    p.write.notify();
                    wake.push(p.callee);
                }
            }
            for k in wake {
                if let Some(a) = self.actors.get(&k) {
                    a.signal.notify_one();
                }
            }
        }
        Some(out)
    }
    pub(super) fn close_port(&mut self, key: u64) {
        self.ports.remove(&key);
        let ids = self
            .pending
            .iter()
            .filter_map(|(&id, p)| {
                matches!(p.caller,rpc::Caller::Port{key:k,..} if k==key).then_some(id)
            })
            .collect::<Vec<_>>();
        for id in ids {
            let p = self.pending.get_mut(&id).expect("已在本锁内筛选");
            if p.write != WriteInterest::None {
                p.stage = rpc::Stage::Expired;
                p.write.notify();
                let k = p.callee;
                if let Some(a) = self.actors.get(&k) {
                    a.signal.notify_one();
                }
            } else {
                self.pending.remove(&id);
            }
        }
    }
}
impl Core {
    pub(super) fn inbound(
        &self,
        s: &mut State,
        port: u64,
        r: InboundCall<'_>,
        entry: Entry,
    ) -> Result<(), Status> {
        let p = s.ports.get(&port).ok_or(Status::STOPPED)?;
        if s.closing {
            return Err(Status::STOPPED);
        }
        if r.source.node().get() != p.node {
            return Err(Status::DENIED);
        }
        if r.session == 0 || r.timeout_ms == 0 || r.wire.response == [0; 32] {
            return Err(Status::PROTOCOL);
        }
        // 每个入站记录独立预留一个无体错误出口；直到出队都占槽，停止或 TooSmall 无需重试控制错误。
        if !s.inbound_room(port, self.b.control_slots.get()) {
            return Err(Status::BUSY);
        }
        if s.has_inbound_session(port, r.source.bits(), r.session) {
            return Err(Status::PROTOCOL);
        }
        if r.max_response_bytes > p.limit as u64 {
            return Err(Status::TOO_SMALL);
        }
        let (key, m) = s.input_method(r.destination, r.wire, entry, r.max_response_bytes)?;
        let a = s.actor(key)?;
        if s.pending
            .values()
            .filter(|p| p.callee == key && matches!(p.caller, rpc::Caller::Port { .. }))
            .count()
            >= self.b.control_slots.get()
        {
            return Err(Status::BUSY);
        }
        let deadline = Instant::now() + self.duration(r.timeout_ms)?;
        let reply = ReplyBudget {
            representation: Representation::Bytes,
            ceiling: r.max_response_bytes,
        };
        let reserve = a
            .budget
            .reserve(reply.ceiling, self.b.payload_bytes.get())?;
        let cost = a
            .budget
            .reserve(r.bytes.len() as u64, self.b.payload_bytes.get())?;
        let mut ids = s.ids;
        let token = ids.issue()?;
        let charge = ids.charge(!r.bytes.is_empty())?;
        s.enqueue(
            key,
            Envelope {
                source: r.source.bits(),
                token,
                wire: r.wire,
                entry,
                reply,
                timeout: r.timeout_ms,
                payload: Payload::queued(
                    Body {
                        representation: Representation::Bytes,
                        bytes: r.bytes.into(),
                    },
                    &mut RNone,
                    cost,
                    charge,
                ),
            },
            &mut RNone,
        )?;
        s.ids = ids;
        s.pending.insert(
            token,
            rpc::Pending {
                caller: rpc::Caller::Port {
                    key: port,
                    source: r.source.bits(),
                    target: r.destination.bits(),
                    session: r.session,
                },
                callee: key,
                method: m,
                remote: None,
                reply,
                deadline,
                reserve,
                stage: rpc::Stage::Open,
                write: WriteInterest::None,
                reply_charge: 0,
                action: None,
            },
        );
        Ok(())
    }
}
