use super::*;
use rpc::{Caller, Stage, WriteInterest};
use std::{
    collections::HashSet,
    ops::Bound::{Excluded, Unbounded},
};

// 拒绝过的记录在一次进展/唤醒前不再尝试，其它专槽仍可推进。ID 全局唯一；
// 暂拒只记仍有权威槽的 ID，不另排 payload，也不因取消与新建交错而无限增长。
#[derive(Default)]
struct Notices {
    cursor: usize,
    last: [u64; 8],
    refused: HashSet<u64>,
    // 首错不再重投；只有这一次投递实际剩下的成功体需要留到 Cancel 后。
    faulted_reply: Option<Payload>,
}
impl Notices {
    // 同类也轮转：即使不断有其它 poll/唤醒，持续暂拒的一个 token 不能遮住下一个。
    fn next<'a, T>(
        &mut self,
        class: usize,
        records: &'a BTreeMap<u64, T>,
        ready: impl Fn(&T) -> bool,
    ) -> Option<(u64, &'a T)> {
        let after = self.last[class];
        let (&id, item) = records
            .range((Excluded(after), Unbounded))
            .chain(records.range(..=after))
            .find(|(id, r)| !self.refused.contains(id) && ready(r))?;
        self.last[class] = id;
        Some((id, item))
    }
}
enum Notice {
    Network(network::Delivery),
    Peer(peer::Delivery),
    Completion {
        token: u64,
        pending: u64,
        reply: Result<Payload, Status>,
    },
    CreateReady(u64, Status),
    ResourceGone(u64, u64),
    Timer(ResourceHandle),
    ReplyWritable(u64),
}
impl Notice {
    fn id(&self) -> u64 {
        match *self {
            Self::Completion { token, .. }
            | Self::CreateReady(token, _)
            | Self::ResourceGone(token, _)
            | Self::ReplyWritable(token) => token,
            Self::Timer(h) => h.id,
            Self::Network(ref e) => e.id(),
            Self::Peer(ref e) => e.id(),
        }
    }
    // 可移动槽就是宿主尚持有的那个槽；拒绝不丢它，panic 后也只释放实际剩余的一侧。
    fn offer(&mut self, instance: &mut Instance, phase: Phase) -> Status {
        let mut empty = RNone;
        let mut endpoint = String::new();
        let (e, slot) = match self {
            Self::Peer(n) => (Event::Peer(n.event(&mut endpoint)), &mut empty),
            Self::Network(n) => (Event::Network(n.event(&mut endpoint)), &mut empty),
            Self::CreateReady(t, s) => (Event::CreateReady(*t, *s), &mut empty),
            Self::ResourceGone(t, a) => (Event::ResourceGone(*t, *a), &mut empty),
            Self::Timer(h) => (Event::Timer(*h), &mut empty),
            Self::ReplyWritable(t) => (Event::ReplyWritable(*t), &mut empty),
            Self::Completion { pending, reply, .. } => {
                let (reply, charge_token, slot) = match reply {
                    Ok(p) => {
                        let representation = p.body().representation;
                        (
                            Reply::Success(Body {
                                representation,
                                bytes: p.bytes.as_ref().into(),
                            }),
                            p.charge,
                            &mut p.local,
                        )
                    }
                    Err(e) => (Reply::Error(*e), 0, &mut empty),
                };
                (
                    Event::Completion {
                        pending: *pending,
                        reply,
                        charge_token,
                    },
                    slot,
                )
            }
        };
        let op = if let Some(a) = phase.action() {
            Control::LifecycleEvent(a, e)
        } else if phase.ending().is_some() {
            Control::Cleanup(e)
        } else {
            Control::Event(e)
        };
        instance.control(op, slot).status
    }
    fn deliver(
        mut self,
        c: &Core,
        key: u64,
        i: &mut Instance,
        phase: Phase,
        notices: &mut Notices,
    ) -> Status {
        let status = self.offer(i, phase); // 不持宿主锁。
        let mut s = lock(&c.state);
        let status = s.settle(key, &mut self, status);
        if status == Status::FAULTED {
            s.stop(key, status);
        } else if status == Status::OK {
            if phase.ending().is_none() {
                notices.refused.clear();
            }
        } else {
            let a = &s.actors[&key];
            notices.refused.retain(|id| {
                s.pending.contains_key(id)
                    || s.tickets.contains_key(id)
                    || a.subs.contains_key(id)
                    || a.timers.contains_key(id)
                    || s.net.has_notice(*id)
                    || s.peers.has_notice(*id)
            });
            notices.refused.insert(self.id());
        }
        #[cfg(test)]
        let label = if status != Status::OK {
            "notice-refused"
        } else {
            match self {
                Self::CreateReady(..) => "create-ready",
                Self::ReplyWritable(_) => "reply-writable",
                Self::Completion { .. } => "completion",
                _ => "control-event",
            }
        };
        // 锁外回调借着 Data 时 socket 可以先关闭。最后一份在途帧归还后才恢复 accept 字节预算。
        if let Self::Network(network::Delivery::Data(_, _, p))
        | Self::Peer(peer::Delivery::Control(_, _, _, _, p)) = &self
        {
            let last = Arc::strong_count(p) == 1;
            drop(self);
            if last {
                s.net.resume_accept();
            }
        } else if status == Status::FAULTED
            && let Self::Completion {
                reply: Ok(body), ..
            } = self
        {
            notices.faulted_reply = Some(body);
        }
        drop(s); // 遗留 LocalOwned 的原库析构不在账本锁内。
        #[cfg(test)]
        super::tests::trace(key, label);
        status
    }
}
impl State {
    fn ticket_result(&mut self, child: u64, status: Status) {
        if let Some(t) = self.tickets.values_mut().find(|t| t.child == child) {
            t.result = Some(status);
            if let Some(a) = self.actors.get(&t.owner) {
                a.signal.notify_one();
            }
        }
    }
    fn due(&mut self, key: u64, now: Instant) {
        if self
            .actors
            .get(&key)
            .and_then(|a| a.deadline)
            .is_some_and(|t| t <= now)
        {
            self.stop(key, Status::TIMEOUT);
        }
        let ids = self
            .pending
            .iter()
            .filter_map(|(&t, p)| {
                ((p.owner() == Some(key) && matches!(p.stage, Stage::Open)
                    || p.callee == key
                        && matches!(p.caller, Caller::Port { .. })
                        && !matches!(p.stage, Stage::Expired))
                    && p.deadline <= now)
                    .then_some(t)
            })
            .collect::<Vec<_>>();
        for t in ids {
            let p = &self.pending[&t];
            if matches!(p.caller, Caller::Port { .. }) {
                if p.write != WriteInterest::None {
                    let p = self.pending.get_mut(&t).expect("本锁内的 token");
                    p.stage = Stage::Expired;
                    p.write.notify();
                    self.actors[&key].signal.notify_one();
                } else {
                    self.pending.remove(&t);
                }
            } else {
                self.finish_error(t, Status::TIMEOUT)
            }
        }
    }
    fn next_deadline(&self, key: u64, notices: &Notices) -> Option<Instant> {
        let a = self.actors.get(&key)?;
        let phase = *a.phase.borrow();
        let timers = a
            .timers
            .iter()
            .filter(|(id, t)| {
                !self.closing
                    && !notices.refused.contains(id)
                    && (phase == Phase::Running
                        || phase.action().is_some() && t.action == phase.action())
            })
            .map(|(_, t)| t.at);
        a.deadline
            .into_iter()
            .chain(timers)
            .chain(
                self.pending
                    .values()
                    .filter(|p| {
                        p.owner() == Some(key) && matches!(p.stage, Stage::Open)
                            || p.callee == key
                                && matches!(p.caller, Caller::Port { .. })
                                && !matches!(p.stage, Stage::Expired)
                    })
                    .map(|p| p.deadline),
            )
            .min()
    }
    fn notice(&mut self, key: u64, phase: Phase, notices: &mut Notices) -> Option<Notice> {
        // 类间轮转，周期 tick 或不断新建的调用不饿死另一类的终态槽。
        let ordinary = phase == Phase::Running && !self.closing;
        let cleanup = phase.ending().is_some();
        let allowed = |action| {
            phase == Phase::Running
                || cleanup
                || phase.action().is_some() && action == phase.action()
        };
        for _ in 0..notices.last.len() {
            let class = notices.cursor;
            notices.cursor = (class + 1) % notices.last.len();
            match class {
                0 => {
                    if let Some((t, _)) = notices.next(class, &self.pending, |p| {
                        p.owner() == Some(key)
                            && allowed(p.action)
                            && matches!(p.stage, Stage::Ready(_))
                    }) {
                        let p = self.pending.get_mut(&t)?;
                        let Caller::Actor { id, .. } = p.caller else {
                            return None;
                        };
                        let Stage::Ready(mut result) =
                            std::mem::replace(&mut p.stage, Stage::Offering)
                        else {
                            return None;
                        };
                        if let Ok(body) = &mut result {
                            body.lend(&mut self.actors.get_mut(&key)?.charges);
                        }
                        return Some(Notice::Completion {
                            token: t,
                            pending: id,
                            reply: result,
                        });
                    }
                }
                1 if ordinary || cleanup => {
                    if let Some((t, ticket)) = notices.next(class, &self.tickets, |t| {
                        t.owner == key && !t.reported && t.result.is_some()
                    }) {
                        return Some(Notice::CreateReady(t, ticket.result?));
                    }
                }
                2 => {
                    let a = self.actors.get(&key)?;
                    if let Some((id, sub)) =
                        notices.next(class, &a.subs, |sub| sub.gone && allowed(sub.action))
                    {
                        return Some(Notice::ResourceGone(id, sub.target));
                    }
                }
                3 if !cleanup && !self.closing => {
                    let now = Instant::now();
                    let a = self.actors.get(&key)?;
                    if let Some((id, t)) =
                        notices.next(class, &a.timers, |t| allowed(t.action) && t.at <= now)
                    {
                        let h = ResourceHandle {
                            id,
                            generation: t.generation,
                        };
                        return Some(Notice::Timer(h));
                    }
                }
                4 if ordinary => {
                    if let Some((t, _)) = notices.next(class, &self.pending, |p| {
                        p.callee == key && p.write == WriteInterest::Ready
                    }) {
                        self.pending.get_mut(&t)?.write = WriteInterest::Offering;
                        return Some(Notice::ReplyWritable(t));
                    }
                }
                7 => {
                    if let Some(p) =
                        self.peers
                            .notice(key, phase, &mut notices.last[class], &notices.refused)
                    {
                        return Some(Notice::Peer(p));
                    }
                }
                5 | 6 if !self.closing || cleanup => {
                    if let Some(n) = self.net.notice(
                        key,
                        phase,
                        self.actors[&key].net_capacity,
                        &mut notices.last[class],
                        &notices.refused,
                        class == 5,
                    ) {
                        return Some(Notice::Network(n));
                    }
                }
                _ => {}
            }
        }
        None
    }
    fn settle(&mut self, key: u64, n: &mut Notice, status: Status) -> Status {
        // Faulted 不能重投。Bytes 可能已由实例持有，保留 charge 直到实际 release 或 destroy。
        if status == Status::FAULTED {
            return status;
        }
        let accepted = status == Status::OK;
        let a = self.actors.get_mut(&key).expect("driver 记录");
        match n {
            Notice::Peer(e) => return self.settle_peer(e, accepted),
            Notice::Network(e) => {
                if accepted
                    && *a.phase.borrow() == Phase::Running
                    && matches!(
                        e,
                        network::Delivery::Data(..) | network::Delivery::Accepted(..)
                    )
                {
                    // 和刚接纳的 mailbox 输入一样，只有下一次正式 Poll 能确认新的任务容量。
                    a.net_capacity = false;
                    a.ordinary = true;
                }
                return self.net.settle(e, accepted);
            }
            Notice::Completion { token, reply, .. } => {
                // 允许在 Offering 回调内同步消费；已消费的槽决不复活，charge 与之独立。
                let Some(p) = self.pending.get_mut(token) else {
                    return Status::OK;
                };
                if accepted {
                    p.stage = Stage::Delivered;
                } else {
                    if let Ok(body) = reply
                        && !body.reclaim(p.reply.representation, &mut a.charges)
                    {
                        return Status::FAULTED; // 已取走对象或归还字节凭据，不能声称尚未接纳而重投。
                    }
                    p.stage = Stage::Ready(std::mem::replace(reply, Err(Status::BUSY)));
                }
            }
            Notice::CreateReady(id, _) => {
                let Some(t) = self.tickets.get_mut(id) else {
                    return Status::OK;
                };
                t.reported = accepted;
            }
            Notice::ResourceGone(id, _) => {
                if accepted {
                    a.subs.remove(id);
                } else if !a.subs.contains_key(id) {
                    return Status::OK;
                }
            }
            Notice::Timer(h) => {
                let Some(t) = a.timers.get_mut(&h.id) else {
                    return Status::OK;
                };
                if accepted {
                    if t.period.is_zero() {
                        a.timers.remove(&h.id);
                    } else {
                        t.at = Instant::now() + t.period;
                    }
                }
            }
            Notice::ReplyWritable(token) => {
                let Some(p) = self.pending.get_mut(token) else {
                    return Status::OK;
                };
                if p.write != WriteInterest::Offering {
                    return Status::OK;
                } // 回调中已再提交。
                p.write = if accepted {
                    WriteInterest::None
                } else {
                    WriteInterest::Ready
                };
                if accepted && matches!(p.stage, Stage::Expired) {
                    self.pending.remove(token);
                }
            }
        }
        status
    }
}
async fn create(c: &Core, key: u64) -> Result<Instance, Status> {
    let identity = c.identity.clone();
    let address = tokio::task::spawn_blocking(move || lock(&identity).issue())
        .await
        .map_err(|_| Status::FAULTED)?
        .map_err(|_| Status::NOT_READY)?;
    let (name, grants, wake, config) = {
        let mut s = lock(&c.state);
        if s.closing {
            return Err(Status::STOPPED);
        }
        if let Some(e) = s.phase(key)?.ending() {
            return Err(e);
        }
        let a = s.actor_mut(key)?;
        a.address = Some(address);
        let data = (
            a.spec.implementation.implementation.as_str(),
            a.spec.grants,
            a.wake,
            std::mem::take(&mut a.spec.config),
        );
        s.addresses.insert(address.bits(), key);
        data
    };
    c.registry.create(
        name,
        CreateArgs {
            host: &HOST,
            context: key,
            wake_token: NonZeroU64::new(wake).ok_or(Status::PROTOCOL)?,
            limits: c.b.sdk_limits(),
            address: address.bits(),
            node: c.node,
            boot: c.boot,
            advertised: RStr::from_str(&c.advertised),
            config: config.as_slice().into(),
        },
        grants,
    )
}
fn lifecycle_result(s: &mut State, key: u64, phase: Phase, p: Progress) {
    if s.phase(key).is_ok_and(|p| p.ending().is_some()) {
        if p.status == Status::FAULTED {
            s.change(key, Phase::Ending(Status::FAULTED));
        }
        return;
    }
    if p.status == Status::PENDING {
        return;
    }
    if p.status != Status::OK {
        s.stop(key, p.status);
        return;
    }
    s.actor_mut(key).expect("driver 仍持有记录").life_ready = false;
    if phase == Phase::Preparing {
        s.initialized(key, Phase::Prepared);
    } else if s.initialized(key, Phase::AwaitingClaim) {
        s.ticket_result(key, Status::OK);
    }
}
// 普通插件 panic 已由 C wrapper 转为 Faulted。宿主自身的不变量若被破坏，不能
// 让 Tokio 仅结束这个任务、遗留仍可被认领的地址或无限等待 destroy 的管理方。
struct DriverGuard;
impl Drop for DriverGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            std::process::abort();
        }
    }
}
pub(super) async fn run(c: Arc<Core>, key: u64) {
    let _guard = DriverGuard;
    let (signal, mailbox) = {
        let s = lock(&c.state);
        let a = &s.actors[&key];
        (a.signal.clone(), a.mailbox.clone())
    };
    let mut instance = None;
    let mut held = None;
    let mut capacity = true;
    let mut notices = Notices::default();
    let mut cause = loop {
        // 包括被拒请求及状态竞争的 continue 分支，每一轮均让出 worker。
        tokio::task::yield_now().await;
        #[cfg(test)]
        super::tests::trace(key, "turn");
        let (phase, goal, closing) = {
            let mut s = lock(&c.state);
            s.due(key, Instant::now());
            (
                *s.actors[&key].phase.borrow(),
                s.actors[&key].goal,
                s.closing,
            )
        };
        if let Some(e) = phase.ending() {
            break e;
        }
        if closing && phase != Phase::Running {
            // 关停期间不推进尚未发布的生命周期；独立停止通知仍会唤醒本 driver。
            signal.notified().await;
            continue;
        }
        let mut work = true;
        match phase {
            Phase::Reserved if goal.is_some() => {
                {
                    let mut s = lock(&c.state);
                    if s.closing || s.phase(key) != Ok(Phase::Reserved) {
                        continue;
                    }
                    s.change(key, Phase::Creating);
                }
                let created = create(&c, key).await;
                let mut s = lock(&c.state);
                match created {
                    Ok(i) => {
                        instance = Some(i);
                        if s.phase(key) == Ok(Phase::Creating) {
                            s.initialized(key, Phase::Created);
                        }
                    }
                    Err(e) => s.stop(key, e),
                }
            }
            Phase::Created if matches!(goal, Some(Kind::Prepare | Kind::Start)) => {
                let mut s = lock(&c.state);
                if s.closing || s.phase(key) != Ok(phase) {
                    continue;
                }
                s.change(key, Phase::Preparing);
                drop(s);
                let p = instance
                    .as_mut()
                    .expect("已创建")
                    .control(Control::Prepare, &mut RNone);
                lifecycle_result(&mut lock(&c.state), key, Phase::Preparing, p);
            }
            Phase::Prepared if goal == Some(Kind::Start) => {
                let mut s = lock(&c.state);
                if s.closing || s.phase(key) != Ok(phase) {
                    continue;
                }
                s.change(key, Phase::Starting);
                drop(s);
                let p = instance
                    .as_mut()
                    .expect("已创建")
                    .control(Control::Start, &mut RNone);
                lifecycle_result(&mut lock(&c.state), key, Phase::Starting, p);
            }
            Phase::Running | Phase::Preparing | Phase::Starting => {
                let i = instance.as_mut().expect("已创建");
                if phase == Phase::Running
                    && !i.is_running()
                    && let Err(e) = i.publish()
                {
                    lock(&c.state).stop(key, e);
                    continue;
                }
                work = false;
                // 每轮每类回调都有上限；任何下一片段须重新经过停止屏障。
                let notice = {
                    let mut s = lock(&c.state);
                    if s.phase(key) != Ok(phase) {
                        continue;
                    }
                    s.notice(key, phase, &mut notices)
                };
                if let Some(n) = notice {
                    work = true;
                    if n.deliver(&c, key, i, phase, &mut notices) == Status::FAULTED {
                        continue;
                    }
                }
                let poll = {
                    let mut s = lock(&c.state);
                    if s.phase(key) != Ok(phase) {
                        continue;
                    }
                    let closing = s.closing;
                    let a = s.actors.get_mut(&key).expect("driver 记录");
                    if closing {
                        false
                    } else if phase == Phase::Running {
                        std::mem::take(&mut a.ordinary)
                    } else {
                        std::mem::take(&mut a.life_ready)
                    }
                };
                if poll {
                    notices.refused.clear();
                    work = true;
                    let budget = c.b.polls_per_turn.get();
                    #[cfg(test)]
                    super::tests::trace(key, "abi-poll");
                    let r = i.control(
                        match phase.action() {
                            Some(a) => Control::LifecyclePoll(a, budget),
                            None => Control::Poll(budget),
                        },
                        &mut RNone,
                    );
                    let mut s = lock(&c.state);
                    if phase.action().is_some() {
                        lifecycle_result(&mut s, key, phase, r);
                        continue;
                    }
                    if r.status != Status::OK {
                        s.stop(key, Status::FAULTED);
                        continue;
                    }
                    capacity = r.task_capacity != 0;
                    let a = s.actor_mut(key).expect("driver 记录");
                    if std::mem::replace(&mut a.net_capacity, capacity) != capacity && capacity {
                        s.net.readable(key);
                    }
                    if r.more_ready != 0 {
                        s.actors.get_mut(&key).expect("driver 记录").ordinary = true;
                    }
                }
                if phase == Phase::Running {
                    let mut s = lock(&c.state);
                    if s.phase(key) != Ok(phase) {
                        continue;
                    }
                    if !s.closing && held.is_none() {
                        held = mailbox.pop()
                    }
                    if !s.closing && held.as_ref().is_some_and(|m| capacity || m.token != 0) {
                        let mut m = held.take().expect("已有信封");
                        let token = m.token;
                        if token != 0
                            && (!s
                                .pending
                                .get(&token)
                                .is_some_and(|p| matches!(p.stage, Stage::Open))
                                || !capacity)
                        {
                            if !capacity {
                                s.finish_error(token, Status::BUSY)
                            }
                            s.actors.get_mut(&key).expect("driver 记录").queued -= 1;
                            drop(s);
                            drop(m);
                            continue;
                        }
                        m.payload
                            .lend(&mut s.actors.get_mut(&key).expect("driver 记录").charges);
                        drop(s);
                        // 准入已核对未挂账且槽唯一；此处只挂本次既有 LocalOwned 凭据，绝不发号。
                        m.payload.attach().unwrap_or_else(|_| std::process::abort());
                        let representation = m.payload.body().representation;
                        let mut r = i.message(
                            Message {
                                source: m.source,
                                reply_token: token,
                                charge_token: m.payload.charge,
                                request: Outbound {
                                    destination: i.address().bits(),
                                    wire: m.wire,
                                    body: Body {
                                        representation,
                                        bytes: m.payload.bytes.as_ref().into(),
                                    },
                                    timeout_ms: m.timeout,
                                    reply: m.reply,
                                    entry: m.entry,
                                },
                            },
                            &mut m.payload.local,
                        );
                        let mut s = lock(&c.state);
                        if r != Status::OK
                            && r != Status::FAULTED
                            && !m.payload.reclaim(
                                representation,
                                &mut s.actors.get_mut(&key).expect("driver 记录").charges,
                            )
                        {
                            r = Status::FAULTED;
                        }
                        if r == Status::BUSY || r == Status::OK {
                            s.actors.get_mut(&key).expect("driver 记录").net_capacity = false;
                        }
                        if r == Status::BUSY && token == 0 {
                            capacity = false;
                            held = Some(m);
                        } else {
                            s.actors.get_mut(&key).expect("driver 记录").queued -= 1;
                            if r != Status::OK && token != 0 {
                                s.finish_error(
                                    token,
                                    if r == Status::FAULTED {
                                        Status::PANICKED
                                    } else if r.is_reply_error() {
                                        r
                                    } else {
                                        Status::PROTOCOL
                                    },
                                )
                            }
                            if r == Status::FAULTED {
                                s.stop(key, r)
                            } else if r == Status::OK {
                                s.actors.get_mut(&key).expect("driver 记录").ordinary = true;
                            }
                            drop(s);
                            drop(m);
                        }
                        work = true;
                    }
                }
            }
            _ => work = false,
        }
        if !work {
            #[cfg(test)]
            super::tests::trace(key, "idle");
            let until = lock(&c.state).next_deadline(key, &notices);
            if let Some(t) = until {
                let _ = tokio::time::timeout_at(t, signal.notified()).await;
            } else {
                signal.notified().await;
            }
            notices.refused.clear();
        }
    };
    // ② 等待本代所有 socket 所有者结束；Closed 保留在预先占用的槽。
    c.stop_network(key).await;
    c.stop_peers(key).await;
    if let Some(i) = &mut instance {
        i.begin_stop();
        if cause != Status::FAULTED {
            // ③ 只处理已经保留的终结记录，不 poll 普通任务。
            notices.refused.clear();
            for _ in 0..c.b.control_slots.get().saturating_mul(3) + c.b.net_connections.get() {
                let n = {
                    let mut s = lock(&c.state);
                    if s.phase(key) == Ok(Phase::Ending(Status::FAULTED)) {
                        break;
                    }
                    s.notice(key, Phase::Ending(cause), &mut notices)
                };
                let Some(n) = n else { break };
                if n.deliver(&c, key, i, Phase::Ending(cause), &mut notices) == Status::FAULTED {
                    cause = Status::FAULTED;
                    break;
                }
            }
        }
    }
    {
        // ④ 终结仍在等待的调用和 timer。
        let mut s = lock(&c.state);
        if s.phase(key) == Ok(Phase::Ending(Status::FAULTED)) {
            cause = Status::FAULTED;
        }
        s.actors.get_mut(&key).expect("driver 记录").timers.clear();
        let ids = s
            .pending
            .iter()
            .filter(|(_, p)| {
                (p.owner() == Some(key) || p.callee == key) && matches!(p.stage, Stage::Open)
            })
            .map(|(&t, _)| t)
            .collect::<Vec<_>>();
        for t in ids {
            s.finish_error(
                t,
                if cause == Status::FAULTED {
                    cause
                } else {
                    Status::STOPPED
                },
            );
        }
    }
    if let Some(i) = &mut instance {
        i.control(Control::Cancel, &mut RNone);
    } // ⑤ 专用取消，Faulted 不例外。
    let discarded = {
        let mut s = lock(&c.state);
        let ids = s
            .pending
            .iter()
            .filter(|(_, p)| {
                p.owner() == Some(key) || p.callee == key && matches!(p.caller, Caller::Port { .. })
            })
            .map(|(&t, _)| t)
            .collect::<Vec<_>>();
        let removed = ids
            .into_iter()
            .filter_map(|t| s.pending.remove(&t))
            .collect::<Vec<_>>();
        let a = s.actors.get_mut(&key).expect("driver 记录");
        // charges 是已经交到 SDK 的字节凭据；服务状态也可持有至 destroy。
        // ChargeReleased 在 Ending 仍可用，不在此替实际持有者提前归还。
        a.subs.clear();
        s.net.discard(key);
        s.discard_peers(key);
        removed
    };
    drop(discarded);
    drop(notices.faulted_reply);
    drop(held);
    while let Some(m) = mailbox.pop() {
        drop(m)
    } // ⑥ 只释放仍持有的负载，锁外执行原库析构。
    tokio::task::yield_now().await;
    drop(instance); // ⑦ 唯一 driver 在原库 destroy 一次。
    let mut s = lock(&c.state);
    if s.phase(key) == Ok(Phase::Ending(Status::FAULTED)) {
        cause = Status::FAULTED;
    }
    s.ticket_result(key, cause);
    if let Some(a) = s.actors.remove(&key) {
        if let Some(alias) = &a.spec.alias {
            s.aliases.remove(alias);
        }
        if let Some(addr) = a.address {
            s.addresses.remove(&addr.bits());
            for other in s.actors.values_mut() {
                for sub in other.subs.values_mut() {
                    if sub.target == addr.bits() {
                        sub.gone = true;
                        other.signal.notify_one();
                    }
                }
            }
        }
        a.phase.send_replace(Phase::Gone(cause));
        #[cfg(test)]
        super::tests::trace(key, "retired");
    }
}
