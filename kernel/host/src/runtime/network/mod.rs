//! 资源、通知和额度的唯一权威。I/O 任务不调用插件；回调中的关闭/释放与原记录在同一账本裁决。
pub(super) mod tcp;
mod udp;
use super::rpc::WriteInterest;
use super::*;
use ace_net_proto::Frame;
use ace_transport::{MAX_FRAME, MAX_PREFIX, SESSION_BUFFER_BYTES, frame_limits};
use std::{collections::VecDeque, net::SocketAddr};
use tokio::task::JoinHandle;

const OUT_ENTRIES: usize = 8;
const HELLO_TIMEOUT: Duration = Duration::from_secs(3);
#[derive(Default)]
pub(super) struct Network {
    listeners: BTreeMap<u64, Listener>,
    slots: BTreeMap<u64, Slot>,
    pub(super) bytes: Arc<Account>,
    pub(super) peer_slots: usize,
}
struct Listener {
    owner: u64,
    h: ResourceHandle,
    transport: Transport,
    action: Option<LifecycleAction>,
    result: Option<Result<SocketAddr, Status>>,
    wake: Arc<Notify>,
    task: Option<JoinHandle<()>>,
}
struct Slot {
    owner: u64,
    conn: Connection,
    // 投递凭据属于本代连接槽而非 socket；物理终结不能替在途 Accepted 确认消费。
    pending_accept: Option<ResourceHandle>,
    peer: SocketAddr,
    close_key: u64,
    writable_key: u64,
    state: Link,
}
enum Link {
    Live(Live),
    Closed {
        reason: Status,
        error: bool,
        offering: bool,
        delivered: bool,
    },
    Released,
}
struct Live {
    data: Option<Arc<Vec<u8>>>,
    out: VecDeque<Arc<[u8]>>,
    out_bytes: usize,
    write: WriteInterest,
    closing: Option<Status>,
    reservation: Arc<Permit>,
    io: IoWake,
    task: Option<JoinHandle<()>>,
}
#[derive(Clone, Default)]
struct IoWake {
    read: Arc<Notify>,
    write: Arc<Notify>,
    close: Arc<Notify>,
}
impl IoWake {
    fn shared(n: &Arc<Notify>) -> Self {
        Self {
            read: n.clone(),
            write: n.clone(),
            close: n.clone(),
        }
    }
}
#[derive(Clone)]
pub(super) enum Delivery {
    Listen(ResourceHandle, Transport, Result<SocketAddr, Status>),
    Accepted(ResourceHandle, Connection, SocketAddr),
    Data(Connection, Arc<Vec<u8>>, Arc<Permit>),
    Writable(Connection, u64),
    Error(Connection, Status),
    Closed(Connection, Status, u64),
}
impl Delivery {
    pub(super) fn id(&self) -> u64 {
        match self {
            Self::Listen(h, ..) => h.generation,
            Self::Closed(_, _, id) | Self::Writable(_, id) => *id,
            Self::Accepted(_, c, _) | Self::Data(c, ..) | Self::Error(c, _) => c.handle.generation,
        }
    }
    pub(super) fn event<'a>(&'a self, endpoint: &'a mut String) -> NetEvent<'a> {
        match self {
            Self::Listen(h, t, Ok(p)) => {
                *endpoint = p.to_string();
                NetEvent::ListenReady(*h, *t, endpoint.as_str().into())
            }
            Self::Listen(h, _, Err(e)) => NetEvent::ListenFailed(*h, *e),
            Self::Accepted(h, c, p) => {
                *endpoint = p.to_string();
                NetEvent::Accepted(*h, *c, endpoint.as_str().into())
            }
            Self::Data(c, b, _) => NetEvent::Data(*c, b.as_slice().into()),
            Self::Writable(c, _) => NetEvent::Writable(*c),
            Self::Error(c, e) => NetEvent::Error(*c, *e),
            Self::Closed(c, e, _) => NetEvent::Closed(*c, *e),
        }
    }
}
impl Network {
    fn slot(&self, c: Connection) -> Result<&Slot, Status> {
        self.slots
            .get(&c.handle.id)
            .filter(|s| s.conn == c)
            .ok_or(Status::NOT_FOUND)
    }
    fn slot_mut(&mut self, c: Connection) -> Result<&mut Slot, Status> {
        self.slots
            .get_mut(&c.handle.id)
            .filter(|s| s.conn == c)
            .ok_or(Status::NOT_FOUND)
    }
    fn live(&self, c: Connection) -> Result<&Live, Status> {
        match &self.slot(c)?.state {
            Link::Live(l) if l.closing.is_none() => Ok(l),
            _ => Err(Status::STOPPED),
        }
    }
    fn owned(&mut self, key: u64, c: Connection) -> Result<&mut Slot, Status> {
        let s = self.slot_mut(c)?;
        if s.owner != key {
            return Err(Status::DENIED);
        }
        Ok(s)
    }
    pub(super) fn resume_accept(&self) {
        for l in self.listeners.values() {
            l.wake.notify_one();
        }
    }
    pub(super) fn phase_changed(&self, key: u64) {
        for l in self.listeners.values().filter(|l| l.owner == key) {
            l.wake.notify_one();
        }
    }
    pub(super) fn readable(&self, key: u64) {
        for slot in self.slots.values().filter(|s| s.owner == key) {
            if let Link::Live(l) = &slot.state {
                l.io.read.notify_one();
            }
        }
    }
    pub(super) fn has_notice(&self, id: u64) -> bool {
        self.listeners.values().any(|l| l.h.generation == id)
            || self.slots.values().any(|s| {
                !matches!(s.state, Link::Released)
                    && (s.conn.handle.generation == id || s.close_key == id || s.writable_key == id)
            })
    }
    pub(super) fn notice(
        &mut self,
        key: u64,
        phase: Phase,
        capacity: bool,
        last: &mut u64,
        refused: &std::collections::HashSet<u64>,
        listeners: bool,
    ) -> Option<Delivery> {
        use std::ops::Bound::{Excluded, Unbounded};
        let ordinary = phase == Phase::Running;
        if listeners {
            if phase.ending().is_some() {
                return None;
            }
            let (&id, l) = self
                .listeners
                .range((Excluded(*last), Unbounded))
                .chain(self.listeners.range(..=*last))
                .find(|(_, l)| {
                    l.owner == key
                        && l.result.is_some()
                        && (ordinary || l.action == phase.action())
                        && !refused.contains(&l.h.generation)
                })?;
            *last = id;
            return Some(Delivery::Listen(l.h, l.transport, l.result?));
        }
        if !ordinary && phase.ending().is_none() {
            return None;
        }
        let (id, ev) = self
            .slots
            .range((Excluded(*last), Unbounded))
            .chain(self.slots.range(..=*last))
            .filter(|(_, s)| s.owner == key)
            .find_map(|(&id, s)| {
                let c = s.conn;
                let allowed = !refused.contains(&c.handle.generation);
                if ordinary && let Some(listener) = s.pending_accept {
                    if capacity && allowed {
                        return Some((id, Delivery::Accepted(listener, c, s.peer)));
                    }
                    if matches!(s.state, Link::Closed { .. }) {
                        return None;
                    }
                }
                let ev = match &s.state {
                    Link::Closed {
                        reason,
                        error: true,
                        ..
                    } if ordinary && allowed => Delivery::Error(c, *reason),
                    Link::Closed {
                        reason,
                        delivered: false,
                        ..
                    } if !refused.contains(&s.close_key) => {
                        Delivery::Closed(c, *reason, s.close_key)
                    }
                    Link::Live(l) if ordinary && l.closing.is_none() => {
                        if l.write == WriteInterest::Ready && !refused.contains(&s.writable_key) {
                            Delivery::Writable(c, s.writable_key)
                        } else if s.pending_accept.is_none() && capacity && allowed {
                            Delivery::Data(c, l.data.clone()?, l.reservation.clone())
                        } else {
                            return None;
                        }
                    }
                    _ => return None,
                };
                Some((id, ev))
            })?;
        if matches!(ev, Delivery::Writable(..))
            && let Link::Live(l) = &mut self.slots.get_mut(&id)?.state
        {
            l.write = WriteInterest::Offering;
        }
        if matches!(ev, Delivery::Closed(..))
            && let Link::Closed { offering, .. } = &mut self.slots.get_mut(&id)?.state
        {
            *offering = true;
        }
        *last = id;
        Some(ev)
    }
    pub(super) fn settle(&mut self, e: &Delivery, accepted: bool) -> Status {
        let retry = if accepted { Status::OK } else { Status::BUSY };
        if let Delivery::Listen(h, _, result) = e {
            if accepted {
                if result.is_err()
                    || self
                        .listeners
                        .get(&h.id)
                        .is_some_and(|l| matches!(l.result, Some(Err(_))))
                {
                    self.listeners.remove(&h.id);
                } else if let Some(l) = self.listeners.get_mut(&h.id) {
                    l.result = None;
                }
            }
            return retry;
        }
        let c = match e {
            Delivery::Accepted(_, c, _)
            | Delivery::Writable(c, _)
            | Delivery::Data(c, ..)
            | Delivery::Closed(c, ..)
            | Delivery::Error(c, _) => *c,
            _ => return Status::PROTOCOL,
        };
        let Ok(s) = self.slot_mut(c) else {
            return Status::OK;
        };
        match (&mut s.state, e) {
            (state, Delivery::Accepted(..)) if accepted => {
                s.pending_accept = None;
                if let Link::Live(l) = state {
                    l.io.read.notify_one();
                }
            }
            (Link::Live(l), Delivery::Data(..)) if accepted => {
                l.data = None;
                l.io.read.notify_one();
            }
            (Link::Live(l), Delivery::Writable(..)) if l.write == WriteInterest::Offering => {
                l.write = if accepted {
                    WriteInterest::None
                } else {
                    WriteInterest::Ready
                }
            }
            (Link::Closed { error, .. }, Delivery::Error(..)) if accepted => *error = false,
            (
                Link::Closed {
                    offering,
                    delivered,
                    error,
                    ..
                },
                Delivery::Closed(..),
            ) => {
                *offering = false;
                *delivered = accepted;
                if accepted {
                    *error = false;
                }
            }
            (Link::Released, _) => return Status::OK,
            _ => {}
        }
        retry
    }
    fn release(&mut self, key: u64, c: Connection) -> Result<(), Status> {
        let s = self.owned(key, c)?;
        match s.state {
            Link::Released => return Ok(()),
            Link::Closed { offering: true, .. }
            | Link::Closed {
                delivered: true, ..
            } => {
                s.pending_accept = None;
                s.state = Link::Released;
            }
            _ => return Err(Status::NOT_READY),
        }
        self.resume_accept();
        Ok(())
    }
    fn close(&mut self, key: u64, c: Connection) -> Result<(), Status> {
        let s = self.owned(key, c)?;
        if let Link::Live(l) = &mut s.state {
            l.closing.get_or_insert(Status::OK);
            l.io.close.notify_one();
        }
        Ok(())
    }
    fn cap(b: Budgets, t: Transport) -> usize {
        match t {
            Transport::Tcp => b.net_frame_bytes.get(),
            Transport::Kcp => b.net_frame_bytes.get().min(MAX_FRAME),
        }
    }
    fn weight(b: Budgets, t: Transport) -> usize {
        3 * Self::cap(b, t)
            + MAX_PREFIX
            + match t {
                Transport::Tcp => 0,
                Transport::Kcp => SESSION_BUFFER_BYTES,
            }
    }
    pub(super) fn listener_count(&self) -> usize {
        self.listeners.len()
    }
    pub(super) fn occupied(&self) -> usize {
        self.peer_slots
            + self
                .slots
                .values()
                .filter(|s| !matches!(s.state, Link::Released))
                .count()
    }
    fn room(&self, b: Budgets, t: Transport) -> bool {
        self.bytes.used() + Self::weight(b, t) as u64 <= b.net_buffer_bytes.get() as u64
            && self.occupied() < b.net_connections.get()
    }
    fn send(&mut self, key: u64, c: Connection, b: &[u8], cap: usize) -> Result<(), Status> {
        let s = self.owned(key, c)?;
        let Link::Live(l) = &mut s.state else {
            return Err(Status::STOPPED);
        };
        if l.closing.is_some() {
            return Err(Status::STOPPED);
        }
        if b.len() > cap {
            return Err(Status::TOO_SMALL);
        }
        Frame::decode(b, frame_limits(cap)).map_err(|_| Status::PROTOCOL)?;
        if l.out.len() >= OUT_ENTRIES || l.out_bytes + b.len() > 2 * cap {
            l.write = WriteInterest::Waiting;
            return Err(Status::BUSY);
        }
        l.out.push_back(b.into());
        l.out_bytes += b.len();
        l.io.write.notify_one();
        Ok(())
    }
    fn sent(&mut self, c: Connection) -> Option<u64> {
        let s = self.slot_mut(c).ok()?;
        let Link::Live(l) = &mut s.state else {
            return None;
        };
        l.out_bytes -= l.out.pop_front()?.len();
        if l.write == WriteInterest::Waiting {
            l.write.notify();
            return Some(s.owner);
        }
        None
    }
    fn finish(&mut self, c: Connection, reason: Status) -> Option<u64> {
        let s = self.slot_mut(c).ok()?;
        let Link::Live(l) = &s.state else { return None };
        let owner = s.owner;
        let reason = l.closing.unwrap_or(reason);
        s.state = Link::Closed {
            reason,
            error: reason == Status::UNREACHABLE,
            offering: false,
            delivered: false,
        };
        self.resume_accept();
        Some(owner)
    }
    pub(super) fn discard(&mut self, key: u64) {
        self.listeners.retain(|_, l| l.owner != key);
        self.slots.retain(|_, s| s.owner != key);
        self.resume_accept();
    }
}
impl State {
    // listener 可以先绑定；全部清单动作成功、owner 已认领才开放外部会话。
    fn accepting(&self, owner: u64) -> bool {
        self.published && !self.closing && self.running(owner).is_ok()
    }
}
impl Core {
    pub(super) fn net_op(
        self: &Arc<Self>,
        s: &mut State,
        key: u64,
        op: HostOp<'_>,
    ) -> Result<HostResult, Status> {
        let phase = s.phase(key)?;
        match op {
            HostOp::NetReleaseClosed(c) => s.net.release(key, c)?,
            HostOp::NetListen(t, endpoint) => {
                if s.closing {
                    return Err(Status::STOPPED);
                }
                if phase != Phase::Running && phase.action().is_none() {
                    return Err(phase.ending().unwrap_or(Status::NOT_READY));
                }
                if endpoint.len() > 128 {
                    return Err(Status::PROTOCOL);
                }
                let addr = endpoint
                    .as_str()
                    .parse::<SocketAddr>()
                    .map_err(|_| Status::PROTOCOL)?;
                if s.net.listeners.len() + s.peers.listening() >= self.b.net_listeners.get() {
                    return Err(Status::BUSY);
                }
                let mut ids = s.ids;
                let h = ResourceHandle {
                    id: ids.issue()?,
                    generation: ids.issue()?,
                };
                s.ids = ids;
                let wake = Arc::default();
                let task = match t {
                    Transport::Tcp => {
                        self.rt
                            .spawn(tcp::listen(self.clone(), key, h, addr, Arc::clone(&wake)))
                    }
                    Transport::Kcp => {
                        self.rt
                            .spawn(udp::listen(self.clone(), key, h, addr, Arc::clone(&wake)))
                    }
                };
                s.net.listeners.insert(
                    h.id,
                    Listener {
                        owner: key,
                        h,
                        transport: t,
                        action: phase.action(),
                        result: None,
                        wake,
                        task: Some(task),
                    },
                );
                return Ok(HostResult {
                    generation: h.generation,
                    ..value(h.id)
                });
            }
            HostOp::NetSend(c, b) => {
                if s.closing {
                    return Err(Status::STOPPED);
                }
                s.running(key)?;
                s.net
                    .send(key, c, b.as_slice(), Network::cap(self.b, c.transport))?;
            }
            HostOp::NetClose(c) => {
                s.running(key)?;
                s.net.close(key, c)?;
            }
            _ => return Err(Status::PROTOCOL),
        }
        Ok(value(0))
    }
    fn listen_result(&self, key: u64, h: ResourceHandle, result: Result<SocketAddr, Status>) {
        let mut s = lock(&self.state);
        if let Some(l) = s.net.listeners.get_mut(&h.id) {
            l.result = Some(result);
            s.actors[&key].signal.notify_one();
        }
    }
    fn retired_listener(&self, key: u64, h: ResourceHandle) {
        let mut s = lock(&self.state);
        let Some(l) = s.net.listeners.get_mut(&h.id) else {
            return;
        };
        if l.result.is_some() {
            // 尚未接纳的监听通知不能让服务误以为可以继续使用此 socket。
            l.result = Some(Err(Status::UNREACHABLE));
            s.actors[&key].signal.notify_one();
        } else {
            s.net.listeners.remove(&h.id);
        }
    }
    fn reserve_conn(
        &self,
        s: &mut State,
        key: u64,
        h: ResourceHandle,
        t: Transport,
        peer: SocketAddr,
        io: IoWake,
    ) -> Result<Connection, Status> {
        if !s.accepting(key) {
            return Err(Status::NOT_READY);
        }
        if !s.net.room(self.b, t) {
            return Err(Status::BUSY);
        }
        let mut ids = s.ids;
        let id = match s
            .net
            .slots
            .iter()
            .find(|(_, s)| matches!(s.state, Link::Released))
        {
            Some((&id, _)) => id,
            None => ids.issue()?,
        };
        let c = Connection {
            transport: t,
            handle: ResourceHandle {
                id,
                generation: ids.issue()?,
            },
        };
        let close_key = ids.issue()?;
        let writable_key = ids.issue()?;
        let reservation = Arc::new(s.net.bytes.reserve(
            Network::weight(self.b, t) as u64,
            self.b.net_buffer_bytes.get() as u64,
        )?);
        s.ids = ids;
        s.net.slots.insert(
            id,
            Slot {
                owner: key,
                conn: c,
                pending_accept: Some(h),
                peer,
                close_key,
                writable_key,
                state: Link::Live(Live {
                    data: None,
                    out: VecDeque::new(),
                    out_bytes: 0,
                    write: WriteInterest::None,
                    closing: None,
                    reservation,
                    io,
                    task: None,
                }),
            },
        );
        s.actors[&key].signal.notify_one();
        Ok(c)
    }
    fn can_read(&self, key: u64, c: Connection) -> bool {
        let s = lock(&self.state);
        !s.closing
            && s.running(key).is_ok()
            && s.actors[&key].net_capacity
            && s.net.slot(c).is_ok_and(|s| {
                s.pending_accept.is_none()
                    && matches!(&s.state, Link::Live(l) if l.closing.is_none() && l.data.is_none())
            })
    }
    fn data(&self, key: u64, c: Connection, b: Vec<u8>) -> Result<(), Status> {
        let mut s = lock(&self.state);
        if s.closing {
            return Err(Status::STOPPED);
        }
        s.running(key)?;
        let Link::Live(l) = &mut s.net.slot_mut(c)?.state else {
            return Err(Status::STOPPED);
        };
        if l.closing.is_some() {
            return Err(Status::STOPPED);
        }
        l.data = Some(Arc::new(b));
        s.actors[&key].signal.notify_one();
        Ok(())
    }
    fn output(&self, c: Connection) -> Result<Option<Arc<[u8]>>, Status> {
        Ok(lock(&self.state).net.live(c)?.out.front().cloned())
    }
    fn sent(&self, c: Connection) {
        let mut s = lock(&self.state);
        if let Some(key) = s.net.sent(c) {
            s.actors[&key].signal.notify_one();
        }
    }
    fn finish_conn(&self, c: Connection, r: Status) {
        let mut s = lock(&self.state);
        if let Some(key) = s.net.finish(c, r) {
            s.actors[&key].signal.notify_one();
            #[cfg(test)]
            {
                drop(s);
                super::tests::trace(key, &format!("net-finished {}", c.handle.generation));
            }
        }
    }
    pub(super) async fn stop_network(&self, key: u64) {
        let (tasks, conns) = {
            let mut s = lock(&self.state);
            let mut tasks = s
                .net
                .listeners
                .values_mut()
                .filter(|l| l.owner == key)
                .filter_map(|l| l.task.take())
                .collect::<Vec<_>>();
            let mut conns = vec![];
            for slot in s.net.slots.values_mut().filter(|s| s.owner == key) {
                if let Link::Live(l) = &mut slot.state {
                    conns.push(slot.conn);
                    tasks.extend(l.task.take());
                }
            }
            for t in &tasks {
                t.abort();
            }
            (tasks, conns)
        };
        // abort 不等于资源已关闭；所有 socket 所有者完成析构后才把 Closed 交给唯一 driver。
        for t in tasks {
            if t.await.is_err_and(|e| !e.is_cancelled()) {
                std::process::abort();
            }
        }
        for c in conns {
            self.finish_conn(c, Status::STOPPED);
        }
    }
}

#[cfg(test)]
mod tests;
