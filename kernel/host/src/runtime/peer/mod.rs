//! 节点 TCP 的资源与代次。只执行受权 owner 的裁决，不解释成员 DTO 或选择拨号方。
mod io;
mod route;
use super::*;
use crate::loader::check_peer_method;
use ace_net_proto::{
    Contract, FrameLimits, NodeFrame,
    bootstrap::{PeerHello, PeerReady},
};
pub(super) use route::Binding;
use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
};
use tokio::task::JoinHandle;

const OUT: usize = 8;
const HANDSHAKE: Duration = Duration::from_secs(3);
#[derive(Default)]
pub(super) struct Peers {
    listener: Option<Listener>,
    links: BTreeMap<u64, Link>,
    admitted: HashMap<u16, ResourceHandle>,
    pub(super) boots: Arc<BTreeMap<u16, u64>>,
    saving: Option<JoinHandle<()>>,
    store_failed: bool,
}
struct Listener {
    owner: u64,
    h: ResourceHandle,
    action: Option<LifecycleAction>,
    result: Option<Result<SocketAddr, Status>>,
    task: Option<JoinHandle<()>>,
}
#[derive(Clone, Default)]
struct Signals {
    read: Arc<Notify>,
    write: Arc<Notify>,
    close: Arc<Notify>,
    admitted: Arc<Notify>,
}
struct Link {
    owner: u64,
    h: ResourceHandle,
    action: Option<LifecycleAction>,
    observed: SocketAddr,
    hello: Option<PeerHello>,
    connected: bool,
    admitted: bool,
    remote_ready: bool,
    ready_queued: bool,
    data_notice: bool,
    control: Option<(PeerStage, WireContract, Arc<[u8]>)>,
    writable: rpc::WriteInterest,
    store_wait: bool,
    out: VecDeque<Output>,
    queued_bytes: usize,
    closing: Option<Status>,
    finished: bool,
    io: Signals,
    task: Option<JoinHandle<()>>,
    reservation: Option<Arc<Permit>>,
}
struct Output {
    bytes: Arc<[u8]>,
    _charge: Option<Permit>,
}
impl Link {
    fn active(&self) -> bool {
        self.admitted && self.remote_ready && self.ready_queued && self.closing.is_none()
    }
    fn stage(&self) -> PeerStage {
        if self.admitted {
            PeerStage::Admitted
        } else {
            PeerStage::Candidate
        }
    }
    fn queue(&mut self, bytes: Vec<u8>, charge: Option<Permit>) {
        self.queued_bytes += bytes.len();
        self.out.push_back(Output {
            bytes: bytes.into(),
            _charge: charge,
        });
        self.io.write.notify_one();
    }
    fn ready(&mut self) {
        if self.admitted && !self.ready_queued && self.closing.is_none() {
            // 专用的一条 8B 槽，在第一条路由数据之前排入同一有序流。
            self.queue(PeerReady.encode().into(), None);
            self.ready_queued = true;
            self.data_notice = self.remote_ready;
        }
    }
    fn send(
        &mut self,
        frame: NodeFrame<'_>,
        limits: FrameLimits,
        cap: usize,
        charge: Option<Permit>,
    ) -> Result<(), Status> {
        let (head, body) = match &frame {
            NodeFrame::Control(_, b) => (104, b.len()),
            NodeFrame::Routed(r, f) => (
                120 + 8 * usize::from(matches!(r.entry, ace_net_proto::NodeEntry::Player(_)))
                    + 12 * usize::from(matches!(f.message, ace_net_proto::Message::Request(_))),
                f.body.len(),
            ),
        };
        let size = body
            .checked_add(head)
            .filter(|&x| x <= cap)
            .ok_or(Status::TOO_SMALL)?;
        if self.out.len() >= OUT || size + self.queued_bytes > 2 * cap {
            return Err(Status::BUSY);
        }
        let mut bytes = vec![0; size];
        frame
            .encode_into(&mut bytes, limits)
            .map_err(|_| Status::PROTOCOL)?;
        self.queue(bytes, charge);
        Ok(())
    }
}
#[derive(Clone)]
pub(super) enum Delivery {
    Listen(ResourceHandle, Result<SocketAddr, Status>),
    Connected(ResourceHandle, SocketAddr, PeerHello),
    Control(
        ResourceHandle,
        PeerStage,
        WireContract,
        Arc<[u8]>,
        Arc<Permit>,
    ),
    Writable(ResourceHandle),
    Ready(ResourceHandle),
    Closed(ResourceHandle, Status),
}
impl Delivery {
    fn handle(&self) -> ResourceHandle {
        match *self {
            Self::Listen(h, ..)
            | Self::Connected(h, ..)
            | Self::Control(h, ..)
            | Self::Writable(h)
            | Self::Ready(h)
            | Self::Closed(h, ..) => h,
        }
    }
    pub(super) fn id(&self) -> u64 {
        match self {
            Self::Writable(h) | Self::Closed(h, _) => h.generation,
            _ => self.handle().id,
        }
    }
    pub(super) fn event<'a>(&'a self, text: &'a mut String) -> PeerEvent<'a> {
        match self {
            Self::Listen(h, Ok(p)) => {
                *text = p.to_string();
                PeerEvent::ListenReady(*h, text.as_str().into())
            }
            Self::Listen(h, Err(e)) => PeerEvent::ListenFailed(*h, *e),
            Self::Connected(h, p, v) => {
                *text = p.to_string();
                PeerEvent::Connected(PeerCandidate {
                    link: *h,
                    observed: text.as_str().into(),
                    remote_node: v.node.get(),
                    remote_boot: v.boot.get(),
                    dialer: v.dialer.get(),
                    dialer_boot: v.dialer_boot.get(),
                    attempt: v.attempt.get(),
                })
            }
            Self::Control(h, s, w, b, _) => PeerEvent::Control(*h, *s, *w, b.as_ref().into()),
            Self::Writable(h) => PeerEvent::Writable(*h),
            Self::Ready(h) => PeerEvent::DataReady(*h),
            Self::Closed(h, e) => PeerEvent::Closed(*h, *e),
        }
    }
}
impl Peers {
    pub(super) fn listening(&self) -> usize {
        usize::from(self.listener.is_some())
    }
    fn check_boot(&self, node: u16, boot: u64) -> Result<u64, Status> {
        let high = self.boots.get(&node).copied().unwrap_or(0);
        if boot < high {
            Err(Status::DENIED)
        } else {
            Ok(high)
        }
    }
    fn link(&self, h: ResourceHandle) -> Result<&Link, Status> {
        self.links
            .get(&h.id)
            .filter(|l| l.h == h && l.closing.is_none())
            .ok_or(Status::UNREACHABLE)
    }
    pub(super) fn has_notice(&self, id: u64) -> bool {
        self.listener.as_ref().is_some_and(|l| l.h.id == id)
            || self
                .links
                .values()
                .any(|l| l.h.id == id || l.h.generation == id)
    }
    pub(super) fn notice(
        &mut self,
        key: u64,
        p: Phase,
        last: &mut u64,
        refused: &HashSet<u64>,
    ) -> Option<Delivery> {
        let allowed = |a| p == Phase::Running || (p.action().is_some() && p.action() == a);
        if let Some(l) = &self.listener
            && l.owner == key
            && allowed(l.action)
            && !refused.contains(&l.h.id)
            && let Some(r) = l.result
        {
            return Some(Delivery::Listen(l.h, r));
        }
        use std::ops::Bound::{Excluded, Unbounded};
        let e = self
            .links
            .range((Excluded(*last), Unbounded))
            .chain(self.links.range(..=*last))
            .find_map(|(_, l)| {
                if l.owner != key {
                    return None;
                }
                let h = l.h;
                if let Some(reason) = l.closing {
                    return (l.finished
                        && !refused.contains(&h.generation)
                        && (allowed(l.action) || p.ending().is_some()))
                    .then_some(Delivery::Closed(h, reason));
                }
                if !allowed(l.action) {
                    return None;
                }
                if !refused.contains(&h.generation) && l.writable == rpc::WriteInterest::Ready {
                    return Some(Delivery::Writable(h));
                }
                if refused.contains(&h.id) {
                    return None;
                }
                if l.connected {
                    return Some(Delivery::Connected(h, l.observed, l.hello?));
                }
                if let Some((stage, w, b)) = &l.control {
                    return Some(Delivery::Control(
                        h,
                        *stage,
                        *w,
                        b.clone(),
                        l.reservation.clone()?,
                    ));
                }
                l.data_notice.then_some(Delivery::Ready(h))
            })?;
        *last = e.handle().id;
        if let Delivery::Writable(h) = e {
            self.links.get_mut(&h.id)?.writable = rpc::WriteInterest::Offering;
        }
        Some(e)
    }
}
impl State {
    pub(super) fn settle_peer(&mut self, e: &Delivery, accepted: bool) -> Status {
        let h = e.handle();
        if let Delivery::Listen(_, r) = e {
            if accepted {
                if r.is_err() {
                    self.peers.listener = None;
                } else if let Some(l) = &mut self.peers.listener {
                    l.result = None;
                }
            }
        } else if let Some(l) = self.peers.links.get_mut(&h.id) {
            match e {
                Delivery::Connected(..) if accepted => l.connected = false,
                Delivery::Control(..) if accepted => {
                    l.control = None;
                    l.io.read.notify_one();
                }
                Delivery::Ready(_) if accepted => l.data_notice = false,
                Delivery::Writable(_) if l.writable == rpc::WriteInterest::Offering => {
                    l.writable = if accepted {
                        rpc::WriteInterest::None
                    } else {
                        rpc::WriteInterest::Ready
                    }
                }
                Delivery::Closed(..) if accepted => {
                    self.peers.links.remove(&h.id);
                    self.net.peer_slots -= 1;
                    self.net.resume_accept();
                }
                _ => {}
            }
        }
        if accepted { Status::OK } else { Status::BUSY }
    }
    fn finish_peer(&mut self, h: ResourceHandle) {
        if let Some(l) = self.peers.links.get_mut(&h.id) {
            l.finished = true;
            l.out.clear();
            l.control = None;
            l.reservation = None;
            self.actors[&l.owner].signal.notify_one();
            self.net.resume_accept();
        }
    }
    fn close_link(&mut self, h: ResourceHandle, reason: Status) {
        let Some(l) = self.peers.links.get_mut(&h.id) else {
            return;
        };
        if l.h != h || l.closing.is_some() {
            return;
        }
        l.closing = Some(reason);
        l.io.close.notify_one();
        if let Some(v) = l.hello
            && self.peers.admitted.get(&v.node.get()) == Some(&h)
        {
            self.peers.admitted.remove(&v.node.get());
        }
        self.close_port(h.id);
        let lost: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(&id, p)| p.remote.is_some_and(|b| b.link == h).then_some(id))
            .collect();
        for id in lost {
            self.finish_error(id, Status::UNREACHABLE);
        }
    }
    pub(super) fn stop_links(&mut self, key: u64) {
        let links: Vec<_> = self
            .peers
            .links
            .values()
            .filter(|l| l.owner == key)
            .map(|l| l.h)
            .collect();
        for h in links {
            self.close_link(h, Status::STOPPED);
        }
    }
    pub(super) fn discard_peers(&mut self, key: u64) {
        if self.peers.listener.as_ref().is_some_and(|l| l.owner == key) {
            self.peers.listener = None;
        }
        self.peers.links.retain(|_, l| l.owner != key);
        self.net.peer_slots = self.peers.links.len();
        self.net.resume_accept();
    }
    pub(super) fn publish_peers(&mut self) {
        for l in self.peers.links.values_mut() {
            l.ready();
            self.actors[&l.owner].signal.notify_one();
        }
    }
}
impl Core {
    fn limits(&self) -> FrameLimits {
        let cap = self.b.net_frame_bytes.get() as u32;
        FrameLimits {
            body_bytes: cap,
            response_bytes: cap - 120,
            timeout_ms: self.b.timeout_ms.get(),
        }
    }
    fn deployment(&self) -> Result<&crate::config::PeerDeployment, Status> {
        self.deployment.as_ref().ok_or(Status::NOT_READY)
    }
    fn reserve_link(
        &self,
        s: &mut State,
        key: u64,
        addr: SocketAddr,
        action: Option<LifecycleAction>,
    ) -> Result<(ResourceHandle, Signals), Status> {
        if s.closing || s.phase(key)?.ending().is_some() {
            return Err(Status::STOPPED);
        }
        if s.net.occupied() >= self.b.net_connections.get() {
            return Err(Status::BUSY);
        }
        let bytes = self.b.net_frame_bytes.get() as u64;
        // 读入、控制交付、发送队列、在写帧、回复出口及编码暂存，加上独立的无体错误槽。
        let permit = s.net.bytes.reserve(
            8 * bytes + 120 * self.b.control_slots.get() as u64,
            self.b.net_buffer_bytes.get() as u64,
        )?;
        let mut ids = s.ids;
        let h = ResourceHandle {
            id: ids.issue()?,
            generation: ids.issue()?,
        };
        let io = Signals::default();
        s.peers.links.insert(
            h.id,
            Link {
                owner: key,
                h,
                action,
                observed: addr,
                hello: None,
                connected: false,
                admitted: false,
                remote_ready: false,
                ready_queued: false,
                data_notice: false,
                control: None,
                writable: rpc::WriteInterest::None,
                store_wait: false,
                out: VecDeque::new(),
                queued_bytes: 0,
                closing: None,
                finished: false,
                io: io.clone(),
                task: None,
                reservation: Some(Arc::new(permit)),
            },
        );
        s.net.peer_slots += 1;
        s.ids = ids;
        Ok((h, io))
    }
    pub(super) fn peer_op(
        self: &Arc<Self>,
        s: &mut State,
        key: u64,
        op: HostOp<'_>,
    ) -> Result<HostResult, Status> {
        let phase = s.phase(key)?;
        if phase != Phase::Running && phase.action().is_none()
            || s.closing && !matches!(op, HostOp::PeerSendControl(..) | HostOp::PeerClose(_))
        {
            return Err(phase.ending().unwrap_or(Status::NOT_READY));
        }
        let deployment = self.deployment()?;
        let result = |h: ResourceHandle| HostResult {
            generation: h.generation,
            ..value(h.id)
        };
        match op {
            HostOp::PeerListen(text) => {
                let addr: SocketAddr = text.as_str().parse().map_err(|_| Status::PROTOCOL)?;
                if addr != deployment.advertised {
                    return Err(Status::DENIED);
                }
                if s.peers.listener.is_some()
                    || s.net.listener_count() >= self.b.net_listeners.get()
                {
                    return Err(Status::BUSY);
                }
                let mut ids = s.ids;
                let h = ResourceHandle {
                    id: ids.issue()?,
                    generation: ids.issue()?,
                };
                s.ids = ids;
                let task = self.rt.spawn(io::listen(self.clone(), key, h, addr));
                s.peers.listener = Some(Listener {
                    h,
                    owner: key,
                    action: phase.action(),
                    result: None,
                    task: Some(task),
                });
                return Ok(result(h));
            }
            HostOp::PeerConnect(text, node) => {
                let addr: SocketAddr = text.as_str().parse().map_err(|_| Status::PROTOCOL)?;
                if node == self.node || !deployment.permits(node, addr, true) {
                    return Err(Status::DENIED);
                }
                let (h, io) = self.reserve_link(s, key, addr, phase.action())?;
                let task = self.rt.spawn(io::connection(
                    self.clone(),
                    h,
                    io,
                    io::Socket::Dial(node, addr),
                ));
                s.peers.links.get_mut(&h.id).expect("本锁内预留").task = Some(task);
                return Ok(result(h));
            }
            HostOp::PeerSendControl(h, w, b) => {
                let l = s.peers.link(h)?;
                if l.owner != key || l.hello.is_none() {
                    return Err(Status::DENIED);
                }
                check_peer_method(s.actor(key)?.spec.implementation, w, l.stage(), 2)?;
                let l = s.peers.links.get_mut(&h.id).ok_or(Status::NOT_FOUND)?;
                let r = l.send(
                    NodeFrame::Control(contract(w), b.as_slice()),
                    self.limits(),
                    self.b.net_frame_bytes.get(),
                    None,
                );
                if r == Err(Status::BUSY) {
                    l.writable = rpc::WriteInterest::Waiting;
                }
                r?;
            }
            HostOp::PeerClose(h) => {
                if s.peers.link(h)?.owner != key {
                    return Err(Status::DENIED);
                }
                s.close_link(h, Status::OK);
            }
            HostOp::PeerAdmit(h, node, boot) => self.admit(s, key, h, node, boot)?,
            _ => return Err(Status::PROTOCOL),
        }
        Ok(value(0))
    }
    fn admit(
        self: &Arc<Self>,
        s: &mut State,
        key: u64,
        h: ResourceHandle,
        node: u16,
        boot: u64,
    ) -> Result<(), Status> {
        let l = s.peers.link(h)?;
        let hello = l.hello.ok_or(Status::NOT_READY)?;
        if l.owner != key || (node, boot) != (hello.node.get(), hello.boot.get()) {
            return Err(Status::DENIED);
        }
        let high = s.peers.check_boot(node, boot)?;
        if s.peers.store_failed {
            return Err(Status::FAULTED);
        }
        if boot > high || s.peers.saving.is_some() {
            s.peers
                .links
                .get_mut(&h.id)
                .ok_or(Status::NOT_FOUND)?
                .store_wait = true;
            if s.peers.saving.is_none() {
                let c = self.clone();
                let mut boots = s.peers.boots.clone();
                s.peers.saving = Some(self.rt.spawn(async move {
                    let identity = c.identity.clone();
                    let saved = tokio::task::spawn_blocking(move || {
                        Arc::make_mut(&mut boots).insert(node, boot);
                        lock(&identity).save_peer_boots(&boots)?;
                        Ok::<_, IdentityError>(boots)
                    })
                    .await;
                    if !matches!(saved, Ok(Ok(_))) {
                        eprintln!(
                            "peer-watermark save failed node={node} boot={boot}; admission disabled"
                        );
                    }
                    let mut s = lock(&c.state);
                    if let Ok(Ok(boots)) = saved {
                        s.peers.boots = boots;
                    } else {
                        s.peers.store_failed = true;
                    }
                    s.peers.saving = None;
                    for l in s.peers.links.values_mut() {
                        if std::mem::take(&mut l.store_wait) {
                            l.writable = rpc::WriteInterest::Ready;
                        }
                    }
                    if let Some(a) = s.actors.get(&key) {
                        a.signal.notify_one();
                    }
                }));
            }
            return Err(Status::BUSY);
        }
        if l.admitted {
            return Ok(());
        }
        if let Some(old) = s.peers.admitted.get(&node).copied() {
            s.close_link(old, Status::UNREACHABLE);
        }
        let l = s.peers.links.get_mut(&h.id).ok_or(Status::UNREACHABLE)?;
        l.admitted = true;
        l.io.admitted.notify_one();
        s.ports.insert(
            h.id,
            port::Port::new(
                node,
                OUT,
                self.b.net_frame_bytes.get(),
                Some(l.io.write.clone()),
            ),
        );
        s.peers.admitted.insert(node, h);
        if s.published {
            l.ready();
        }
        s.actor(key)?.signal.notify_one();
        Ok(())
    }
    pub(super) async fn stop_peers(&self, key: u64) {
        let (tasks, save, links) = {
            let mut s = lock(&self.state);
            let mut tasks = Vec::new();
            if let Some(l) = &mut s.peers.listener
                && l.owner == key
            {
                tasks.extend(l.task.take());
            }
            let mut links = Vec::new();
            for l in s.peers.links.values_mut().filter(|l| l.owner == key) {
                links.push(l.h);
                tasks.extend(l.task.take());
            }
            for t in &tasks {
                t.abort();
            }
            let save = if s.actors[&key].spec.grants.peer_control {
                s.peers.saving.take()
            } else {
                None
            };
            (tasks, save, links)
        };
        for t in tasks.into_iter().chain(save) {
            if t.await.is_err_and(|e| !e.is_cancelled()) {
                std::process::abort()
            }
        }
        let mut s = lock(&self.state);
        for h in links {
            s.finish_peer(h);
        }
    }
}
fn contract(w: WireContract) -> Contract {
    Contract {
        key: w.key,
        codec: w.codec,
        request: w.request,
        response: w.response,
    }
}
fn native(w: Contract) -> WireContract {
    WireContract {
        key: w.key,
        codec: w.codec,
        request: w.request,
        response: w.response,
    }
}
