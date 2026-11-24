//! 本地节点运行时。唯一 driver 持有插件指针；所有接纳、票据和终态共用宿主账本。
mod driver;
#[cfg(test)]
mod measurements;
mod network;
mod payload;
mod peer;
mod port;
mod rpc;
mod shutdown;
mod startup_peer;
pub use shutdown::run_manifest;
use shutdown::{Shutdown, StopStage};
#[cfg(test)]
mod tests;
use crate::{
    config::{Budgets, ConfigError, IngressGrants, Kind, Manifest},
    identity::{Identity, IdentityError},
    loader::{Implementations, Instance},
};
use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RStr,
};
use ace_net_proto::ServiceAddress;
use ace_server_abi::{ABI_VERSION, Status, native::*};
use crossbeam_queue::ArrayQueue;
use payload::{Account, Envelope, Payload, Permit};
pub use port::{InboundCall, PortReply, ReplyPort};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    num::NonZeroU64,
    path::Path,
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::Duration,
};
use tokio::{
    runtime::{Builder, Handle, Runtime},
    sync::{Notify, watch},
    time::Instant,
};

#[derive(Debug)]
/// 配置、持久身份、runtime 构建或实例初始化的实际失败原因。
pub enum StartError {
    /// 静态清单不满足约束。
    Config(ConfigError),
    /// 身份目录不可用。
    Identity(IdentityError),
    /// 多线程调度器构建失败。
    Runtime(std::io::Error),
    /// 启动动作未成功，不发布节点。
    Instance(Status),
}
impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "启动失败：{self:?}")
    }
}
impl std::error::Error for StartError {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Reserved,
    Creating,
    Created,
    Preparing,
    Prepared,
    Starting,
    AwaitingClaim,
    Running,
    Ending(Status),
    Gone(Status),
}
impl Phase {
    fn ending(self) -> Option<Status> {
        match self {
            Self::Ending(s) | Self::Gone(s) => Some(s),
            _ => None,
        }
    }
    fn action(self) -> Option<LifecycleAction> {
        match self {
            Self::Preparing => Some(LifecycleAction::Prepare),
            Self::Starting => Some(LifecycleAction::Start),
            _ => None,
        }
    }
}
#[derive(Clone)]
struct Spec {
    implementation: &'static Descriptor,
    role: Option<String>,
    alias: Option<String>,
    config: Vec<u8>,
    grants: IngressGrants,
    manage: Vec<String>,
    dependencies: Vec<String>,
}
impl Spec {
    fn provides(&self, role: &Role) -> bool {
        self.role.as_deref() == Some(role.name.as_str())
            && self.implementation.roles.as_slice().iter().any(|r| {
                r.name == role.name && r.revision == role.revision && r.codec == role.codec
            })
    }
}
struct Actor {
    spec: Spec,
    address: Option<ServiceAddress>,
    wake: u64,
    phase: watch::Sender<Phase>,
    stop_rank: isize,
    goal: Option<Kind>,
    signal: Arc<Notify>,
    mailbox: Arc<ArrayQueue<Envelope>>,
    budget: Arc<Account>,
    queued: usize,
    ordinary: bool,
    net_capacity: bool,
    life_ready: bool,
    deadline: Option<Instant>,
    timers: BTreeMap<u64, Timer>,
    subs: BTreeMap<u64, Subscription>,
    charges: HashMap<u64, Permit>,
}
struct Timer {
    at: Instant,
    period: Duration,
    generation: u64,
    action: Option<LifecycleAction>,
}
struct Subscription {
    target: u64,
    gone: bool,
    action: Option<LifecycleAction>,
}
struct Ticket {
    owner: u64,
    child: u64,
    result: Option<Status>,
    reported: bool,
    expires: Instant,
}
// 只在账本锁内发号。RPC 先在副本上取齐整组凭据，入队后才提交高水位。
#[derive(Clone, Copy)]
struct Ids(u64);
impl Ids {
    fn issue(&mut self) -> Result<u64, Status> {
        self.0 = self.0.checked_add(1).ok_or(Status::BUSY)?;
        Ok(self.0)
    }
    fn charge(&mut self, nonempty_bytes: bool) -> Result<u64, Status> {
        if nonempty_bytes { self.issue() } else { Ok(0) }
    }
}
struct State {
    ids: Ids,
    published: bool,
    closing: bool,
    net: network::Network,
    peers: peer::Peers,
    actors: HashMap<u64, Actor>,
    addresses: HashMap<u64, u64>,
    aliases: HashMap<String, u64>,
    tickets: BTreeMap<u64, Ticket>,
    pending: BTreeMap<u64, rpc::Pending>,
    registered_routes: HashMap<rpc::RegisteredRoute, [Representation; 2]>,
    ports: HashMap<u64, port::Port>,
}
impl State {
    fn actor(&self, key: u64) -> Result<&Actor, Status> {
        self.actors.get(&key).ok_or(Status::STOPPED)
    }
    fn actor_mut(&mut self, key: u64) -> Result<&mut Actor, Status> {
        self.actors.get_mut(&key).ok_or(Status::STOPPED)
    }
    fn key(&self, address: u64) -> Result<u64, Status> {
        self.addresses
            .get(&address)
            .copied()
            .ok_or(Status::NOT_FOUND)
    }
    fn phase(&self, key: u64) -> Result<Phase, Status> {
        Ok(*self.actor(key)?.phase.borrow())
    }
    fn change(&mut self, key: u64, p: Phase) {
        if let Some(a) = self.actors.get_mut(&key) {
            a.phase.send_replace(p);
            self.net.phase_changed(key);
            a.signal.notify_one();
        }
    }
    fn initialized(&mut self, key: u64, phase: Phase) -> bool {
        let a = self.actors.get_mut(&key).expect("driver 记录");
        if a.deadline.is_some_and(|t| t <= Instant::now()) {
            self.stop(key, Status::TIMEOUT);
            return false;
        }
        // 清单的下一动作由管理方显式开始；动态 ticket 的总时限保留到认领。
        if matches!(
            (a.goal, phase),
            (Some(Kind::Create), Phase::Created) | (Some(Kind::Prepare), Phase::Prepared)
        ) {
            a.deadline = None;
        }
        self.change(key, phase);
        true
    }
    fn running(&self, key: u64) -> Result<(), Status> {
        match self.phase(key)? {
            Phase::Running => Ok(()),
            p => Err(p.ending().unwrap_or(Status::NOT_READY)),
        }
    }
    fn stop(&mut self, key: u64, status: Status) {
        if status == Status::FAULTED && matches!(self.phase(key), Ok(Phase::Ending(_))) {
            self.change(key, Phase::Ending(status));
            return;
        }
        if self.actors.contains_key(&key)
            && self.phase(key).ok().is_some_and(|p| p.ending().is_none())
        {
            // ① phase 同时是所有入队和普通 wake 的准入线；停止用独立 Notify。
            let a = self.actors.get_mut(&key).expect("刚核对该代次");
            a.wake = 0;
            a.ordinary = false;
            a.life_ready = false;
            self.change(key, Phase::Ending(status));
            self.stop_links(key);
            let tickets: Vec<_> = self
                .tickets
                .iter()
                .filter(|(_, t)| t.owner == key)
                .map(|(&i, _)| i)
                .collect();
            for id in tickets {
                self.abandon(id);
            }
        }
    }
    fn abandon(&mut self, id: u64) {
        if let Some(t) = self.tickets.remove(&id) {
            self.stop(t.child, Status::STOPPED);
        }
    }
    fn claim(&mut self, key: u64, id: u64) -> Result<u64, Status> {
        if self.closing {
            return Err(Status::STOPPED);
        }
        let t = self.tickets.get(&id).ok_or(Status::NOT_FOUND)?;
        if t.owner != key {
            return Err(Status::DENIED);
        }
        let child = t.child;
        if t.result.is_none() {
            return Err(Status::NOT_READY);
        }
        let status = t.result.unwrap_or(Status::NOT_READY);
        if status != Status::OK {
            self.tickets.remove(&id);
            return Err(status);
        }
        if Instant::now() >= t.expires {
            self.abandon(id);
            return Err(Status::TIMEOUT);
        }
        let address = self.publish(child)?;
        self.tickets.remove(&id);
        Ok(address)
    }
    fn publish(&mut self, key: u64) -> Result<u64, Status> {
        if self.closing {
            return Err(Status::STOPPED);
        }
        let p = self.phase(key)?;
        if p != Phase::AwaitingClaim {
            return Err(p.ending().unwrap_or(Status::NOT_READY));
        }
        if self
            .actor(key)?
            .deadline
            .is_some_and(|t| t <= Instant::now())
        {
            self.stop(key, Status::TIMEOUT);
            return Err(Status::TIMEOUT);
        }
        let a = self.actor_mut(key)?;
        a.deadline = None;
        a.ordinary = true;
        let address = a.address.ok_or(Status::NOT_READY)?.bits();
        self.change(key, Phase::Running);
        Ok(address)
    }
}
struct Core {
    state: Mutex<State>,
    registry: Implementations,
    identity: Arc<Mutex<Identity>>,
    boot: u64,
    node: u16,
    deployment: Option<crate::config::PeerDeployment>,
    advertised: String,
    stop_config: crate::config::ShutdownConfig,
    b: Budgets,
    rt: Handle,
}
static ACTIVE: OnceLock<Weak<Core>> = OnceLock::new();
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|_| std::process::abort())
}
fn value(n: u64) -> HostResult {
    HostResult {
        value: n,
        ..HostResult::status(Status::OK)
    }
}
fn guard<T>(f: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|_| std::process::abort())
}
extern "C" fn invoke(key: u64, op: HostOp<'_>, slot: &mut LocalSlot) -> HostResult {
    guard(|| {
        ACTIVE
            .get()
            .and_then(Weak::upgrade)
            .ok_or(Status::STOPPED)
            .and_then(|c| c.invoke(key, op, slot))
            .unwrap_or_else(HostResult::status)
    })
}
extern "C" fn wake(key: u64, token: u64) {
    guard(|| {
        if let Some(c) = ACTIVE.get().and_then(Weak::upgrade) {
            let mut s = lock(&c.state);
            if !s.closing
                && let Some(a) = s.actors.get_mut(&key)
                && a.wake == token
                && *a.phase.borrow() == Phase::Running
            {
                a.ordinary = true;
                a.signal.notify_one();
            }
        }
    })
}
static HOST: HostApi = HostApi {
    abi_size: size_of::<HostApi>() as u32,
    abi_version: ABI_VERSION,
    invoke,
    wake,
    startup_peer_check: RSome(&startup_peer::API),
};
impl Core {
    fn duration(&self, ms: u64) -> Result<Duration, Status> {
        if ms > self.b.timeout_ms.get() {
            Err(Status::PROTOCOL)
        } else {
            Ok(Duration::from_millis(ms))
        }
    }
    fn descriptor(&self, name: &str) -> Result<&'static Descriptor, Status> {
        self.registry
            .descriptors()
            .find(|d| d.implementation.as_str() == name)
            .ok_or(Status::NOT_FOUND)
    }
    fn reserve(
        self: &Arc<Self>,
        s: &mut State,
        spec: Spec,
        dynamic: bool,
    ) -> Result<(u64, watch::Receiver<Phase>), Status> {
        if s.closing {
            return Err(Status::STOPPED);
        }
        if s.actors.len() >= self.b.instances.get() {
            return Err(Status::BUSY);
        }
        if let Some(alias) = &spec.alias
            && (alias.is_empty() || alias.len() > 128 || s.aliases.contains_key(alias))
        {
            return Err(Status::DENIED);
        }
        let key = s.ids.issue()?;
        let wake = s.ids.issue()?;
        let (phase, watch) = watch::channel(Phase::Reserved);
        if let Some(alias) = &spec.alias {
            s.aliases.insert(alias.clone(), key);
        }
        s.actors.insert(
            key,
            Actor {
                stop_rank: if dynamic {
                    0
                } else {
                    spec.alias
                        .as_deref()
                        .map_or(0, |a| self.stop_config.rank(a))
                },
                spec,
                address: None,
                wake,
                phase,
                goal: dynamic.then_some(Kind::Start),
                signal: Arc::default(),
                mailbox: Arc::new(ArrayQueue::new(self.b.mailbox_entries.get())),
                budget: Arc::default(),
                queued: 0,
                ordinary: false,
                net_capacity: false,
                life_ready: false,
                deadline: dynamic
                    .then(|| Instant::now() + Duration::from_millis(self.b.timeout_ms.get())),
                timers: BTreeMap::new(),
                subs: BTreeMap::new(),
                charges: HashMap::new(),
            },
        );
        // 每个已占实例槽只有一个任务；实例从账本移除前 driver 必须已经 destroy。
        let driver = driver::run(self.clone(), key);
        #[cfg(test)]
        if s.actors[&key].spec.alias.as_deref() == Some("handoff") && tests::enabled() {
            self.rt.spawn(tests::handoff(driver));
            return Ok((key, watch));
        }
        self.rt.spawn(driver);
        Ok((key, watch))
    }
    fn invoke(
        self: &Arc<Self>,
        key: u64,
        op: HostOp<'_>,
        slot: &mut LocalSlot,
    ) -> Result<HostResult, Status> {
        op.validate(slot)?;
        match op {
            HostOp::Select(dst, wire, mode) => {
                let (_, _, paths) = self.select(key, dst, wire, mode)?;
                return Ok(HostResult { paths, ..value(0) });
            }
            HostOp::Send(r) | HostOp::Request(r) => return self.submit(key, r, slot).map(value),
            HostOp::Reply(t, r) => return self.reply(key, t, r, slot).map(|()| value(0)),
            _ => {}
        }
        let mut s = lock(&self.state);
        let phase = s.phase(key)?;
        let a = s.actor(key)?;
        a.spec.grants.check(&op)?;
        if let HostOp::Log(_, text) = op {
            if text.len() > 4096 {
                return Err(Status::TOO_SMALL);
            }
            let address = a.address.map_or(0, ServiceAddress::bits);
            drop(s);
            #[cfg(test)]
            tests::trace(key, text.as_str());
            eprintln!("actor[{address}] {}", text.as_str());
            return Ok(value(0));
        }
        if let HostOp::ChargeReleased(id) = op {
            return s
                .actor_mut(key)?
                .charges
                .remove(&id)
                .map(|_| value(0))
                .ok_or(Status::NOT_FOUND);
        }
        if let HostOp::CompletionConsumed(id) = op {
            return s.consume(key, id).map(|_| value(0));
        }
        if let HostOp::CreateAbandon(id) = op {
            if s.tickets.get(&id).is_some_and(|t| t.owner != key) {
                return Err(Status::DENIED);
            }
            s.abandon(id);
            return Ok(value(0));
        }
        if matches!(
            op,
            HostOp::NetReleaseClosed(_)
                | HostOp::NetListen(..)
                | HostOp::NetSend(..)
                | HostOp::NetClose(_)
        ) {
            return self.net_op(&mut s, key, op);
        }
        if let Some(e) = phase.ending() {
            return Err(if e == Status::FAULTED {
                e
            } else {
                Status::STOPPED
            });
        }
        if matches!(
            op,
            HostOp::PeerListen(_)
                | HostOp::PeerConnect(..)
                | HostOp::PeerSendControl(..)
                | HostOp::PeerAdmit(..)
                | HostOp::PeerClose(_)
        ) {
            return self.peer_op(&mut s, key, op);
        }
        let ok = match op {
            HostOp::Resolve(name) | HostOp::ResolvePrepared(name) => {
                let k = *s.aliases.get(name.as_str()).ok_or(Status::NOT_FOUND)?;
                if matches!(op, HostOp::ResolvePrepared(_)) {
                    if phase != Phase::Preparing
                        || !a.spec.dependencies.iter().any(|v| v == name.as_str())
                    {
                        return Err(Status::DENIED);
                    }
                    if let Some(e) = s.phase(k)?.ending() {
                        return Err(e);
                    }
                } else {
                    s.running(k)?;
                }
                s.actor(k)?.address.ok_or(Status::NOT_READY)?.bits()
            }
            HostOp::ServiceCreate(name, cfg, alias) => {
                if s.closing {
                    return Err(Status::STOPPED);
                }
                s.running(key)?;
                if !a.spec.manage.iter().any(|v| v == name.as_str()) {
                    return Err(Status::DENIED);
                }
                if cfg.len() > 65536 || alias.as_ref().into_option().is_some_and(|a| a.len() > 128)
                {
                    return Err(Status::TOO_SMALL);
                }
                if s.tickets.values().filter(|t| t.owner == key).count()
                    >= self.b.control_slots.get()
                {
                    return Err(Status::BUSY);
                }
                let implementation = self.descriptor(name.as_str())?;
                for role in implementation.requires.as_slice() {
                    if !s
                        .actors
                        .values()
                        .any(|a| *a.phase.borrow() == Phase::Running && a.spec.provides(role))
                    {
                        return Err(Status::NOT_READY);
                    }
                }
                let id = s.ids.issue()?;
                let (child, _) = self.reserve(
                    &mut s,
                    Spec {
                        implementation,
                        role: None,
                        config: cfg.as_slice().into(),
                        alias: alias.into_option().map(|s| s.as_str().to_owned()),
                        grants: IngressGrants::default(),
                        manage: Vec::new(),
                        dependencies: Vec::new(),
                    },
                    true,
                )?;
                s.tickets.insert(
                    id,
                    Ticket {
                        owner: key,
                        child,
                        result: None,
                        reported: false,
                        expires: Instant::now() + Duration::from_millis(self.b.timeout_ms.get()),
                    },
                );
                id
            }
            HostOp::CreateClaim(id) => s.claim(key, id)?,
            HostOp::ServiceStop(dst) => {
                if s.closing {
                    return Err(Status::STOPPED);
                }
                let k = s.key(dst)?;
                if !a
                    .spec
                    .manage
                    .iter()
                    .any(|v| v == s.actors[&k].spec.implementation.implementation.as_str())
                {
                    return Err(Status::DENIED);
                }
                s.stop(k, Status::STOPPED);
                0
            }
            HostOp::LifecycleReady(action) => {
                if phase.action() != Some(action) {
                    return Err(Status::NOT_READY);
                }
                let a = s.actor_mut(key)?;
                a.life_ready = true;
                a.signal.notify_one();
                0
            }
            HostOp::TimerSet(delay, period) => {
                if s.closing {
                    return Err(Status::STOPPED);
                }
                if phase != Phase::Running && phase.action().is_none() {
                    return Err(Status::NOT_READY);
                }
                if a.timers.len() >= self.b.control_slots.get() {
                    return Err(Status::BUSY);
                }
                let id = s.ids.issue()?;
                let generation = s.ids.issue()?;
                let at = Instant::now() + self.duration(delay)?;
                let period = self.duration(period)?;
                let a = s.actor_mut(key)?;
                a.timers.insert(
                    id,
                    Timer {
                        at,
                        period,
                        generation,
                        action: phase.action(),
                    },
                );
                a.signal.notify_one();
                return Ok(HostResult {
                    generation,
                    ..value(id)
                });
            }
            HostOp::TimerCancel(h) => {
                let a = s.actor_mut(key)?;
                if a.timers
                    .get(&h.id)
                    .is_none_or(|t| t.generation != h.generation)
                {
                    return Err(Status::NOT_FOUND);
                }
                a.timers.remove(&h.id);
                0
            }
            HostOp::ServiceSubscribe(dst) => {
                let target = s.key(dst)?;
                if let Some(e) = s.phase(target)?.ending() {
                    return Err(if e == Status::FAULTED {
                        e
                    } else {
                        Status::STOPPED
                    });
                }
                if s.phase(target)? != Phase::Running
                    && !(phase == Phase::Preparing
                        && s.actor(target)?
                            .spec
                            .alias
                            .as_ref()
                            .is_some_and(|name| a.spec.dependencies.contains(name)))
                {
                    return Err(Status::DENIED);
                }
                if let Some((&id, _)) = a.subs.iter().find(|(_, sub)| sub.target == dst) {
                    return Ok(value(id));
                }
                if a.subs.len() >= self.b.control_slots.get()
                    || s.actors
                        .values()
                        .flat_map(|a| a.subs.values())
                        .filter(|x| x.target == dst)
                        .count()
                        >= self.b.control_slots.get()
                {
                    return Err(Status::BUSY);
                }
                let id = s.ids.issue()?;
                s.actor_mut(key)?.subs.insert(
                    id,
                    Subscription {
                        target: dst,
                        gone: false,
                        action: phase.action(),
                    },
                );
                id
            }
            HostOp::ResourceCancel(id) => {
                s.actor_mut(key)?
                    .subs
                    .remove(&id)
                    .ok_or(Status::NOT_FOUND)?;
                0
            }
            _ => return Err(Status::NOT_READY),
        };
        Ok(value(ok))
    }
}
/// 本机已显式启动的节点。由同步管理线程拥有，析构等待所有唯一 driver 收尾。
pub struct Node {
    core: Arc<Core>,
    rt: Option<Runtime>,
    shutdown: Arc<Shutdown>,
    started: bool,
}
impl Node {
    /// 在既有身份目录和已装载实现上执行清单三动作。只有实例显式申请才建立网络监听。
    /// # Errors
    /// 清单或身份无效、重复启动、动作失败/超时均失败；先逆序关闭本次预留。
    pub fn start(
        manifest: &Manifest,
        registry: Implementations,
        identity_dir: &Path,
    ) -> Result<Self, StartError> {
        let mut node = Self::new(
            manifest,
            registry,
            identity_dir,
            Shutdown::new(&manifest.shutdown),
        )?;
        node.start_manifest(manifest)
            .map_err(StartError::Instance)?;
        node.started = true;
        Ok(node)
    }
    fn new(
        manifest: &Manifest,
        registry: Implementations,
        identity_dir: &Path,
        shutdown: Arc<Shutdown>,
    ) -> Result<Self, StartError> {
        if Handle::try_current().is_ok() {
            return Err(StartError::Instance(Status::DENIED));
        }
        manifest
            .validate()
            .and_then(|()| manifest.check(&registry))
            .map_err(StartError::Config)?;
        let identity = Identity::open(identity_dir, manifest.node, manifest.sequence_block)
            .map_err(StartError::Identity)?;
        let mut peers = peer::Peers::default();
        if manifest.peers.is_some() {
            peers.boots = Arc::new(identity.peer_boots().map_err(StartError::Identity)?);
        }
        let rt = Builder::new_multi_thread()
            .worker_threads(manifest.budgets.workers.get())
            .max_blocking_threads(1)
            .enable_time()
            .enable_io()
            .build()
            .map_err(StartError::Runtime)?;
        let core = Arc::new(Core {
            state: Mutex::new(State {
                ids: Ids(0),
                published: false,
                closing: false,
                net: network::Network::default(),
                peers,
                actors: HashMap::new(),
                addresses: HashMap::new(),
                aliases: HashMap::new(),
                tickets: BTreeMap::new(),
                pending: BTreeMap::new(),
                registered_routes: HashMap::new(),
                ports: HashMap::new(),
            }),
            registry,
            boot: identity.boot(),
            identity: Arc::new(Mutex::new(identity)),
            stop_config: manifest.shutdown.clone(),
            b: manifest.budgets,
            rt: rt.handle().clone(),
            node: manifest.node.get(),
            deployment: manifest.peers.clone(),
            advertised: manifest
                .peers
                .as_ref()
                .map(|p| p.advertised.to_string())
                .unwrap_or_default(),
        });
        shutdown
            .core
            .set(Arc::downgrade(&core))
            .map_err(|_| StartError::Instance(Status::DENIED))?;
        ACTIVE
            .set(Arc::downgrade(&core))
            .map_err(|_| StartError::Instance(Status::DENIED))?;
        Ok(Self {
            core,
            rt: Some(rt),
            shutdown,
            started: false,
        })
    }
    fn start_manifest(&self, m: &Manifest) -> Result<(), Status> {
        let mut stop = self.shutdown.stage.subscribe();
        let mut created = HashMap::new();
        {
            let mut s = lock(&self.core.state);
            // 所有 alias 先预留，key 顺序与显式 create 一致，失败时反序停止。
            for action in m.actions.iter().filter(|a| a.kind == Kind::Create) {
                if *stop.borrow() != StopStage::Open {
                    return Err(Status::STOPPED);
                }
                let e = m
                    .instances
                    .iter()
                    .find(|e| e.alias == action.alias)
                    .ok_or(Status::NOT_FOUND)?;
                let spec = Spec {
                    implementation: self.core.descriptor(&e.implementation)?,
                    role: e.role.clone(),
                    config: e.config.as_bytes().into(),
                    alias: Some(e.alias.clone()),
                    grants: e.grants,
                    manage: e.manage.clone(),
                    dependencies: e.prepare_dependencies.clone(),
                };
                created.insert(&e.alias, self.core.reserve(&mut s, spec, false)?);
            }
        }
        for a in &m.actions {
            let (key, rx) = created.get_mut(&a.alias).ok_or(Status::NOT_FOUND)?;
            {
                let mut s = lock(&self.core.state);
                if *stop.borrow() != StopStage::Open {
                    return Err(Status::STOPPED);
                }
                let actor = s.actor_mut(*key)?;
                actor.goal = Some(a.kind);
                actor.deadline =
                    Some(Instant::now() + Duration::from_millis(self.core.b.timeout_ms.get()));
                actor.signal.notify_one();
            }
            let target = match a.kind {
                Kind::Create => Phase::Created,
                Kind::Prepare => Phase::Prepared,
                Kind::Start => Phase::AwaitingClaim,
            };
            let phase = self.core.rt.block_on(async {
                tokio::select! {
                    biased;
                    _ = stop.wait_for(|p| *p != StopStage::Open) => Err(Status::STOPPED),
                    r = rx.wait_for(|p| *p == target || p.ending().is_some()) => r.map(|p| *p).map_err(|_| Status::STOPPED),
                }
            })?;
            if let Some(e) = phase.ending() {
                return Err(e);
            }
            if a.kind == Kind::Start {
                lock(&self.core.state).publish(*key)?;
            }
        }
        let mut s = lock(&self.core.state);
        let stage = stop.borrow();
        if s.closing || *stage != StopStage::Open {
            return Err(Status::STOPPED);
        }
        for (key, _) in created.values() {
            s.running(*key)?;
        }
        s.published = true;
        s.publish_peers();
        s.net.resume_accept();
        Ok(())
    }
    /// 查询已经认领发布的别名；旧地址和待认领实例不可见。
    pub fn resolve(&self, name: &str) -> Option<ServiceAddress> {
        let s = lock(&self.core.state);
        let key = *s.aliases.get(name)?;
        s.running(key).ok()?;
        s.actors[&key].address
    }
    /// 本机管理方停止实例并等待唯一 destroy；插件侧 self-stop 只登记命令。
    /// # Errors
    /// 已回收或不属于本节点时返回 NotFound。
    pub fn stop(&self, address: ServiceAddress) -> Result<(), Status> {
        if Handle::try_current().is_ok() {
            return Err(Status::DENIED);
        }
        let (mut rx, key) = {
            let s = lock(&self.core.state);
            let k = s.key(address.bits())?;
            (s.actors[&k].phase.subscribe(), k)
        };
        lock(&self.core.state).stop(key, Status::STOPPED);
        self.core
            .rt
            .block_on(rx.wait_for(|p| matches!(p, Phase::Gone(_))))
            .map_err(|_| Status::FAULTED)?;
        Ok(())
    }
    /// 当前实例、RPC/入站 reply 记录及仍由活实例账户持有的受管字节，用于管理观测。
    pub fn usage(&self) -> (usize, usize, u64) {
        let s = lock(&self.core.state);
        (
            s.actors.len(),
            s.pending.len(),
            s.actors.values().map(|a| a.budget.used()).sum(),
        )
    }
}
