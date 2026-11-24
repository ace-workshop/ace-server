use super::*;
use ace_net_proto::{Call, Contract, Frame, FrameLimits, Message as Kind, RpcError};
use ace_server_sdk::{ManagedBytes, TypedRpcError, native_wire};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    net::SocketAddr,
    num::NonZeroU64,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    tcp: SocketAddr,
    kcp: SocketAddr,
    watchdog_alias: String,
    #[serde(default = "admission_limit")]
    bind_timeout_ms: u64,
    #[serde(default)]
    online_aliases: Vec<String>,
}
fn admission_limit() -> u64 {
    5000
}
#[derive(Clone, Copy)]
struct Binding {
    target: ServiceAddress,
    push: NonZeroU64,
    subscription: Option<u64>,
}
#[derive(Clone, Copy)]
enum Route {
    Unbound,
    Admitting,
    Bound(Binding),
    Closing,
}
struct Session {
    conn: Connection,
    route: Route,
    bind_by: Instant,
    // 每个登记都由一个 SDK 入站任务等待独立的 raw pending，受同一 task_slots 上限限制。
    calls: BTreeSet<NonZeroU64>,
}
fn role_conn(c: Connection) -> gateway::Connection {
    gateway::Connection {
        transport: match c.transport {
            Transport::Tcp => gateway::Transport::Tcp,
            Transport::Kcp => gateway::Transport::Kcp,
        },
        id: c.handle.id,
        generation: c.handle.generation,
    }
}
/// `Gateway<C>` 独占的路由状态；连接代次及绑定不进入宿主业务表。
pub struct Routes<C> {
    ctx: Context,
    cfg: Settings,
    watchdog: Option<(ServiceAddress, u64)>,
    pub(super) managers: BTreeSet<ServiceAddress>,
    listeners: Vec<(Transport, ResourceHandle)>,
    ready: Vec<ResourceHandle>,
    failed: Option<Status>,
    sessions: BTreeMap<u64, Session>,
    next_push: u64,
    marker: PhantomData<fn() -> C>,
}
fn status(e: TypedRpcError) -> Status {
    match e {
        TypedRpcError::Status(s) => s,
        TypedRpcError::Codec(_) => Status::PROTOCOL,
    }
}
impl<C: RoutingCodec> Routes<C> {
    pub(super) fn new(ctx: Context, cfg: &[u8]) -> Result<Self, Status> {
        let cfg: Settings = toml::from_str(std::str::from_utf8(cfg).map_err(|_| Status::PROTOCOL)?)
            .map_err(|_| Status::PROTOCOL)?;
        if !(1..=60_000).contains(&cfg.bind_timeout_ms)
            || cfg.online_aliases.len() > 128
            || std::iter::once(&cfg.watchdog_alias)
                .chain(&cfg.online_aliases)
                .any(|s| s.is_empty() || s.len() > 128)
        {
            return Err(Status::PROTOCOL);
        }
        Ok(Self {
            ctx,
            cfg,
            watchdog: None,
            managers: BTreeSet::new(),
            listeners: vec![],
            ready: vec![],
            failed: None,
            sessions: BTreeMap::new(),
            next_push: 0,
            marker: PhantomData,
        })
    }
    fn log(&self, msg: &str) {
        self.ctx.command(HostOp::Log(0, msg.into()));
    }
    fn start(&mut self) -> Result<(), Status> {
        let wd = self.ctx.resolve(&self.cfg.watchdog_alias)?;
        self.ctx
            .check_startup_peer(
                wd,
                StartupPeer {
                    role: watchdog::ROLE.into(),
                    send_methods: [
                        Self::OPEN.descriptor().wire.key,
                        Self::CLOSE.descriptor().wire.key,
                    ]
                    .as_slice()
                    .into(),
                    receive_methods: [
                        Self::BIND.descriptor().wire.key,
                        Self::KICK.descriptor().wire.key,
                    ]
                    .as_slice()
                    .into(),
                    require_player_entry: false,
                    require_subscription: true,
                },
            )
            .inspect_err(|e| {
                self.log(&format!(
                    "gate-watchdog-check failed target={} status={}",
                    wd.bits(),
                    e.0
                ))
            })?;
        self.watchdog = Some((wd, self.ctx.subscribe(wd)?));
        for alias in &self.cfg.online_aliases {
            self.managers.insert(self.ctx.resolve(alias)?);
        }
        for (t, addr) in [
            (Transport::Tcp, self.cfg.tcp),
            (Transport::Kcp, self.cfg.kcp),
        ] {
            self.listeners
                .push((t, self.ctx.net_listen(t, &addr.to_string())?));
        }
        Ok(())
    }
    fn listen_result(&mut self, h: ResourceHandle, r: Result<(Transport, &str), Status>) -> Status {
        let Some(&(t, _)) = self.listeners.iter().find(|(_, id)| *id == h) else {
            return Status::NOT_FOUND;
        };
        match r {
            Ok((actual, addr)) if actual == t && !self.ready.contains(&h) => {
                self.ready.push(h);
                self.log(&format!("gate-listen {t:?} {addr}"));
            }
            Ok(_) => return Status::PROTOCOL,
            Err(e) => self.failed = Some(e),
        }
        self.ctx
            .command(HostOp::LifecycleReady(LifecycleAction::Start))
            .status
    }
    pub(super) fn event(&mut self, op: Control<'_>) -> Status {
        use NetEvent::*;
        match op {
            // Busy 的 Closed 留在 host 原槽。单个周期提示即可保证空闲时也重试，不为每次拒绝另排任务。
            Control::Prepare => self.ctx.event_timer(100, 100).err().unwrap_or(Status::OK),
            Control::Start => self.start().err().unwrap_or(Status::PENDING),
            Control::LifecycleEvent(
                LifecycleAction::Start,
                Event::Network(ListenReady(h, t, addr)),
            ) => self.listen_result(h, Ok((t, addr.as_str()))),
            Control::LifecycleEvent(LifecycleAction::Start, Event::Network(ListenFailed(h, e))) => {
                self.listen_result(h, Err(e))
            }
            Control::LifecycleEvent(LifecycleAction::Start, Event::ResourceGone(_, _)) => {
                self.failed = Some(Status::STOPPED);
                self.ctx
                    .command(HostOp::LifecycleReady(LifecycleAction::Start))
                    .status
            }
            Control::LifecyclePoll(LifecycleAction::Start, _) => {
                self.failed.unwrap_or(if self.ready.len() == 2 {
                    Status::OK
                } else {
                    Status::PENDING
                })
            }
            Control::Event(Event::Network(Accepted(listener, c, peer))) => {
                if !self.listeners.contains(&(c.transport, listener)) {
                    return Status::PROTOCOL;
                }
                if let Some((wd, _)) = self.watchdog {
                    let q = watchdog::Open::Accepted {
                        conn: role_conn(c),
                        peer: peer.as_str().into(),
                    };
                    // 先记录 accept，再向另一个可并行的 Actor 提交；暂拒不产生可准入的路由。
                    self.log(&format!(
                        "gate-accepted {:?} conn={}:{} watchdog={}",
                        c.transport,
                        c.handle.id,
                        c.handle.generation,
                        wd.bits()
                    ));
                    if self.ctx.send(Self::OPEN, wd, q).is_ok() {
                        self.sessions.insert(
                            c.handle.id,
                            Session {
                                conn: c,
                                route: Route::Unbound,
                                bind_by: Instant::now()
                                    + Duration::from_millis(self.cfg.bind_timeout_ms),
                                calls: BTreeSet::new(),
                            },
                        );
                        return Status::OK;
                    }
                }
                self.close(c);
                Status::OK
            }
            Control::Event(Event::Network(Closed(c, reason))) => self.closed(c, reason),
            Control::Event(Event::ResourceGone(token, addr)) => {
                self.gone(token, addr);
                Status::OK
            }
            Control::Event(Event::Timer(_)) => {
                // Watchdog 没有容量建立准入记录、其 Kick 也被暂拒时，未绑定连接不能永久占槽。
                let now = Instant::now();
                let expired: Vec<_> = self
                    .sessions
                    .values()
                    .filter(|s| {
                        matches!(s.route, Route::Unbound | Route::Admitting) && s.bind_by <= now
                    })
                    .map(|s| s.conn)
                    .collect();
                for c in expired {
                    self.close(c);
                }
                Status::OK
            }
            Control::Event(Event::Network(Writable(_) | Error(..))) => Status::OK,
            Control::Event(Event::Network(ListenFailed(..))) => Status::FAULTED,
            _ => Status::NOT_READY,
        }
    }
    pub(super) fn is_watchdog(&self, src: ServiceAddress) -> bool {
        self.watchdog.is_some_and(|(wd, _)| wd == src)
    }
    fn session(&self, c: Connection) -> Option<&Session> {
        self.sessions.get(&c.handle.id).filter(|s| s.conn == c)
    }
    fn live_connection(&self, conn: &gateway::Connection) -> Result<Connection, Status> {
        self.sessions
            .get(&conn.id)
            .filter(|s| role_conn(s.conn) == *conn)
            .map(|s| s.conn)
            .ok_or(Status::NOT_FOUND)
    }
    pub(super) fn bind(&mut self, src: ServiceAddress, q: &gateway::Bind) -> Result<(), Status> {
        if !self.is_watchdog(src) {
            return Err(Status::DENIED);
        }
        let c = self.live_connection(&q.conn)?;
        if !self
            .session(c)
            .is_some_and(|s| matches!(s.route, Route::Admitting) && s.bind_by > Instant::now())
        {
            return Err(Status::STOPPED);
        }
        let target = ServiceAddress::try_from(q.target).map_err(|_| Status::PROTOCOL)?;
        let push = self
            .next_push
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(Status::BUSY)?;
        // 先取得本地目标终态槽，失败不改绑定；远端只有本代路由／调用终态，没有本地订阅。
        let subscription = (target.node() == self.ctx.address().node())
            .then(|| self.ctx.subscribe(target))
            .transpose()?;
        self.sessions
            .get_mut(&q.conn.id)
            .ok_or(Status::NOT_FOUND)?
            .route = Route::Bound(Binding {
            target,
            push,
            subscription,
        });
        self.next_push = push.get();
        self.log(&format!(
            "gate-bound conn={}:{} target={}",
            q.conn.id, q.conn.generation, q.target
        ));
        self.ctx.wake(); // 重新检查同连接在准入期间暂扣的 Data。
        Ok(())
    }
    fn invalidate(&mut self, c: Connection) {
        let Some(s) = self.sessions.get_mut(&c.handle.id).filter(|s| s.conn == c) else {
            return;
        };
        s.calls.clear();
        if let Route::Bound(b) = std::mem::replace(&mut s.route, Route::Closing)
            && let Some(t) = b.subscription
            && self.watchdog.is_none_or(|(_, wt)| wt != t)
            && !self
                .sessions
                .values()
                .any(|s| matches!(s.route, Route::Bound(other) if other.subscription == Some(t)))
        {
            // 与已投递的 ResourceGone 竞争时 token 可能已经消费；不能取消其它代次。
            self.ctx.command(HostOp::ResourceCancel(t));
        }
    }
    fn close(&mut self, c: Connection) {
        self.invalidate(c);
        if let Err(e) = self.ctx.net_close(c) {
            self.log(&format!(
                "gate-close conn={}:{} status={}",
                c.handle.id, c.handle.generation, e.0
            ));
        }
    }
    pub(super) fn kick(&mut self, src: ServiceAddress, q: &gateway::Kick) -> Result<(), Status> {
        if !self.is_watchdog(src) {
            return Err(Status::DENIED);
        }
        let c = self.live_connection(&q.conn)?;
        self.invalidate(c);
        self.ctx.net_close(c)
    }
    pub(super) fn online(&self) -> u32 {
        self.sessions
            .values()
            .filter(|s| !matches!(s.route, Route::Closing))
            .count() as u32
    }
    fn gone(&mut self, token: u64, addr: u64) {
        let lost_watchdog = self
            .watchdog
            .is_some_and(|(a, t)| t == token && a.bits() == addr);
        if lost_watchdog {
            self.watchdog = None;
        }
        let dead: Vec<_> = self.sessions.values().filter(|s| {
            lost_watchdog || matches!(s.route, Route::Bound(b) if b.target.bits() == addr && b.subscription == Some(token))
        }).map(|s| s.conn).collect();
        for c in dead {
            self.close(c);
        }
    }
    fn closed(&mut self, c: Connection, reason: Status) -> Status {
        self.invalidate(c);
        if let Some((wd, _)) = self.watchdog {
            match self
                .ctx
                .send(Self::CLOSE, wd, watchdog::Close { conn: role_conn(c) })
                .map_err(status)
            {
                Ok(()) | Err(Status::NOT_FOUND | Status::STOPPED | Status::FAULTED) => {}
                Err(Status::BUSY) => return Status::BUSY,
                // 永久契约错误不能冒充接纳或让旧会话继续存在。自身终结走 ResourceGone。
                Err(_) => return Status::FAULTED,
            }
        }
        if self.session(c).is_some() {
            self.sessions.remove(&c.handle.id);
        }
        let r = self.release(c);
        self.log(&format!(
            "gate-release {:?} conn={}:{} reason={} release={}",
            c.transport, c.handle.id, c.handle.generation, reason.0, r.0
        ));
        r
    }
    pub(super) fn cleanup_closed(&self, c: Connection) -> Status {
        let r = self.release(c);
        self.log(&format!(
            "gate-cleanup conn={}:{} release={}",
            c.handle.id, c.handle.generation, r.0
        ));
        r
    }
    fn release(&self, c: Connection) -> Status {
        self.ctx.net_release_closed(c).err().unwrap_or(Status::OK)
    }
    fn limits(&self) -> FrameLimits {
        let cap = self.ctx.limits().payload_bytes.get().min(u32::MAX.into()) as u32;
        FrameLimits {
            body_bytes: cap,
            response_bytes: cap,
            timeout_ms: 86_400_000,
        }
    }
    fn binding(&self, c: Connection) -> Result<Binding, Status> {
        match self.session(c).map(|s| s.route) {
            Some(Route::Bound(b)) => Ok(b),
            _ => Err(Status::NOT_FOUND),
        }
    }
    fn send_to_session(
        &mut self,
        c: Connection,
        target: ServiceAddress,
        frame: &[u8],
    ) -> Result<(), Status> {
        if self.binding(c)?.target != target {
            return Err(Status::DENIED);
        }
        if let Err(e) = self.ctx.net_send(c, frame) {
            self.log(&format!(
                "gate-egress conn={}:{} status={}",
                c.handle.id, c.handle.generation, e.0
            ));
            // host 的有限出口就是队列上限。满即关，不在插件里再堆一层帧或保证对端收到。
            self.close(c);
            return Err(e);
        }
        Ok(())
    }
    pub(super) fn push(
        &mut self,
        src: ServiceAddress,
        q: &gateway::SendToSession,
    ) -> Result<(), Status> {
        let c = self.sessions.values().find(|s| {
            matches!(s.route, Route::Bound(b) if b.push.get() == q.push_session && b.target == src)
        }).map(|s| s.conn).ok_or(Status::DENIED)?;
        let f = Frame::from_player(&q.frame, self.limits()).map_err(|_| Status::PROTOCOL)?;
        if f.message != Kind::Send {
            return Err(Status::PROTOCOL);
        }
        self.send_to_session(c, src, &q.frame)
    }
    pub(super) fn data(
        &mut self,
        state: State<Self>,
        c: Connection,
        buf: &[u8],
    ) -> Result<Option<Handler>, Status> {
        let Some(s) = self.session(c) else {
            return Ok(None);
        };
        let b = match s.route {
            Route::Admitting => return Err(Status::BUSY), // 保留 host 的同一帧；Bind/Kick/Closed 会推进。
            Route::Closing => return Ok(None),
            Route::Unbound => {
                let l = self.ctx.limits_codec();
                let q = Frame::from_player(buf, self.limits())
                    .ok()
                    .and_then(|f| demo::admission::<C>().decode_frame_input(&f, l).ok());
                if let (Some(handshake), Some((wd, _))) = (q, self.watchdog)
                    && self
                        .ctx
                        .send(
                            Self::OPEN,
                            wd,
                            watchdog::Open::Handshake {
                                conn: role_conn(c),
                                handshake,
                            },
                        )
                        .is_ok()
                {
                    self.sessions
                        .get_mut(&c.handle.id)
                        .ok_or(Status::NOT_FOUND)?
                        .route = Route::Admitting;
                } else {
                    self.close(c);
                }
                return Ok(None);
            }
            Route::Bound(b) => b,
        };
        let Ok(f) = Frame::from_player(buf, self.limits()) else {
            self.close(c);
            return Ok(None);
        };
        let call = match f.message {
            Kind::Send => None,
            Kind::Request(q) => Some(q),
            Kind::Response { .. } => {
                self.close(c);
                return Ok(None);
            }
        };
        let out = Outbound {
            destination: b.target.bits(),
            wire: native_wire(f.wire),
            body: Body {
                representation: Representation::Bytes,
                bytes: f.body.into(),
            },
            timeout_ms: call.map_or(0, |q| q.timeout_ms.get()),
            reply: ReplyBudget {
                representation: Representation::Bytes,
                ceiling: call.map_or(0, |q| q.max_response_bytes.into()),
            },
            entry: Entry::Player {
                push_session: Some(b.push).into(),
            },
        };
        let Some(call) = call else {
            if let Err(e) = self.ctx.raw_send(out) {
                self.log(&format!(
                    "gate-send rejected method={} status={}",
                    f.wire.key, e.0
                ));
                self.remote_failure(c, b, e);
            }
            return Ok(None);
        };
        if !self
            .sessions
            .get_mut(&c.handle.id)
            .ok_or(Status::NOT_FOUND)?
            .calls
            .insert(call.session)
        {
            // 同代两次请求无法无歧义地共用一个回包号：拒绝连接，两者均不能错配回复。
            self.close(c);
            return Ok(None);
        }
        let deadline = Instant::now() + Duration::from_millis(call.timeout_ms.get());
        let wire = f.wire;
        self.log(&format!(
            "gate-route conn={}:{} target={} session={}",
            c.handle.id,
            c.handle.generation,
            b.target.bits(),
            call.session
        ));
        match self.ctx.raw_call(out) {
            Ok(pending) => Ok(Some(Box::pin(async move {
                let r = pending.await;
                let r = if r.is_ok() && Instant::now() >= deadline {
                    Err(Status::TIMEOUT)
                } else {
                    r
                };
                state.with(|s| s.complete(c, b, wire, call, r));
            }))),
            Err(e) => {
                self.complete(c, b, wire, call, Err(e));
                Ok(None)
            }
        }
    }
    fn remote_failure(&mut self, c: Connection, b: Binding, e: Status) {
        if b.target.node() != self.ctx.address().node()
            && matches!(
                e,
                Status::UNREACHABLE | Status::NOT_FOUND | Status::STOPPED | Status::FAULTED
            )
        {
            self.close(c);
        }
    }
    fn complete(
        &mut self,
        c: Connection,
        b: Binding,
        wire: Contract,
        call: Call,
        r: Result<ManagedBytes, Status>,
    ) {
        let Some(s) = self.sessions.get_mut(&c.handle.id).filter(|s| {
            s.conn == c
                && matches!(s.route, Route::Bound(now) if now.push == b.push && now.target == b.target)
        }) else {
            return;
        };
        if !s.calls.remove(&call.session) {
            return;
        }
        let error = r
            .as_ref()
            .err()
            .map(|e| RpcError::try_from(e.0).unwrap_or(RpcError::Protocol));
        let f = Frame {
            wire,
            message: Kind::Response {
                session: call.session,
                error,
            },
            body: r.as_deref().unwrap_or_default(),
        };
        let mut bytes = vec![0; 104 + f.body.len()]; // 公共响应只有固定头，无请求扩展。
        match f.encode_into(&mut bytes, self.limits()) {
            Ok(n) => {
                if self.send_to_session(c, b.target, &bytes[..n]).is_ok() {
                    self.log(&format!(
                        "gate-response conn={}:{} session={} status={}",
                        c.handle.id,
                        c.handle.generation,
                        call.session,
                        error.map_or(0, |e| e as u32)
                    ));
                }
            }
            Err(_) => self.close(c),
        }
        if let Err(e) = r {
            self.remote_failure(c, b, e);
        }
    }
}
