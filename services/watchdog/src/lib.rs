//! 同一 Gate 两种传输共用的准入 Actor；只相信已订阅的本地 Gate 和配置的策略调用结果。
use ace_codec::{Codec, NoReply};
use ace_role_gateway as gateway;
use ace_role_watchdog as watchdog;
use ace_server_sdk::{
    Actor, ActorService, Context, Direction::*, Handler, Incoming, RpcMethod, ServiceAddress,
    State, Status, native::*,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    time::{Duration, Instant},
};

/// 编译期装配的 Watchdog，默认薄入口选用 AceCodec。
pub type Watchdog<C> = Actor<Sessions<C>>;
/// 准入实现实际使用的角色协议，包括绑定和踢出的两向本地表示。
pub trait AdmissionCodec:
    watchdog::WatchdogCodec
    + Codec<gateway::Bind>
    + Codec<gateway::Kick>
    + Codec<gateway::Ack>
    + 'static
{
}
impl<C> AdmissionCodec for C where
    C: watchdog::WatchdogCodec
        + Codec<gateway::Bind>
        + Codec<gateway::Kick>
        + Codec<gateway::Ack>
        + 'static
{
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    gate_alias: String,
    default_target_alias: Option<String>,
    admission_policy_alias: Option<String>,
    #[serde(default = "timeout")]
    admission_timeout_ms: u64,
    #[serde(default = "capacity")]
    max_sessions: usize,
}
fn timeout() -> u64 {
    3000
}
fn capacity() -> usize {
    64
}
// 策略调用与 Bind 共用 Accepted 时的单调截止时间，不各取一份完整准入期限。
fn time_left(deadline: Instant) -> Option<u64> {
    let left = deadline.checked_duration_since(Instant::now())?;
    (!left.is_zero()).then(|| (left.as_millis() as u64).max(1))
}
#[derive(Clone, Copy)]
enum Admission {
    Demo(ServiceAddress),
    Policy(ServiceAddress),
}
enum Phase {
    Pending {
        timer: ResourceHandle,
        deadline: Instant,
        peer: String,
        checking: bool,
    },
    Bound,
    Rejecting {
        retry: Instant,
    },
}
struct Session {
    conn: gateway::Connection,
    phase: Phase,
}
/// `Watchdog<C>` 独占的准入记录；一个连接号只承载核对过的 Gate、传输和代次。
pub struct Sessions<C> {
    ctx: Context,
    cfg: Settings,
    gate: Option<(ServiceAddress, u64)>,
    admission: Option<Admission>,
    sessions: BTreeMap<u64, Session>,
    retry: Option<ResourceHandle>,
    marker: PhantomData<fn() -> C>,
}
impl<C: AdmissionCodec> Sessions<C> {
    const OPEN: RpcMethod<watchdog::Open, NoReply> =
        RpcMethod::send(watchdog::open::<C>(), Receive);
    const CLOSE: RpcMethod<watchdog::Close, NoReply> =
        RpcMethod::send(watchdog::close::<C>(), Receive);
    const CHECK: RpcMethod<watchdog::CheckAdmission, watchdog::AdmissionDecision> =
        RpcMethod::call(watchdog::check_admission::<C>(), Send);
    const BIND: RpcMethod<gateway::Bind, gateway::Ack> =
        RpcMethod::call(gateway::bind::<C>(), Send)
            .local_input()
            .local_output(size_of::<gateway::Ack>() as u64);
    const KICK: RpcMethod<gateway::Kick, gateway::Ack> =
        RpcMethod::call(gateway::kick::<C>(), Send)
            .local_input()
            .local_output(size_of::<gateway::Ack>() as u64);
    fn prepare(&mut self) -> Result<(), Status> {
        let gate = self.ctx.resolve_prepared(&self.cfg.gate_alias)?;
        self.ctx
            .check_startup_peer(
                gate,
                StartupPeer {
                    role: gateway::ROLE.into(),
                    send_methods: [
                        Self::BIND.descriptor().wire.key,
                        Self::KICK.descriptor().wire.key,
                    ]
                    .as_slice()
                    .into(),
                    receive_methods: [
                        Self::OPEN.descriptor().wire.key,
                        Self::CLOSE.descriptor().wire.key,
                    ]
                    .as_slice()
                    .into(),
                    require_player_entry: true,
                    require_subscription: false,
                },
            )
            .inspect_err(|e| {
                self.log(&format!(
                    "watchdog-gate-check failed target={} status={}",
                    gate.bits(),
                    e.0
                ))
            })?;
        let token = self.ctx.subscribe(gate)?;
        let admission = match &self.cfg.admission_policy_alias {
            Some(name) => Admission::Policy(self.ctx.resolve(name)?),
            None => Admission::Demo(
                self.ctx.resolve(
                    self.cfg
                        .default_target_alias
                        .as_deref()
                        .ok_or(Status::PROTOCOL)?,
                )?,
            ),
        };
        self.retry = Some(self.ctx.event_timer(self.cfg.admission_timeout_ms, 100)?);
        self.gate = Some((gate, token));
        self.admission = Some(admission);
        self.log(&format!("watchdog-prepared gate={}", gate.bits()));
        Ok(())
    }
    fn log(&self, msg: &str) {
        self.ctx.command(HostOp::Log(0, msg.into()));
    }
    fn is_gate(&self, src: ServiceAddress) -> bool {
        self.gate.is_some_and(|(g, _)| g == src)
    }
    fn session(&mut self, c: &gateway::Connection) -> Option<&mut Session> {
        self.sessions.get_mut(&c.id).filter(|s| s.conn == *c)
    }
    fn bind_time(&mut self, gate: ServiceAddress, c: &gateway::Connection) -> Option<u64> {
        if !self.is_gate(gate) {
            return None;
        }
        let Phase::Pending {
            checking: true,
            deadline,
            ..
        } = self.session(c)?.phase
        else {
            return None;
        };
        time_left(deadline)
    }
    fn cancel_timer(&self, phase: Phase) {
        if let Phase::Pending { timer, .. } = phase {
            self.ctx.command(HostOp::TimerCancel(timer));
        }
    }
    fn close(&mut self, gate: ServiceAddress, c: &gateway::Connection) {
        if self.is_gate(gate)
            && self.session(c).is_some()
            && let Some(s) = self.sessions.remove(&c.id)
        {
            self.cancel_timer(s.phase);
            self.log(&format!(
                "watchdog-close conn={}:{} remaining={}",
                c.id,
                c.generation,
                self.sessions.len()
            ));
        }
    }
    fn gone(&mut self, token: u64, addr: u64, cancel_timers: bool) {
        if self
            .gate
            .is_some_and(|(g, t)| g.bits() == addr && t == token)
        {
            self.gate = None;
            let sessions = std::mem::take(&mut self.sessions);
            let retry = self.retry.take();
            // Stopping 的 timer 由宿主收尾，不恢复准入任务。
            if cancel_timers {
                // 唯一受信 Gate 已终结，不再有 Kick 可重试。
                if let Some(h) = retry {
                    self.ctx.command(HostOp::TimerCancel(h));
                }
                for s in sessions.into_values() {
                    self.cancel_timer(s.phase);
                }
            }
            self.log("watchdog-gate-gone");
        }
    }
    fn kick(&self, gate: ServiceAddress, c: &gateway::Connection) {
        // Kick 的业务确认是本代 Close。等待期间不占普通任务；RPC 本身有 deadline，完成槽仍由 SDK 消费。
        if let Err(e) = self.ctx.call(
            Self::KICK,
            gate,
            gateway::Kick { conn: c.clone() },
            self.cfg.admission_timeout_ms,
            1024,
        ) {
            self.log(&format!(
                "watchdog-kick conn={}:{} refused={e}",
                c.id, c.generation
            ));
        }
    }
    fn reject(&mut self, gate: ServiceAddress, c: &gateway::Connection) {
        let retry = Instant::now() + Duration::from_millis(self.cfg.admission_timeout_ms);
        if !self.is_gate(gate) {
            return;
        }
        let Some(s) = self.session(c) else { return };
        if matches!(s.phase, Phase::Pending { .. }) {
            let old = std::mem::replace(&mut s.phase, Phase::Rejecting { retry });
            self.cancel_timer(old);
            self.kick(gate, c);
        }
    }
    fn accepted(&mut self, gate: ServiceAddress, c: &gateway::Connection, peer: &str) {
        if !self.is_gate(gate) || self.sessions.contains_key(&c.id) {
            return;
        }
        if self.sessions.len() >= self.cfg.max_sessions
            || c.generation == 0
            || c.id == 0
            || peer.len() > 128
        {
            self.kick(gate, c);
            return;
        }
        let delay = self.cfg.admission_timeout_ms;
        let deadline = Instant::now() + Duration::from_millis(delay);
        let phase = match self.ctx.event_timer(delay, 0) {
            Ok(timer) => Phase::Pending {
                timer,
                deadline,
                peer: peer.into(),
                checking: false,
            },
            Err(_) => {
                self.kick(gate, c);
                Phase::Rejecting { retry: deadline }
            }
        };
        self.sessions.insert(
            c.id,
            Session {
                conn: c.clone(),
                phase,
            },
        );
        self.log(&format!(
            "watchdog-open gate={} conn={}:{}",
            gate.bits(),
            c.id,
            c.generation
        ));
    }
    fn tick(&mut self, h: ResourceHandle) {
        let Some((gate, _)) = self.gate else { return };
        if self.retry == Some(h) {
            // 每轮最多重交一条 Kick；仍占原会话直到 Gate 确认关闭或整体终结。
            let now = Instant::now();
            let due = self
                .sessions
                .values_mut()
                .filter_map(|s| match &mut s.phase {
                    Phase::Rejecting { retry } if *retry <= now => Some((retry, &s.conn)),
                    _ => None,
                })
                .min_by_key(|(at, _)| **at);
            if let Some((at, conn)) = due {
                *at = now + Duration::from_millis(self.cfg.admission_timeout_ms);
                let c = conn.clone();
                self.kick(gate, &c);
            }
        } else if let Some(c) = self
            .sessions
            .values()
            .find(|s| matches!(s.phase, Phase::Pending { timer, .. } if timer == h))
            .map(|s| s.conn.clone())
        {
            self.reject(gate, &c);
        }
    }
    async fn open(
        state: State<Self>,
        gate: ServiceAddress,
        q: ace_server_sdk::Managed<watchdog::Open>,
    ) {
        let (c, handshake) = match &*q {
            watchdog::Open::Accepted { conn, peer } => {
                state.with(|s| s.accepted(gate, conn, peer));
                return;
            }
            watchdog::Open::Handshake { conn, handshake } => (conn, handshake),
        };
        let Some((ctx, admission, ms, peer)) = state.with(|s| {
            if !s.is_gate(gate) {
                return None;
            }
            let admission = s.admission?;
            let ctx = s.ctx.clone();
            let Phase::Pending {
                checking,
                deadline,
                peer,
                ..
            } = &mut s.session(c)?.phase
            else {
                return None;
            };
            let ms = time_left(*deadline)?;
            if *checking {
                return None;
            }
            *checking = true;
            Some((ctx, admission, ms, peer.clone()))
        }) else {
            return;
        };
        let target = match admission {
            Admission::Demo(target) => (handshake.token == "demo").then_some(target),
            Admission::Policy(policy) => {
                let query = watchdog::CheckAdmission {
                    conn: c.clone(),
                    peer,
                    handshake: handshake.clone(),
                };
                match ctx.call(Self::CHECK, policy, query, ms, 1024) {
                    Ok(p) => match p.await.as_deref() {
                        Ok(watchdog::AdmissionDecision::Admitted { target }) => {
                            ServiceAddress::try_from(*target).ok()
                        }
                        _ => None,
                    },
                    Err(_) => None,
                }
            }
        };
        let Some(target) = target else {
            state.with(|s| s.reject(gate, c));
            return;
        };
        let bind = state.with(|s| {
            let ms = s.bind_time(gate, c)?;
            s.log(&format!(
                "watchdog-admitted conn={}:{} target={}",
                c.id,
                c.generation,
                target.bits()
            ));
            Some(ctx.call(
                Self::BIND,
                gate,
                gateway::Bind {
                    conn: c.clone(),
                    target: target.bits(),
                },
                ms,
                1024,
            ))
        });
        let Some(bind) = bind else { return };
        let success = match bind {
            Ok(p) => p.await.is_ok(),
            Err(_) => false,
        };
        state.with(|s| {
            if success
                && s.bind_time(gate, c).is_some()
                && let Some(session) = s.session(c)
            {
                let old = std::mem::replace(&mut session.phase, Phase::Bound);
                s.cancel_timer(old);
            } else {
                s.reject(gate, c);
            }
        });
    }
}
impl<C: AdmissionCodec> ActorService for Sessions<C> {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        "watchdog",
        env!("CARGO_PKG_VERSION"),
        &[Role::new(watchdog::ROLE, watchdog::REVISION, C::ID)],
        &[Role::new(gateway::ROLE, gateway::REVISION, C::ID)],
        &[
            Self::OPEN.descriptor(),
            Self::CLOSE.descriptor(),
            Self::CHECK.descriptor(),
            Self::BIND.descriptor(),
            Self::KICK.descriptor(),
        ],
    );
    fn create(ctx: Context, config: &[u8]) -> Result<Self, Status> {
        let cfg: Settings =
            toml::from_str(std::str::from_utf8(config).map_err(|_| Status::PROTOCOL)?)
                .map_err(|_| Status::PROTOCOL)?;
        if !(1..=60_000).contains(&cfg.admission_timeout_ms)
            || !(1..=4096).contains(&cfg.max_sessions)
            || std::iter::once(&cfg.gate_alias)
                .chain(&cfg.default_target_alias)
                .chain(&cfg.admission_policy_alias)
                .any(|v| v.is_empty() || v.len() > 128)
        {
            return Err(Status::PROTOCOL);
        }
        Ok(Self {
            ctx,
            cfg,
            gate: None,
            admission: None,
            sessions: BTreeMap::new(),
            retry: None,
            marker: PhantomData,
        })
    }
    fn handle(state: State<Self>, msg: Incoming) -> Handler {
        let gate = msg.source();
        if !matches!(msg.entry(), Entry::Service) || !state.with(|s| s.is_gate(gate)) {
            return msg.reject(Status::DENIED);
        }
        match msg.wire().key {
            k if k == Self::OPEN.descriptor().wire.key => {
                msg.receive(Self::OPEN, move |q| Self::open(state, gate, q))
            }
            k if k == Self::CLOSE.descriptor().wire.key => msg
                .receive(Self::CLOSE, move |q| async move {
                    state.with(|s| s.close(gate, &q.conn))
                }),
            _ => msg.reject(Status::UNKNOWN_METHOD),
        }
    }
    fn control(&mut self, op: Control<'_>) -> Progress {
        Progress::idle(match op {
            Control::Prepare => self.prepare().err().unwrap_or(Status::OK),
            Control::Start if self.gate.is_some() => Status::OK,
            Control::Event(Event::Timer(h)) => {
                self.tick(h);
                Status::OK
            }
            Control::Event(Event::ResourceGone(t, a)) => {
                self.gone(t, a, true);
                Status::OK
            }
            _ => Status::NOT_READY,
        })
    }
    fn cleanup(&mut self, e: Event<'_>) -> Status {
        if let Event::ResourceGone(t, a) = e {
            self.gone(t, a, false);
            Status::OK
        } else {
            Status::NOT_READY
        }
    }
}
