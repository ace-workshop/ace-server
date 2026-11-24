//! 玩家流程的独立样例库，不提供内置角色。Echo 由 SDK 自动回复；主动推送是另一个单向业务动作。
use ace_example_protocol::{Echo, EchoReply, echo};
#[cfg(not(feature = "codec_ace"))]
use ace_fixture_codec::FixedCodec as Selected;
use ace_fixture_codec::notice;
use ace_net_proto::{Frame, FrameLimits, Message};
use ace_role_gateway as gateway;
use ace_role_watchdog as watchdog;
#[cfg(feature = "codec_ace")]
use ace_server_sdk::AceCodec as Selected;
use ace_server_sdk::{
    Actor, ActorService, Context,
    Direction::*,
    Handler, Incoming, RpcMethod, ServiceAddress, State, Status, TypedRpcError,
    codec::{Limits, NoReply},
    native::*,
};
const ECHO: RpcMethod<Echo, EchoReply> = RpcMethod::call(echo::<Selected>(), Receive)
    .local_input()
    .local_output(65536);
const NOTICE: RpcMethod<Echo, NoReply> = RpcMethod::send(notice::<Selected>(), Receive);
const PUSH: RpcMethod<gateway::SendToSession, gateway::Ack> =
    RpcMethod::call(gateway::send_to_session::<Selected>(), Both);
const ONLINE: RpcMethod<gateway::Online, gateway::OnlineCount> =
    RpcMethod::call(gateway::online::<Selected>(), Send);
const CHECK: RpcMethod<watchdog::CheckAdmission, watchdog::AdmissionDecision> =
    RpcMethod::call(watchdog::check_admission::<Selected>(), Receive);
const BIND: RpcMethod<gateway::Bind, gateway::Ack> =
    RpcMethod::call(gateway::bind::<Selected>(), Send)
        .local_input()
        .local_output(size_of::<gateway::Ack>() as u64);
const KICK: RpcMethod<gateway::Kick, gateway::Ack> =
    RpcMethod::call(gateway::kick::<Selected>(), Send)
        .local_input()
        .local_output(size_of::<gateway::Ack>() as u64);
// Denied 是本机能力结果；已接纳请求不能把它当作 wire 错误，须收到无体 Protocol。
fn expect_denial<T>(result: Result<T, TypedRpcError>) -> Result<(), TypedRpcError> {
    if matches!(result, Err(TypedRpcError::Status(Status::PROTOCOL))) {
        Ok(())
    } else {
        Err(Status::PROTOCOL.into())
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Echo,
    Policy,
    ExplicitPolicy,
}
struct Player {
    ctx: Context,
    mode: Mode,
    demo_attempts: [u8; 2],
}
fn log(ctx: &Context, s: &str) {
    ctx.command(HostOp::Log(0, s.into()));
}
fn push_frame() -> Result<Vec<u8>, TypedRpcError> {
    let m = notice::<Selected>();
    let mut dst = [0; 128];
    let body = m.encode_input(
        &Echo {
            data: b"pushed".into(),
        },
        &mut dst,
        &mut [0; 2048],
        Limits::default(),
    )?;
    let mut out = vec![0; 104 + body.len()];
    Frame {
        wire: m.wire(),
        message: Message::Send,
        body,
    }
    .encode_into(
        &mut out,
        FrameLimits {
            body_bytes: 128,
            response_bytes: 0,
            timeout_ms: 0,
        },
    )
    .map_err(|_| Status::PROTOCOL)?;
    Ok(out)
}
impl ActorService for Player {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        "example.player",
        env!("CARGO_PKG_VERSION"),
        &[],
        &[],
        &[
            Method {
                client_callable: 1,
                ..ECHO.descriptor()
            },
            Method {
                client_callable: 1,
                ..NOTICE.descriptor()
            },
            PUSH.descriptor(),
            ONLINE.descriptor(),
            CHECK.descriptor(),
            BIND.descriptor(),
            KICK.descriptor(),
        ],
    );
    fn create(ctx: Context, cfg: &[u8]) -> Result<Self, Status> {
        let mode = match cfg {
            b"echo" => Mode::Echo,
            b"policy" => Mode::Policy,
            b"explicit-policy" => Mode::ExplicitPolicy,
            _ => return Err(Status::PROTOCOL),
        };
        Ok(Self {
            ctx,
            mode,
            demo_attempts: [0; 2],
        })
    }
    fn control(&mut self, op: Control<'_>) -> Progress {
        Progress::idle(match op {
            Control::Prepare | Control::Start => Status::OK,
            _ => Status::NOT_READY,
        })
    }
    fn handle(state: State<Self>, msg: Incoming) -> Handler {
        let (ctx, mode) = state.with(|s| (s.ctx.clone(), s.mode));
        let policy = mode != Mode::Echo;
        let source = msg.source();
        // 同一合法推送信封由另一个实际实例提交；令牌仍来自已绑定业务 handler 的受控元数据。
        if policy
            && msg.wire().key == PUSH.descriptor().wire.key
            && matches!(msg.entry(), Entry::Service)
        {
            return msg.handle(PUSH, move |q| async move {
                if ctx.resolve("player")? != source {
                    return Err(Status::DENIED.into());
                }
                expect_denial(
                    ctx.call(PUSH, ctx.resolve("gate")?, (*q).clone(), 1000, 1024)?
                        .await,
                )?;
                log(&ctx, "policy-other-push-denied");
                Ok(gateway::Ack {})
            });
        }
        if policy
            && msg.wire().key == CHECK.descriptor().wire.key
            && matches!(msg.entry(), Entry::Service)
        {
            return msg.handle(CHECK, move |q| async move {
                use watchdog::AdmissionDecision::*;
                log(&ctx, "policy-check");
                let conn = q.conn.clone();
                let audit = |outcome: &str| {
                    log(
                        &ctx,
                        &format!(
                            "policy-{outcome} {:?} conn={}:{} watchdog={}",
                            conn.transport,
                            conn.id,
                            conn.generation,
                            source.bits()
                        ),
                    )
                };
                audit("query");
                // 同一策略对每种传输依次返回三个失败结局。每次都给原本能过 demo 的
                // 同一个标记，才能检出仅在 Unavailable 或 Timeout 时才启用的 demo 后备。
                let token = if mode == Mode::ExplicitPolicy && q.handshake.token == "demo" {
                    state.with(|s| {
                        let attempts = &mut s.demo_attempts
                            [usize::from(conn.transport == gateway::Transport::Kcp)];
                        let previous = *attempts;
                        *attempts = attempts.saturating_add(1);
                        match previous {
                            0 => "deny",
                            1 => "unavailable",
                            _ => "late",
                        }
                    })
                } else {
                    q.handshake.token.as_str()
                };
                let decision = match token {
                    "deny" => Denied { code: 1 },
                    "unavailable" => Unavailable,
                    "late" | "closed" => {
                        let timeout = token == "late";
                        if timeout {
                            audit("wait");
                        }
                        ctx.timer(if timeout { 700 } else { 150 }, 0)?
                            .tick()
                            .await?;
                        log(
                            &ctx,
                            if timeout {
                                "policy-late"
                            } else {
                                "policy-after-close"
                            },
                        );
                        Admitted {
                            target: ctx.resolve("player")?.bits(),
                        }
                    }
                    "permissions" => {
                        let gate = ctx.resolve("gate")?;
                        let target = ctx.resolve("player")?.bits();
                        expect_denial(
                            ctx.call(
                                BIND,
                                gate,
                                gateway::Bind {
                                    conn: q.conn.clone(),
                                    target,
                                },
                                1000,
                                1024,
                            )?
                            .await,
                        )?;
                        expect_denial(
                            ctx.call(
                                KICK,
                                gate,
                                gateway::Kick {
                                    conn: q.conn.clone(),
                                },
                                1000,
                                1024,
                            )?
                            .await,
                        )?;
                        expect_denial(
                            ctx.call(ONLINE, gate, gateway::Online {}, 1000, 1024)?
                                .await,
                        )?;
                        log(&ctx, "policy-bind-kick-online-denied");
                        Admitted { target }
                    }
                    "elsewhere" => Admitted {
                        target: ServiceAddress::try_from((2_u64 << 48) | 1)
                            .map_err(|_| Status::PROTOCOL)?
                            .bits(),
                    },
                    token
                        if matches!(
                            (mode, token),
                            (Mode::Policy, "demo") | (Mode::ExplicitPolicy, "permit")
                        ) =>
                    {
                        Admitted {
                            target: ctx.resolve("player")?.bits(),
                        }
                    }
                    _ => Denied { code: 2 },
                };
                audit(match decision {
                    Admitted { .. } => "admitted",
                    Denied { .. } => "denied",
                    Unavailable => "unavailable",
                });
                Ok(decision)
            });
        }
        let Entry::Player { push_session } = msg.entry() else {
            return msg.reject(Status::DENIED);
        };
        if policy {
            return msg.reject(Status::DENIED);
        }
        if msg.wire().key == ECHO.descriptor().wire.key {
            return msg.handle(ECHO, move |q| async move {
                log(&ctx, &format!("player-echo gate={}", source.bits()));
                if q.data == b"held" {
                    log(&ctx, "hold-callback");
                }
                if q.data == b"pending-stop" {
                    let child = ctx
                        .create_service("example.player", b"echo", Some("created-player"))?
                        .await?;
                    // 早于关停夹具的第二阶段结束；全局撤销普通 poll 后不能恢复本任务。
                    let mut timer = ctx.timer(350, 0)?;
                    struct Held(Context);
                    impl Drop for Held {
                        fn drop(&mut self) {
                            log(&self.0, "player-stop-task-dropped");
                        }
                    }
                    let _held = Held(ctx.clone());
                    log(&ctx, &format!("player-stop-pending child={}", child.bits()));
                    timer.tick().await?;
                    log(&ctx, "player-stop-resumed");
                }
                if q.data == b"slow" {
                    ctx.timer(400, 0)?.tick().await?;
                    log(&ctx, "player-slow-finished");
                }
                Ok(EchoReply {
                    data: q.data.clone(),
                })
            });
        }
        msg.receive(NOTICE, move |q| async move {
            let data = q.data.as_slice();
            let action = async {
                match data {
                    b"push" | b"bad-token" | b"other-caller" => {
                        let token = push_session.into_option().ok_or(Status::DENIED)?.get();
                        let r = ctx
                            .call(
                                PUSH,
                                if data == b"other-caller" {
                                    ctx.resolve("admission")?
                                } else {
                                    source
                                },
                                gateway::SendToSession {
                                    push_session: if data == b"bad-token" { 0 } else { token },
                                    frame: push_frame()?,
                                },
                                1000,
                                1024,
                            )?
                            .await;
                        if data == b"bad-token" {
                            expect_denial(r)?;
                            log(&ctx, "player-bad-token-denied");
                        } else {
                            r?;
                            if data == b"push" {
                                log(&ctx, "player-pushed");
                            }
                        }
                    }
                    b"online" => {
                        let n = ctx
                            .call(ONLINE, source, gateway::Online {}, 1000, 1024)?
                            .await?
                            .count;
                        log(&ctx, &format!("player-online count={n}"));
                    }
                    _ => log(&ctx, "player-oneway"),
                }
                Ok::<_, TypedRpcError>(())
            }
            .await;
            if let Err(e) = action {
                log(&ctx, &format!("player-action-error {e}"));
            }
        })
    }
}
#[abi_stable::export_root_module]
fn root() -> ServiceModuleRef {
    ace_server_sdk::export::<Actor<Player>>()
}
