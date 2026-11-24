//! 正式 runtime 的跨库夹具；复用同一份标量、字符串和向量以及真实 ace codec 计数器。
use super::{
    Codec, DECODE, ENCODE, PeerSample, RELEASED, Relaxed, Sample, Selected, WIRE_DIGEST, observed,
};
use ace_codec::{CodecError, Format, Limits, NoReply, Schema};
pub use ace_server_sdk::native::ServiceModuleRef;
use ace_server_sdk::{
    Actor, ActorService, Context, Direction, FromLocal, Handler, Incoming, LocalDto, RpcMethod,
    Service, ServiceAddress, State, Status, TypedMethod, TypedRpcError, native::*,
};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::{Poll, Waker},
};
// 线 DTO 由两个 codec 共用；只有这个服务侧类型提供本库的稳定表示及析构计数。
struct Wire(PeerSample);
impl std::ops::Deref for Wire {
    type Target = PeerSample;
    fn deref(&self) -> &PeerSample {
        &self.0
    }
}
impl Schema for Wire {
    const CANONICAL: &'static str = include_str!("../wire.txt");
    const DIGEST: [u8; 32] = WIRE_DIGEST;
}
impl LocalDto for Wire {
    type Stable = Sample;
    fn into_stable(self) -> Sample {
        Sample {
            id: self.id,
            text: self.0.text.into(),
            samples: self.0.samples.into(),
        }
    }
}
impl FromLocal for Wire {
    fn from_stable(x: &Sample) -> Self {
        Self(PeerSample {
            id: x.id,
            text: x.text.as_str().into(),
            samples: x.samples.as_slice().into(),
        })
    }
}
static LAST_ENCODED_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct Counted;
impl Format for Counted {
    const ID: [u32; 3] = Selected::ID;
}
impl Codec<Wire> for Counted {
    fn encode_into(v: &Wire, d: &mut [u8], s: &mut [u8], l: Limits) -> Result<usize, CodecError> {
        ENCODE.fetch_add(1, Relaxed);
        let n = Selected::encode_into(&v.0, d, s, l)?;
        LAST_ENCODED_BYTES.store(n, Relaxed);
        Ok(n)
    }
    fn decode(b: &[u8], l: Limits) -> Result<Wire, CodecError> {
        DECODE.fetch_add(1, Relaxed);
        Selected::decode(b, l).map(Wire)
    }
}
// 仅夹具管理输入：恰好一个小端 u64，同一结构作返回值，无网络或 HostApi 能力。
struct Word(u64);
impl Schema for Word {
    const CANONICAL: &'static str = "test.runtime.Word/1:u64le";
    const DIGEST: [u8; 32] = WORD_DIGEST;
}
const WORD_DIGEST: [u8; 32] = [
    105, 140, 195, 169, 142, 147, 78, 240, 234, 169, 116, 37, 222, 35, 177, 167, 10, 34, 195, 34,
    61, 248, 64, 49, 52, 175, 200, 247, 109, 217, 193, 181,
];
struct Little;
impl Format for Little {
    const ID: [u32; 3] = [0xa31, 1, 0];
}
impl Codec<Word> for Little {
    fn encode_into(v: &Word, d: &mut [u8], _: &mut [u8], _: Limits) -> Result<usize, CodecError> {
        d.get_mut(..8)
            .ok_or(CodecError::Limit)?
            .copy_from_slice(&v.0.to_le_bytes());
        Ok(8)
    }
    fn decode(b: &[u8], _: Limits) -> Result<Word, CodecError> {
        Ok(Word(u64::from_le_bytes(
            b.try_into().map_err(|_| CodecError::Invalid)?,
        )))
    }
}
const CMD: RpcMethod<Word, Word> = RpcMethod::call(
    TypedMethod::<Little, Word, Word>::call(100, 8),
    Direction::Both,
);
const ONEWAY: RpcMethod<Wire, NoReply> = RpcMethod::send(
    TypedMethod::<Counted, Wire, NoReply>::one_way(104),
    Direction::Both,
)
.local_input();
const fn sample(key: u32, caller: bool) -> RpcMethod<Wire, Wire> {
    let m = RpcMethod::call(
        TypedMethod::<Counted, Wire, Wire>::call(key, 1024),
        Direction::Both,
    );
    let m = if caller || key != 103 {
        m.local_input()
    } else {
        m
    };
    if caller || key == 101 {
        m.local_output(1024)
    } else {
        m
    }
}
fn input(id: u64) -> Wire {
    Wire(PeerSample {
        id,
        text: "跨库".into(),
        samples: vec![2, 5, 8],
    })
}
fn ok(r: HostResult) -> Result<u64, Status> {
    if r.status == Status::OK {
        Ok(r.value)
    } else {
        Err(r.status)
    }
}
fn log(ctx: &Context, s: &str) {
    ctx.command(HostOp::Log(0, s.into()));
}
type Create = Pin<Box<dyn Future<Output = Result<ServiceAddress, Status>> + Send>>;
struct Fixture<const CALLER: bool> {
    ctx: Context,
    config: Vec<u8>,
    ticket: Option<Create>,
    released: bool,
    wait: Vec<Waker>,
    init: Option<ResourceHandle>,
    ticks: u64,
    marks: std::collections::BTreeSet<u64>,
}
const fn methods(c: bool) -> [Method; 8] {
    [
        CMD.descriptor(),
        sample(101, c).descriptor(),
        sample(102, c).descriptor(),
        sample(103, c).descriptor(),
        ONEWAY.descriptor(),
        MARK.descriptor(),
        Method {
            client_callable: 1,
            ..sample(106, c).descriptor()
        },
        ace_server_sdk::bytes_method(ace_role_cluster::resolve::<Selected>(), Direction::Send),
    ]
}
const CALLER_METHODS: [Method; 8] = methods(true);
const ECHO_METHODS: [Method; 8] = methods(false);
const MARK: RpcMethod<Word, NoReply> = RpcMethod::send(
    TypedMethod::<Little, Word, NoReply>::one_way(105),
    Direction::Both,
);
impl<const C: bool> ActorService for Fixture<C> {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        if C { "test.caller" } else { "test.echo" },
        "runtime",
        &[],
        &[],
        if C { &CALLER_METHODS } else { &ECHO_METHODS },
    );
    fn create(ctx: Context, config: &[u8]) -> Result<Self, Status> {
        log(&ctx, "create");
        if config == b"reject" {
            return Err(Status::DENIED);
        }
        Ok(Self {
            ctx,
            config: config.into(),
            ticket: None,
            released: false,
            wait: Vec::new(),
            init: None,
            ticks: 0,
            marks: Default::default(),
        })
    }
    fn handle(state: State<Self>, msg: Incoming) -> Handler {
        let key = msg.wire().key;
        let measure = state.with(|s| s.config.starts_with(b"measure-"));
        if key == 105 {
            if state.with(|s| s.config == b"peer-probe" && msg.source() == s.ctx.address()) {
                let ctx = state.with(|s| s.ctx.clone());
                return msg.receive(MARK, move |q| async move {
                    let r = peer_probe::<C>(&ctx, q.0).await;
                    match r {
                        Ok(n) => log(&ctx, &format!("probe {} ok {n}", q.0)),
                        Err(e) => log(&ctx, &format!("probe {} error {e:?}", q.0)),
                    }
                });
            }
            return msg.receive(MARK, move |q| async move {
                state.with(|s| {
                    assert!(s.marks.insert(q.0));
                    log(&s.ctx, "marked");
                });
            });
        }
        if key == 104 {
            return msg.receive(ONEWAY, move |q| async move {
                assert_eq!(q.id, 9);
                state.with(|s| log(&s.ctx, "oneway"));
            });
        }
        let ctx = state.with(|s| s.ctx.clone());
        if key != 100 {
            if let Entry::Player { push_session } = msg.entry() {
                log(
                    &ctx,
                    &format!(
                        "peer-player source={} push={}",
                        msg.source().bits(),
                        push_session.into_option().map_or(0, |t| t.get())
                    ),
                );
            }
            if state.with(|s| s.config == b"peer-echo") {
                log(
                    &ctx,
                    &format!(
                        "peer-input source={} target={} method={key}",
                        msg.source().bits(),
                        ctx.address().bits()
                    ),
                );
            }
            return msg.handle(sample(key, C), move |q| async move {
                if !measure {
                    log(&ctx, "sample-poll");
                }
                if q.id == 666 {
                    panic!("夹具：已接纳受管输入的 handler 故障")
                }
                if q.id == 667 {
                    // 真实响应编码失败，不提交部分缓冲。
                    let mut v = input(42);
                    v.0.text = "x".repeat(ctx.limits().payload_bytes.get() as usize + 1);
                    return Ok(v);
                }
                if q.id == 9000 {
                    state.with(|s| s.released = false);
                    poll_fn(|cx| {
                        state.with(|s| {
                            if s.released {
                                Poll::Ready(())
                            } else {
                                s.wait.push(cx.waker().clone());
                                log(&ctx, "holding-input");
                                Poll::Pending
                            }
                        })
                    })
                    .await;
                } else if !measure {
                    ctx.timer(1, 0)?.tick().await?;
                }
                Ok(input(
                    observed(q.id, &q.text, &q.samples).ok_or(Status::PROTOCOL)?,
                ))
            });
        }
        msg.handle(CMD, move |q| async move {
            let addr = || {
                ServiceAddress::try_from(ok(ctx.command(HostOp::Resolve("echo".into())))?)
                    .map_err(|_| Status::PROTOCOL)
            };
            let command = q.0;
            if measure {
                let value = match command {
                    49 => ENCODE.load(Relaxed) as u64,
                    50 => DECODE.load(Relaxed) as u64,
                    51 => LAST_ENCODED_BYTES.load(Relaxed) as u64,
                    address => {
                        let dst =
                            ServiceAddress::try_from(address).map_err(|_| Status::PROTOCOL)?;
                        let key = if state.with(|s| s.config == b"measure-local") {
                            101
                        } else {
                            103
                        };
                        ctx.call(sample(key, C), dst, input(27), 2000, 1024)?
                            .await?
                            .id
                    }
                };
                return Ok(Word(value));
            }
            let result = match command {
                1..=3 => {
                    let r = ctx
                        .call(sample(100 + q.0 as u32, C), addr()?, input(27), 2000, 1024)?
                        .await?;
                    assert_eq!(r.id, 42);
                    r.id
                }
                4 => ENCODE.load(Relaxed) as u64 * 100 + DECODE.load(Relaxed) as u64,
                5 => {
                    ctx.send(ONEWAY, addr()?, input(9))?;
                    1
                }
                6 => {
                    let r = ctx.call(sample(101, C), addr()?, input(27), 2000, 0)?.await;
                    assert!(matches!(r, Err(TypedRpcError::Status(Status::TOO_SMALL))));
                    u64::from(Status::TOO_SMALL.0)
                }
                7 => {
                    drop(ctx.call(sample(101, C), addr()?, input(27), 2000, 1024)?);
                    1
                }
                8 => {
                    log(&ctx, "parallel");
                    8
                }
                9 => {
                    let mut first = true;
                    poll_fn(|cx| {
                        log(&ctx, "user-poll");
                        if std::mem::take(&mut first) {
                            std::thread::scope(|s| {
                                for _ in 0..3 {
                                    let w = cx.waker().clone();
                                    s.spawn(move || {
                                        for _ in 0..32 {
                                            w.wake_by_ref()
                                        }
                                    });
                                }
                            });
                            Poll::Pending
                        } else {
                            Poll::Ready(())
                        }
                    })
                    .await;
                    log(&ctx, "wake-resumed");
                    9
                }
                10 | 34 => {
                    if command == 34 {
                        state.with(|s| s.released = false);
                    }
                    let mut poll = 0;
                    poll_fn(|cx| {
                        poll += 1;
                        state.with(|s| {
                            if s.released {
                                Poll::Ready(())
                            } else {
                                if poll == 1 {
                                    log(
                                        &ctx,
                                        if command == 34 {
                                            "holding-word"
                                        } else {
                                            "pending"
                                        },
                                    );
                                    s.wait.push(cx.waker().clone());
                                }
                                Poll::Pending
                            }
                        })
                    })
                    .await;
                    10
                }
                11 => {
                    let wakes = state.with(|s| {
                        s.released = true;
                        std::mem::take(&mut s.wait)
                    });
                    for w in wakes {
                        w.wake();
                    }
                    11
                }
                12 => {
                    let mut t = ctx.timer(1, 1)?;
                    t.tick().await?;
                    t.tick().await?;
                    t.cancel();
                    12
                }
                13 => panic!("夹具：普通 handler 故障"),
                14 => {
                    log(&ctx, "hold-callback");
                    14
                }
                15 => {
                    let f = ctx.call(CMD, addr()?, Word(34), 100, 8)?;
                    assert!(matches!(
                        f.await,
                        Err(TypedRpcError::Status(Status::TIMEOUT))
                    ));
                    u64::from(Status::TIMEOUT.0)
                }
                32 => ctx
                    .create_service("test.caller", b"", Some("source"))?
                    .await?
                    .bits(),
                33 | 38 => {
                    let mut qv = input(9000);
                    qv.0.samples.reserve(128);
                    ctx.call(
                        sample(if q.0 == 33 { 101 } else { 103 }, C),
                        addr()?,
                        qv,
                        2000,
                        1024,
                    )?
                    .await?
                    .id
                }
                35 => {
                    let bits = 19u64.to_le_bytes();
                    let r = ctx
                        .raw_call(Outbound {
                            destination: addr()?.bits(),
                            wire: CMD.descriptor().wire,
                            body: Body {
                                representation: Representation::Bytes,
                                bytes: bits.as_slice().into(),
                            },
                            timeout_ms: 2000,
                            reply: ReplyBudget {
                                representation: Representation::Bytes,
                                ceiling: 8,
                            },
                            entry: Entry::Service,
                        })?
                        .await?;
                    u64::from_le_bytes(r[..].try_into().map_err(|_| Status::PROTOCOL)?)
                }
                37 => {
                    drop(ctx.call(CMD, addr()?, Word(34), 100, 8)?);
                    37
                }
                16 => RELEASED.load(Relaxed) as u64,
                17 => {
                    let mut calls = Vec::new();
                    for _ in 0..4 {
                        calls.push(ctx.call(CMD, addr()?, Word(19), 2000, 8)?);
                    }
                    state.with(|s| s.released = false);
                    poll_fn(|cx| {
                        state.with(|s| {
                            if s.released {
                                Poll::Ready(())
                            } else {
                                s.wait.push(cx.waker().clone());
                                log(&ctx, "before-await");
                                Poll::Pending
                            }
                        })
                    })
                    .await;
                    let mut n = 0;
                    for f in calls {
                        n = f.await?.0;
                    }
                    n
                }
                18 => {
                    std::future::pending::<()>().await;
                    18
                }
                19 => {
                    log(&ctx, "word-poll");
                    19
                }
                28 => {
                    log(&ctx, "fault-probe");
                    panic!("夹具：poll panic 后清理再 panic")
                }
                29 => {
                    let r = ctx.command(HostOp::ServiceStop(addr()?.bits()));
                    r.status.0.into()
                }
                41 => {
                    let target =
                        ServiceAddress::try_from(ok(ctx.command(HostOp::Resolve("child".into())))?)
                            .map_err(|_| Status::PROTOCOL)?;
                    ctx.call(sample(101, C), target, input(666), 2000, 1024)?
                        .await?
                        .id
                }
                42 | 43 | 46 | 47 => command, // 下方实库接收入口的背压/故障开关。
                44 => {
                    let mut v = input(27);
                    v.0.text = "x".repeat(ctx.limits().payload_bytes.get() as usize + 1);
                    match ctx.call(sample(103, C), addr()?, v, 2000, 1024) {
                        #[cfg(not(feature = "codec_ace"))]
                        Err(TypedRpcError::Codec(CodecError::Limit)) => {
                            log(&ctx, "提交前编码失败：Limit");
                            u64::from(Status::TOO_SMALL.0)
                        }
                        #[cfg(feature = "codec_ace")]
                        Err(TypedRpcError::Codec(CodecError::Ace(e))) => {
                            log(&ctx, &format!("提交前编码失败：{e:?}"));
                            e.code() as u64
                        }
                        _ => return Err(Status::PROTOCOL.into()),
                    }
                }
                45 => {
                    let r = ctx.call(CMD, addr()?, Word(34), 2000, 8)?.await;
                    assert!(matches!(r, Err(TypedRpcError::Codec(CodecError::Invalid))));
                    log(&ctx, "成功体本机解码失败：Invalid");
                    45
                }
                48 => {
                    let r = ctx
                        .call(sample(103, C), addr()?, input(667), 2000, 1024)?
                        .await;
                    assert!(matches!(r, Err(TypedRpcError::Status(Status::PROTOCOL))));
                    48
                }
                40 => ok(ctx.command(HostOp::TimerSet(3000, 2000)))?,
                30 => state.with(|s| s.ticks),
                31 => state.with(|s| s.marks.len() as u64),
                20 => ctx
                    .create_service("test.echo", b"", Some("child"))?
                    .await?
                    .bits(),
                21 => {
                    let f = ctx.create_service("test.echo", b"", Some("unclaimed"))?;
                    state.with(|s| s.ticket = Some(Box::pin(f)));
                    21
                }
                22 => {
                    let t = state.with(|s| s.ticket.take());
                    drop(t);
                    22
                }
                23 => {
                    let e = ctx
                        .create_service("test.echo", b"reject", Some("rejected"))?
                        .await
                        .unwrap_err();
                    u64::from(e.0)
                }
                24 => {
                    let f = state.with(|s| s.ticket.take()).ok_or(Status::NOT_FOUND)?;
                    f.await?.bits()
                }
                25 => {
                    ok(ctx.command(HostOp::ServiceStop(ctx.address().bits())))?;
                    25
                }
                26 => {
                    let r = ctx.command(HostOp::ServiceSubscribe(addr()?.bits()));
                    ok(r)?
                }
                27 => {
                    let f = ctx.call(CMD, addr()?, Word(19), 2000, 8)?;
                    state.with(|s| s.released = false);
                    poll_fn(|cx| {
                        state.with(|s| {
                            if s.released {
                                Poll::Ready(())
                            } else {
                                s.wait.push(cx.waker().clone());
                                Poll::Pending
                            }
                        })
                    })
                    .await;
                    f.await?.0
                }
                _ => return Err(Status::UNKNOWN_METHOD.into()),
            };
            Ok(Word(result))
        })
    }
    fn control(&mut self, op: Control<'_>) -> Progress {
        let s = match op {
            Control::Prepare if self.config == b"peer-probe" => {
                match self.ctx.net_listen(Transport::Tcp, "127.0.0.1:0") {
                    Ok(h) => {
                        self.init = Some(h);
                        Status::PENDING
                    }
                    Err(e) => e,
                }
            }
            Control::LifecycleEvent(
                LifecycleAction::Prepare,
                Event::Network(NetEvent::ListenReady(h, Transport::Tcp, p)),
            ) if self.init == Some(h) => {
                self.init = None;
                log(&self.ctx, &format!("probe-listen {}", p.as_str()));
                self.ctx
                    .command(HostOp::LifecycleReady(LifecycleAction::Prepare))
                    .status
            }
            Control::Event(Event::Network(e)) if self.config == b"peer-probe" => match e {
                NetEvent::Data(_, b) => {
                    let l = ace_net_proto::FrameLimits {
                        body_bytes: 8,
                        response_bytes: 0,
                        timeout_ms: 1,
                    };
                    match ace_net_proto::Frame::from_player(b.as_slice(), l) {
                        Ok(f)
                            if f.message == ace_net_proto::Message::Send
                                && ace_server_sdk::native_wire(f.wire)
                                    == MARK.descriptor().wire
                                && f.body.len() == 8 =>
                        {
                            self.ctx
                                .raw_send(Outbound {
                                    destination: self.ctx.address().bits(),
                                    wire: MARK.descriptor().wire,
                                    body: Body {
                                        representation: Representation::Bytes,
                                        bytes: f.body.into(),
                                    },
                                    entry: Entry::Service,
                                    timeout_ms: 0,
                                    reply: ReplyBudget {
                                        representation: Representation::Bytes,
                                        ceiling: 0,
                                    },
                                })
                                .err()
                                .unwrap_or(Status::OK)
                        }
                        _ => Status::PROTOCOL,
                    }
                }
                NetEvent::Closed(c, _) => {
                    self.ctx.net_release_closed(c).err().unwrap_or(Status::OK)
                }
                NetEvent::Accepted(..) | NetEvent::Writable(_) | NetEvent::Error(..) => Status::OK,
                _ => Status::PROTOCOL,
            },
            Control::Prepare if self.config == b"timer-init" => {
                let r = self.ctx.command(HostOp::TimerSet(1, 0));
                if r.status != Status::OK {
                    return Progress::idle(r.status);
                }
                self.init = Some(ResourceHandle {
                    id: r.value,
                    generation: r.generation,
                });
                Status::PENDING
            }
            Control::Prepare if self.config == b"dependency" => {
                self.ctx
                    .command(HostOp::ResolvePrepared("echo".into()))
                    .status
            }
            Control::Prepare if self.config == b"pause-prepare" => {
                native_pause(&self.ctx, "native-prepare");
                Status::OK
            }
            Control::Prepare if self.config == b"never-ready" => {
                log(&self.ctx, "prepare-pending");
                Status::PENDING
            }
            Control::Prepare | Control::Start => Status::OK,
            Control::LifecycleEvent(LifecycleAction::Prepare, Event::Timer(h))
                if self.init == Some(h) =>
            {
                self.init = None;
                self.ctx
                    .command(HostOp::LifecycleReady(LifecycleAction::Prepare))
                    .status
            }
            Control::LifecyclePoll(LifecycleAction::Prepare, _) if self.init.is_none() => {
                Status::OK
            }
            Control::Event(Event::Network(NetEvent::ListenReady(_, Transport::Tcp, endpoint))) => {
                log(&self.ctx, &format!("net-listen Tcp {}", endpoint.as_str()));
                log(&self.ctx, "runtime-listen-ready");
                Status::OK
            }
            Control::Event(Event::ResourceGone(..)) => {
                self.released = true;
                for w in std::mem::take(&mut self.wait) {
                    w.wake();
                }
                log(&self.ctx, "gone");
                Status::OK
            }
            Control::Event(Event::Timer(_)) => {
                self.ticks += 1;
                log(&self.ctx, "tick");
                Status::OK
            }
            _ => Status::NOT_READY,
        };
        Progress::idle(s)
    }
    fn cancel(&mut self) {
        log(&self.ctx, "cancel");
        if self.config == b"fault-cleanup" {
            log(&self.ctx, "hold-callback");
        }
        if self.config == b"pause-cancel" {
            native_pause(&self.ctx, "native-cancel");
        }
        if self.config == b"panic-cancel" {
            panic!("夹具：再次 panic 必须 fail-stop")
        }
    }
}
// 只在隔离的 signal 子进程中模拟不让出 worker 的可信回调；即使管理方失效也自行返回。
fn native_pause(ctx: &Context, phase: &str) {
    log(ctx, phase);
    std::thread::sleep(std::time::Duration::from_millis(600));
    log(ctx, "native-returned");
}
// 私有回环指令仅触发普通 caller Actor，不构造对端回复、地址或 reply token。
async fn peer_probe<const C: bool>(ctx: &Context, word: u64) -> Result<u64, TypedRpcError> {
    let address = |alias: &str| {
        ServiceAddress::try_from(ok(ctx.command(HostOp::Resolve(alias.into())))?)
            .map_err(|_| Status::PROTOCOL)
    };
    let max = ServiceAddress::MAX_SEQUENCE;
    let command = word & max;
    let node = (word >> 48) as u16;
    if command == max - 5 {
        return ok(ctx.command(HostOp::ServiceStop(address("echo")?.bits()))).map_err(Into::into);
    }
    if command == max - 6 {
        return ctx
            .peer_connect(ctx.advertised_endpoint(), node)
            .map_or_else(|e| Ok(u64::from(e.0)), |_| Err(Status::PROTOCOL.into()));
    }
    if command == max - 7 {
        return ok(ctx.command(HostOp::ServiceStop(address("members")?.bits())))
            .map_err(Into::into);
    }
    if command == max - 8 {
        return Ok(ctx.call(CMD, address("echo")?, Word(11), 1000, 8)?.await?.0);
    }
    let target = if command >= max - 12 {
        let m = RpcMethod::call(ace_role_cluster::resolve::<Selected>(), Direction::Send);
        let r = ctx
            .call(
                m,
                address("members")?,
                ace_role_cluster::Resolve {
                    node,
                    alias: if command == max - 12 {
                        "caller"
                    } else {
                        "echo"
                    }
                    .into(),
                },
                1500,
                1024,
            )?
            .await?;
        ServiceAddress::try_from(r.address).map_err(|_| Status::PROTOCOL)?
    } else {
        ServiceAddress::try_from(word).map_err(|_| Status::PROTOCOL)?
    };
    if command == max || command == max - 12 {
        return Ok(target.bits());
    }
    if command == max - 2 {
        ctx.send(ONEWAY, target, input(9))?;
        return Ok(target.bits());
    }
    if command == max - 9 || command == max - 10 {
        let mut b = [0; 512];
        let n = Counted::encode_into(&input(27), &mut b, &mut [0; 4096], Limits::default())
            .map_err(TypedRpcError::Codec)?;
        let r = ctx
            .raw_call(Outbound {
                destination: target.bits(),
                wire: sample(if command == max - 9 { 106 } else { 101 }, C)
                    .descriptor()
                    .wire,
                timeout_ms: 1500,
                entry: Entry::Player {
                    push_session: std::num::NonZeroU64::new(71).into(),
                },
                body: Body {
                    representation: Representation::Bytes,
                    bytes: b[..n].into(),
                },
                reply: ReplyBudget {
                    representation: Representation::Bytes,
                    ceiling: 1024,
                },
            })?
            .await?;
        return Ok(Counted::decode(&r, Limits::default())
            .map_err(TypedRpcError::Codec)?
            .id);
    }
    let held = command == max - 3 || command == max - 4;
    let (en, de) = (ENCODE.load(Relaxed), DECODE.load(Relaxed));
    let r = ctx
        .call(
            sample(101, C),
            target,
            input(if held { 9000 } else { 27 }),
            if command == max - 4 { 250 } else { 2000 },
            if command == max - 11 { 0 } else { 1024 },
        )?
        .await?;
    log(
        ctx,
        &format!(
            "peer-result target={} value={} codec={}/{}",
            target.bits(),
            r.id,
            ENCODE.load(Relaxed) - en,
            DECODE.load(Relaxed) - de
        ),
    );
    Ok(r.id)
}
impl<const C: bool> Drop for Fixture<C> {
    fn drop(&mut self) {
        log(&self.ctx, "destroy");
    }
}
// 只在实库夹具中改变接收方的准入，宿主不伪造 Busy；放行后仍交同一个 SDK Actor。
enum NoticeGate {
    Open,
    Busy,
    FaultBeforeTake,
    FaultAfterTake,
}
struct Receiver<const C: bool> {
    actor: Actor<Fixture<C>>,
    ctx: Context,
    gate: NoticeGate,
    fault_on_cleanup: bool,
    measure: bool,
}
impl<const C: bool> Service for Receiver<C> {
    const DESCRIPTOR: Descriptor = Fixture::<C>::DESCRIPTOR;
    fn create(ctx: Context, config: &[u8]) -> Result<Self, Status> {
        Ok(Self {
            actor: Actor::create(ctx.clone(), config)?,
            ctx,
            gate: NoticeGate::Open,
            fault_on_cleanup: config == b"fault-cleanup",
            measure: config.starts_with(b"measure-"),
        })
    }
    fn on_message(&mut self, m: Message<'_>, slot: &mut LocalSlot) -> Status {
        let command = (m.request.wire == CMD.descriptor().wire)
            .then(|| {
                m.request
                    .body
                    .bytes
                    .as_slice()
                    .try_into()
                    .ok()
                    .map(u64::from_le_bytes)
            })
            .flatten();
        let r = self.actor.on_message(m, slot);
        if r == Status::OK {
            match command {
                Some(42) => self.gate = NoticeGate::Busy,
                Some(43) => self.gate = NoticeGate::Open,
                Some(46) => self.gate = NoticeGate::FaultBeforeTake,
                Some(47) => self.gate = NoticeGate::FaultAfterTake,
                _ => {}
            }
        }
        r
    }
    fn on_control(&mut self, op: Control<'_>, slot: &mut LocalSlot) -> Progress {
        if let Control::Event(e) = &op {
            if let NoticeGate::Busy = self.gate {
                let label = match e {
                    Event::ResourceGone(..) => "busy-gone",
                    Event::CreateReady(..) => "busy-create",
                    Event::Completion {
                        pending,
                        reply,
                        charge_token,
                    } => {
                        log(
                            &self.ctx,
                            &format!("暂拒完成 {pending} charge={charge_token}"),
                        );
                        match reply {
                            Reply::Success(b) if b.representation == Representation::LocalOwned => {
                                assert!(slot.is_some());
                                "busy-local"
                            }
                            Reply::Success(b) => {
                                assert_eq!(*charge_token == 0, b.bytes.is_empty());
                                "busy-bytes"
                            }
                            Reply::Error(_) => "busy-error",
                        }
                    }
                    _ => return self.actor.on_control(op, slot),
                };
                log(&self.ctx, label);
                return Progress::idle(Status::BUSY);
            }
            if matches!(e, Event::Completion { .. })
                && matches!(
                    self.gate,
                    NoticeGate::FaultBeforeTake | NoticeGate::FaultAfterTake
                )
            {
                assert!(slot.is_some());
                let _owned = matches!(self.gate, NoticeGate::FaultAfterTake).then(|| slot.take());
                panic!("夹具：完成回调故障，槽已取走={}", slot.is_none());
            }
        }
        let completion = match &op {
            Control::Event(Event::Completion {
                pending,
                charge_token,
                ..
            }) => Some((*pending, *charge_token)),
            _ => None,
        };
        let r = self.actor.on_control(op, slot);
        if r.status == Status::OK
            && !self.measure
            && let Some((id, charge)) = completion
        {
            log(&self.ctx, &format!("接纳完成 {id} charge={charge}"));
        }
        r
    }
    fn on_cleanup(&mut self, e: Event<'_>, slot: &mut LocalSlot) -> Status {
        if self.fault_on_cleanup && matches!(e, Event::Completion { .. }) && slot.is_some() {
            log(&self.ctx, "fault-local-cleanup");
            Status::FAULTED
        } else if matches!(self.gate, NoticeGate::Busy) {
            log(&self.ctx, "busy-cleanup");
            Status::BUSY
        } else {
            self.actor.on_cleanup(e, slot)
        }
    }
    fn cancel(&mut self) {
        self.actor.cancel()
    }
}
/// 为两个独立测试动态库分别生成正式的服务表。
pub fn export<const CALLER: bool>() -> ServiceModuleRef {
    ace_server_sdk::export::<Receiver<CALLER>>()
}

// 单列同一份 DTO 的移动/物化成本，不把无 codec 调用说成零分配。
#[test]
#[ignore = "tools/measure.py 的 release DTO 对照；不含 host 或布局检查"]
fn dto_conversion() {
    use std::{hint::black_box, time::Instant};
    const N: usize = 32_768;
    let inputs: Vec<_> = (0..N).map(|i| input(i as u64)).collect();
    let mut stable = Vec::with_capacity(N);
    let mut received = Vec::with_capacity(N);
    let start = Instant::now();
    for w in inputs {
        stable.push(black_box(w).into_stable());
    }
    let into = start.elapsed();
    let start = Instant::now();
    for value in &stable {
        received.push(Wire::from_stable(black_box(value)));
    }
    let from = start.elapsed();
    assert_eq!(black_box(received.last().unwrap()).id, (N - 1) as u64);
    use ace_server_local::StableMessage;
    let sample = &stable[0];
    println!(
        "dto samples={N} into_stable_ns_each={:.2} from_stable_ns_each={:.2} body_bytes={} heap_capacity_bytes={}",
        into.as_secs_f64() * 1e9 / N as f64,
        from.as_secs_f64() * 1e9 / N as f64,
        size_of::<Sample>(),
        sample.heap_bytes().unwrap()
    );
}
