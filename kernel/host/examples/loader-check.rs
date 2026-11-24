//! 加载实际薄库并核对启动、故障和收尾；不需要网络，也不链接夹具实现。
#[path = "loader-check/exchange.rs"]
mod exchange;
use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RSlice,
};
use ace_server_abi::{ABI_VERSION, Status, native::*};
use ace_server_host::{
    config::{IngressGrants, Manifest},
    identity::{Identity, provision_new},
    loader::{Implementations, Instance, LoadError, inspection_args},
};
use std::{
    error::Error,
    num::{NonZeroU16, NonZeroU64},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
};

const LISTENER: ResourceHandle = ResourceHandle {
    id: 21,
    generation: 5,
};
const CLOSED: Connection = Connection {
    transport: Transport::Tcp,
    handle: ResourceHandle {
        id: 22,
        generation: 3,
    },
};
static LIFECYCLE_READY: AtomicU64 = AtomicU64::new(0);
static WOKEN: AtomicU64 = AtomicU64::new(0);
static REPLIED: AtomicU64 = AtomicU64::new(0);
static DROPPED_WORK: AtomicUsize = AtomicUsize::new(0);
static CANCEL: AtomicUsize = AtomicUsize::new(0);
static DESTROY: AtomicUsize = AtomicUsize::new(0);
static HOST: HostApi = HostApi {
    abi_size: size_of::<HostApi>() as u32,
    abi_version: ABI_VERSION,
    invoke,
    wake,
    startup_peer_check: RNone,
};
// 这是测试驱动的受控通知和观测点，不建立真实监听器、pending 或调度线程。
extern "C" fn invoke(ctx: u64, op: HostOp<'_>, slot: &mut LocalSlot) -> HostResult {
    if let Err(s) = op.validate(slot) {
        return HostResult::status(s);
    }
    match op {
        HostOp::NetListen(Transport::Tcp, _) => {
            return HostResult {
                value: LISTENER.id,
                generation: LISTENER.generation,
                ..HostResult::status(Status::OK)
            };
        }
        HostOp::LifecycleReady(LifecycleAction::Prepare) => LIFECYCLE_READY.store(ctx, SeqCst),
        HostOp::Reply(7, Reply::Error(Status::NOT_FOUND)) => REPLIED.store(ctx, SeqCst),
        HostOp::NetReleaseClosed(c) if c == CLOSED => (),
        HostOp::CompletionConsumed(12) => (),
        HostOp::Log(_, text) => match text.as_str() {
            "drop-work" => {
                DROPPED_WORK.fetch_add(1, SeqCst);
            }
            "cancel" => {
                CANCEL.fetch_add(1, SeqCst);
            }
            "destroy" => {
                DESTROY.fetch_add(1, SeqCst);
            }
            _ => return HostResult::status(Status::NOT_READY),
        },
        _ => return HostResult::status(Status::NOT_READY),
    }
    HostResult::status(Status::OK)
}
extern "C" fn wake(ctx: u64, token: u64) {
    if token == NonZeroU64::MAX.get() {
        WOKEN.store(ctx, SeqCst);
    }
}
fn control(actor: &mut Instance, event: Control<'_>) -> Status {
    actor.control(event, &mut RNone).status
}
// 同一实际 Watchdog 已获测试依赖地址，但宿主没有 v1 能力时不得退回普通订阅。
fn unsupported_startup_peer_check(registry: &Implementations, ids: &mut Identity) {
    extern "C" fn resolve_only(key: u64, op: HostOp<'_>, slot: &mut LocalSlot) -> HostResult {
        if matches!(op, HostOp::ResolvePrepared(_)) {
            return HostResult {
                value: (1 << 48) | 1,
                ..HostResult::status(Status::OK)
            };
        }
        if matches!(op, HostOp::ServiceSubscribe(_)) {
            UNEXPECTED_CALLS.fetch_add(1, SeqCst);
        }
        invoke(key, op, slot)
    }
    static UNEXPECTED_CALLS: AtomicUsize = AtomicUsize::new(0);
    extern "C" fn other_version(_: u64, _: u64, _: StartupPeer<'_>) -> Status {
        UNEXPECTED_CALLS.fetch_add(1, SeqCst);
        Status::FAULTED
    }
    static ABSENT: HostApi = HostApi {
        invoke: resolve_only,
        ..HOST
    };
    static DIFFERENT: HostApi = HostApi {
        startup_peer_check: RSome(&StartupPeerCheck {
            version: 2,
            check: other_version,
        }),
        ..ABSENT
    };
    for (host, expected) in [(&ABSENT, Status::NOT_READY), (&DIFFERENT, Status::PROTOCOL)] {
        let mut args = inspection_args(
            ids.issue().expect("可用测试身份"),
            ids.boot(),
            b"gate_alias='gate'\ndefault_target_alias='player'",
        );
        args.host = host;
        let mut actor = registry
            .create("watchdog", args, IngressGrants::default())
            .unwrap();
        assert_eq!(control(&mut actor, Control::Prepare), expected);
        assert!(!actor.is_running());
    }
    assert_eq!(UNEXPECTED_CALLS.load(SeqCst), 0);
    println!("启动对端检查：实库缺能力 NotReady、错版 Protocol，未调用错版入口");
}
fn deferred_prepare(actor: &mut Instance, peer: WireContract) {
    use LifecycleAction::{Prepare, Start};
    let poll = || Control::LifecyclePoll(Prepare, 1);
    let listening = |h| {
        Event::Network(NetEvent::ListenReady(
            h,
            Transport::Tcp,
            "127.0.0.1:7000".into(),
        ))
    };
    let bootstrap = |w| {
        Event::Peer(PeerEvent::Control(
            LISTENER,
            PeerStage::Candidate,
            w,
            RSlice::from_slice(&[]),
        ))
    };
    assert_eq!(control(actor, Control::Prepare), Status::PENDING);
    assert_eq!(control(actor, Control::Poll(1)), Status::NOT_READY);
    assert_eq!(
        control(actor, Control::LifecyclePoll(Start, 1)),
        Status::NOT_READY
    );
    assert_eq!(
        control(actor, Control::Event(listening(LISTENER))),
        Status::NOT_READY
    );
    assert_eq!(
        control(actor, Control::LifecycleEvent(Start, listening(LISTENER))),
        Status::NOT_READY
    );
    assert_eq!(
        control(
            actor,
            Control::LifecycleEvent(
                Prepare,
                listening(ResourceHandle {
                    generation: 4,
                    ..LISTENER
                })
            )
        ),
        Status::NOT_FOUND
    );
    assert_eq!(
        control(
            actor,
            Control::LifecycleEvent(
                Prepare,
                bootstrap(WireContract {
                    codec: [9; 3],
                    ..peer
                })
            )
        ),
        Status::PROTOCOL
    );
    for drift in [
        WireContract {
            request: [9; 32],
            ..peer
        },
        WireContract {
            response: [9; 32],
            ..peer
        },
    ] {
        assert_eq!(
            control(actor, Control::LifecycleEvent(Prepare, bootstrap(drift))),
            Status::PROTOCOL
        );
    }
    assert_eq!(
        control(actor, Control::LifecycleEvent(Prepare, bootstrap(peer))),
        Status::OK
    );
    assert_eq!(control(actor, poll()), Status::PENDING);
    assert_eq!(LIFECYCLE_READY.load(SeqCst), 0);
    assert_eq!(
        control(actor, Control::LifecycleEvent(Prepare, listening(LISTENER))),
        Status::OK
    );
    assert_eq!(LIFECYCLE_READY.load(SeqCst), actor.address().bits());
    assert_eq!(control(actor, poll()), Status::OK);
}
fn timeout() -> Event<'static> {
    Event::Completion {
        pending: 12,
        reply: Reply::Error(Status::TIMEOUT),
        charge_token: 0,
    }
}
fn drops() -> (usize, usize) {
    (CANCEL.load(SeqCst), DESTROY.load(SeqCst))
}
fn main() -> Result<(), Box<dyn Error>> {
    let paths = std::env::args_os()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if let [mode, manifest, dir] = paths.as_slice()
        && mode == Path::new("runtime")
    {
        return runtime_check(manifest, dir);
    }
    if let [mode, input, output, libraries @ ..] = paths.as_slice()
        && mode == Path::new("exchange")
    {
        return exchange::run(input, output, libraries);
    }
    // SAFETY: 本测试只接收当前构建产出的受信 SDK 夹具/薄库。
    let registry = unsafe { Implementations::load_startup(&paths) };
    if std::env::var_os("ACE_FIXTURE_WRONG_VERSION").is_some() {
        assert!(matches!(registry, Err(LoadError::Contract(_))));
        println!("不兼容版本在 create 前已拒绝");
        return Ok(());
    }
    let registry = registry?;
    assert_eq!(registry.descriptors().count(), 5);
    let methods = |name| {
        registry
            .descriptors()
            .find(|d| d.implementation == name)
            .unwrap()
            .methods
            .as_slice()
    };
    let (gate, sessions) = (methods("gateway"), methods("watchdog"));
    for (list, entries) in [
        (
            gate,
            &[
                (257, 1),
                (258, 1),
                (259, 1),
                (260, 1),
                (513, 2),
                (514, 2),
                (1025, 1),
            ][..],
        ),
        (
            sessions,
            &[(513, 1), (514, 1), (515, 2), (257, 2), (258, 2)][..],
        ),
    ] {
        assert_eq!(list.len(), entries.len());
        for &(key, dir) in entries {
            let m = list
                .iter()
                .find(|m| m.wire.key == key)
                .expect("声明了角色方法");
            assert_eq!(
                (
                    m.directions,
                    m.wire.codec,
                    m.client_callable,
                    m.peer_control
                ),
                (
                    dir,
                    [1, 1, 1],
                    u8::from(key == 1025 && dir == 1),
                    PeerAccess::None
                )
            );
        }
    }
    // Bind 和 Kick 实际声明应逐向选择本地表示，不能为了玩家字节桥而退化为全 Bytes。
    for key in [257, 258] {
        let method = |ms: &[Method]| *ms.iter().find(|m| m.wire.key == key).unwrap();
        assert_eq!(
            method(sessions).select(&method(gate), RouteScope::Local, ReplyMode::Typed),
            Ok([Representation::LocalOwned; 2])
        );
    }
    let manifest = Manifest::read(Path::new("config/server.toml"))?;
    manifest.check(&registry)?;
    let gate = manifest.ingress_grants("gate").unwrap();
    let peer = manifest.ingress_grants("members").unwrap();
    registry.check_bindings(&[
        ("gateway", Some("gateway")),
        ("gateway", Some("gateway")), // 同角色可有多个实例，不等于多授一个节点管理权。
        ("watchdog", Some("watchdog")),
        ("cluster", Some("cluster")),
        ("test.policy", None),
    ])?;
    assert!(
        registry
            .check_bindings(&[("gateway", Some("gateway"))])
            .is_err()
    );
    let dir = tempfile::tempdir()?;
    let p = dir.path().join("identity");
    let node = NonZeroU16::MIN;
    provision_new(&p, node)?;
    let mut ids = Identity::open(&p, node, NonZeroU64::new(3).unwrap())?;
    let mut make = |key: &str, config: &[u8], grants: IngressGrants| {
        let mut args = inspection_args(ids.issue().expect("可用测试身份"), ids.boot(), config);
        args.host = &HOST;
        args.limits = manifest.budgets.sdk_limits();
        args.wake_token = NonZeroU64::MAX;
        registry.create(key, args, grants)
    };
    let none = IngressGrants::default();
    // 缺少真正的 timer／Prepare 依赖，不因库现已实现而允许假发布。
    for (role, config) in [
        (
            "gateway",
            &b"tcp='127.0.0.1:0'\nkcp='127.0.0.1:0'\nwatchdog_alias='sessions'"[..],
        ),
        (
            "watchdog",
            &b"gate_alias='gate'\ndefault_target_alias='player'"[..],
        ),
    ] {
        let mut actor = make(role, config, none).unwrap();
        assert_eq!(control(&mut actor, Control::Prepare), Status::NOT_READY);
        assert!(!actor.is_running());
    }
    assert!(matches!(make("cluster", b"", none), Err(Status::NOT_READY)));
    assert!(matches!(
        make("test.policy", b"reject", none),
        Err(Status::DENIED)
    ));
    assert!(matches!(
        make("test.policy", b"panic-create", none),
        Err(Status::FAULTED)
    ));
    assert_eq!(DESTROY.load(SeqCst), 0);
    let mut a = make("test.policy", b"", gate).unwrap();
    assert_eq!(a.address().sequence(), 6);
    let mut b = make("test.policy", b"deferred", peer).unwrap();
    assert_ne!(a.address(), b.address());
    let mut c = make("test.policy", b"panic-poll", none).unwrap();
    let methods = registry
        .descriptors()
        .find(|d| d.implementation == "test.policy")
        .unwrap()
        .methods;
    let out = Outbound {
        destination: b.address().bits(),
        wire: methods[0].wire,
        body: Body {
            representation: Representation::Bytes,
            bytes: RSlice::from_slice(&[]),
        },
        timeout_ms: 100,
        reply: ReplyBudget {
            representation: Representation::Bytes,
            ceiling: 0,
        },
        entry: Entry::Player {
            push_session: RSome(NonZeroU64::new(29).unwrap()),
        },
    };
    assert_eq!(none.check(&HostOp::Request(out)), Err(Status::DENIED));
    assert_eq!(gate.check(&HostOp::Request(out)), Ok(()));
    let msg = Message {
        source: a.address().bits(),
        reply_token: 7,
        charge_token: 0,
        request: out,
    };
    for (actor, deferred) in [(&mut a, false), (&mut b, true), (&mut c, false)] {
        assert_eq!(control(actor, Control::Start), Status::NOT_READY);
        if deferred {
            assert_eq!(actor.message(msg, &mut RNone), Status::NOT_READY);
            deferred_prepare(actor, methods[1].wire);
        } else {
            assert_eq!(control(actor, Control::Prepare), Status::OK);
        }
        assert_eq!(control(actor, Control::Start), Status::OK);
        assert!(!actor.is_running());
        assert_eq!(control(actor, Control::Poll(1)), Status::NOT_READY);
        actor.publish().unwrap();
        assert!(actor.is_running());
    }
    assert_eq!(
        b.message(
            Message {
                reply_token: 0,
                ..msg
            },
            &mut RNone
        ),
        Status::PROTOCOL
    );
    assert_eq!(
        b.message(
            Message {
                request: Outbound {
                    reply: ReplyBudget {
                        ceiling: 1,
                        ..out.reply
                    },
                    ..out
                },
                ..msg
            },
            &mut RNone
        ),
        Status::TOO_SMALL
    );
    assert_eq!(b.message(msg, &mut RNone), Status::OK);
    assert_eq!(REPLIED.load(SeqCst), b.address().bits());
    assert_eq!(
        control(
            &mut a,
            Control::Event(Event::Peer(PeerEvent::DataReady(LISTENER)))
        ),
        Status::DENIED
    );
    assert_eq!(a.control(Control::Poll(1), &mut RNone).more_ready, 1);
    std::thread::spawn(move || assert_eq!(a.control(Control::Poll(1), &mut RNone).more_ready, 0))
        .join()
        .unwrap();
    assert_eq!(b.control(Control::Poll(1), &mut RNone).more_ready, 1);
    assert_eq!(WOKEN.load(SeqCst), b.address().bits());
    b.begin_stop();
    assert_eq!(b.message(msg, &mut RNone), Status::STOPPED);
    assert_eq!(control(&mut b, Control::Poll(1)), Status::STOPPED);
    assert_eq!(control(&mut b, Control::Event(timeout())), Status::STOPPED);
    assert_eq!(
        control(&mut b, Control::Cleanup(Event::Timer(LISTENER))),
        Status::PROTOCOL
    );
    assert_eq!(
        control(
            &mut b,
            Control::Cleanup(Event::Network(NetEvent::Closed(CLOSED, Status::STOPPED)))
        ),
        Status::OK
    );
    assert_eq!(control(&mut b, Control::Cleanup(timeout())), Status::OK);
    assert_eq!(
        DROPPED_WORK.load(SeqCst),
        1,
        "清理通知没有让 b 的 Pending 业务 Future 再次 poll 到 Ready"
    );
    assert_eq!(control(&mut b, Control::Stop), Status::OK);
    assert_eq!(
        DROPPED_WORK.load(SeqCst),
        2,
        "取消已实际 drop 任务，不等 destroy"
    );
    assert_eq!(
        control(&mut b, Control::Cleanup(timeout())),
        Status::STOPPED
    );
    assert_eq!(control(&mut b, Control::Cancel), Status::OK);
    assert_eq!(
        drops(),
        (2, 1),
        "这里宿主可以释放残留额度，插件已取消但尚未析构"
    );
    drop(b);
    assert_eq!(control(&mut c, Control::Poll(1)), Status::FAULTED);
    c.begin_stop();
    assert!(!c.is_running());
    assert_eq!(
        control(&mut c, Control::Cleanup(timeout())),
        Status::FAULTED
    );
    assert_eq!(control(&mut c, Control::Stop), Status::FAULTED);
    assert_eq!(control(&mut c, Control::Cancel), Status::OK);
    assert_eq!(drops(), (3, 2));
    assert_eq!(DROPPED_WORK.load(SeqCst), 3);
    drop(c);
    assert_eq!(drops(), (3, 3));
    unsupported_startup_peer_check(&registry, &mut ids);
    println!(
        "三角色真实装载且未假报 ready；所属事件唤醒 Pending、入口令牌与无体错误、独立发布、隔离／迁移、失败不复用地址、Stopping 仅清理、Faulted 专用取消、cancel/destroy 各 3 次"
    );
    Ok(())
}

// 不带测试调度钩子的 production Node：本机有限入口触发实库之间的 typed RPC。
fn runtime_check(manifest: &Path, dir: &Path) -> Result<(), Box<dyn Error>> {
    use ace_server_host::runtime::{InboundCall, Node};
    use std::time::Duration;
    let m = Manifest::read(manifest)?;
    // SAFETY: smoke 只传本次独立构建的受信 fixture。
    let libs = unsafe { Implementations::load_startup(&m.library_paths(dir)) }?;
    let wire = libs
        .descriptors()
        .find(|d| d.implementation == "test.caller")
        .and_then(|d| d.methods.as_slice().iter().find(|m| m.wire.key == 100))
        .ok_or("夹具缺少有界控制输入")?
        .wire;
    let tmp = tempfile::tempdir()?;
    let identity = tmp.path().join("identity");
    provision_new(&identity, m.node)?;
    let node = Node::start(&m, libs, &identity)?;
    let caller = node.resolve("caller").ok_or("caller 未发布")?;
    let echo = node.resolve("echo").ok_or("echo 未发布")?;
    let status = |s: Status| format!("运行时状态 {}", s.0);
    let port = node.reply_port(2, 2, 32).map_err(status)?;
    let src = ace_net_proto::ServiceAddress::try_from((2u64 << 48) + 1)?;
    let mut session = 0;
    let mut call = |destination, word: u64| -> Result<u64, Box<dyn Error>> {
        session += 1;
        port.request(InboundCall {
            source: src,
            destination,
            session,
            wire,
            bytes: &word.to_le_bytes(),
            max_response_bytes: 8,
            timeout_ms: 2000,
        })
        .map_err(status)?;
        let r = port.recv(Duration::from_secs(3)).map_err(status)?;
        assert_eq!(
            (r.session, r.destination, r.wire),
            (session, src.bits(), wire)
        );
        Ok(u64::from_le_bytes(
            r.body.map_err(status)?.as_ref().try_into()?,
        ))
    };
    for (i, paths, expected) in [
        (1, "local/local", (0, 0)),
        (2, "local/bytes", (1, 100)),
        (3, "bytes/bytes", (102, 201)),
    ] {
        assert_eq!(call(caller, i)?, 42);
        let counts = (call(caller, 4)?, call(echo, 4)?);
        assert_eq!(counts, expected);
        println!(
            "{paths} → 42；累计实际 codec (encode,decode)：caller ({},{})，echo ({},{})",
            counts.0 / 100,
            counts.0 % 100,
            counts.1 / 100,
            counts.1 % 100
        );
    }
    Ok(())
}
