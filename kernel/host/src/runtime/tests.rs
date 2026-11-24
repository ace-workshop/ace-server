use super::*;
use std::{
    collections::HashSet,
    future::{Future, poll_fn},
    path::PathBuf,
    sync::{
        Barrier, Condvar, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    task::Context,
    thread::{self, ThreadId},
};
const WAIT: Duration = Duration::from_secs(8);
struct Trace {
    events: Vec<(u64, String, ThreadId)>,
    released: HashSet<String>,
    wait_gap: Option<u64>,
}
static PROBE: OnceLock<(Mutex<Trace>, Condvar)> = OnceLock::new();
pub(super) fn start_trace() {
    PROBE
        .set((
            Mutex::new(Trace {
                events: vec![],
                released: HashSet::from(["cancel".into()]), // 仅在核对原库析构顺序时暂停 Cancel。
                wait_gap: None,
            }),
            Condvar::new(),
        ))
        .ok()
        .unwrap();
}
pub(super) fn bound(key: u64, t: Transport) -> std::net::SocketAddr {
    listener_endpoint(key, "net-listen", t)
}
pub(super) fn listener_endpoint(key: u64, label: &str, t: Transport) -> std::net::SocketAddr {
    let prefix = format!("{label} {t:?} ");
    lock(&PROBE.get().unwrap().0)
        .events
        .iter()
        .find_map(|(k, msg, _)| (*k == key).then(|| msg.strip_prefix(&prefix)).flatten())
        .unwrap()
        .parse()
        .unwrap()
}
pub(super) fn enabled() -> bool {
    PROBE.get().is_some()
}
pub(super) fn trace(key: u64, label: &str) {
    let Some((m, cv)) = PROBE.get() else { return };
    let mut t = lock(m);
    assert!(t.events.len() < 65536, "driver 无进展空转");
    t.events.push((key, label.into(), thread::current().id()));
    let label = if label == "idle" && t.wait_gap == Some(key) {
        t.wait_gap = None;
        t.events
            .push((key, "wait-gap".into(), thread::current().id()));
        "wait-gap"
    } else {
        label
    };
    cv.notify_all();
    if matches!(
        label,
        "parallel"
            | "hold-callback"
            | "wait-gap"
            | "busy-gone"
            | "accept-busy"
            | "accept-ok"
            | "cancel"
    ) {
        let (_guard, timed) = cv
            .wait_timeout_while(t, WAIT, |t| !t.released.contains(label))
            .unwrap();
        assert!(!timed.timed_out(), "夹具屏障超时 {label}");
    }
}
pub(super) fn count(k: u64, label: &str) -> usize {
    lock(&PROBE.get().unwrap().0)
        .events
        .iter()
        .filter(|(key, l, _)| (*key == k || k == 0) && l == label)
        .count()
}
pub(super) fn net_events(key: u64, c: Connection) -> Vec<String> {
    let prefix = format!("net[{}] ", c.handle.generation);
    lock(&PROBE.get().unwrap().0)
        .events
        .iter()
        .filter(|(k, ..)| *k == key)
        .filter_map(|(_, l, _)| l.strip_prefix(&prefix).map(str::to_owned))
        .collect()
}
pub(super) fn hold_idle(core: &Core, key: u64) {
    lock(&PROBE.get().unwrap().0).wait_gap = Some(key);
    let w = lock(&core.state).actors[&key].wake;
    wake(key, w);
    until("休眠前的交接点", || count(key, "wait-gap") == 1);
}
pub(super) fn release(label: &str) {
    let (m, cv) = PROBE.get().unwrap();
    lock(m).released.insert(label.into());
    cv.notify_all();
}
pub(super) fn until(name: &str, mut ready: impl FnMut() -> bool) {
    let (m, cv) = PROBE.get().unwrap();
    let end = std::time::Instant::now() + WAIT;
    loop {
        let n = lock(m).events.len();
        if ready() {
            return;
        }
        let (t, r) = cv
            .wait_timeout_while(
                lock(m),
                end.saturating_duration_since(std::time::Instant::now()),
                |t| t.events.len() == n,
            )
            .unwrap();
        if r.timed_out() {
            panic!("等待 {name} 超时，末次事件 {:?}", t.events.last())
        }
    }
}
/// 只改变测试轮次的执行线程，不手动调用 loader。交接的是同一个正式 driver Future；
/// 每轮通过双向屏障接过所有权并 join 返回，不能用 Tokio 碰巧迁移代替。
pub(super) async fn handoff(f: impl Future<Output = ()> + Send) {
    let mut f = Box::pin(f);
    let rt = Handle::current();
    poll_fn(move |cx| {
        let w = cx.waker().clone();
        let f = f.as_mut();
        let gate = Barrier::new(2);
        thread::scope(|scope| {
            let t = scope.spawn(|| {
                let _enter = rt.enter();
                gate.wait();
                f.poll(&mut Context::from_waker(&w))
            });
            gate.wait();
            t.join().unwrap()
        })
    })
    .await
}
fn manifest(p: &Path, config: &str) -> Manifest {
    let text =
        include_str!("../../../../tests/fixtures/runtime.toml").replace("timer-init", config);
    std::fs::write(p, text).unwrap();
    Manifest::read(p).unwrap()
}
static SESSION: AtomicU64 = AtomicU64::new(1);
static SOURCE: LazyLock<ServiceAddress> =
    LazyLock::new(|| ServiceAddress::try_from((2u64 << 48) + 1).unwrap());
fn send(port: &ReplyPort, addr: ServiceAddress, wire: WireContract, n: u64) -> Result<u64, Status> {
    let session = SESSION.fetch_add(1, Ordering::Relaxed);
    port.request(InboundCall {
        source: *SOURCE,
        destination: addr,
        session,
        wire,
        bytes: &n.to_le_bytes(),
        max_response_bytes: 8,
        timeout_ms: 3000,
    })?;
    Ok(session)
}
fn read(r: PortReply) -> Result<u64, Status> {
    r.body
        .map(|b| u64::from_le_bytes(b.as_ref().try_into().unwrap()))
}
fn call(port: &ReplyPort, addr: ServiceAddress, wire: WireContract, n: u64) -> Result<u64, Status> {
    let session = send(port, addr, wire, n)?;
    let r = port.recv(WAIT)?;
    assert_eq!(r.session, session);
    read(r)
}
fn stage(c: &Core, key: u64) -> Phase {
    lock(&c.state).phase(key).unwrap()
}
fn mark(c: &Arc<Core>, key: u64, dst: ServiceAddress, wire: WireContract, n: u64) -> Status {
    c.invoke(
        key,
        HostOp::Send(Outbound {
            destination: dst.bits(),
            wire,
            body: Body {
                representation: Representation::Bytes,
                bytes: n.to_le_bytes().as_slice().into(),
            },
            timeout_ms: 0,
            reply: ReplyBudget {
                representation: Representation::Bytes,
                ceiling: 0,
            },
            entry: Entry::Service,
        }),
        &mut RNone,
    )
    .map_or_else(|e| e, |r| r.status)
}
fn req(s: &State, owner: u64) -> Option<(u64, u64)> {
    s.pending.iter().find_map(|(&t, p)| match p.caller {
        rpc::Caller::Actor { key, id } if key == owner => Some((t, id)),
        _ => None,
    })
}
fn read_replies<const N: usize>(port: &ReplyPort, mut expected: [(u64, Result<u64, Status>); N]) {
    let mut actual = [(); N].map(|()| {
        let r = port.recv(WAIT).unwrap();
        (r.session, read(r))
    });
    actual.sort_by_key(|p| p.0);
    expected.sort_by_key(|p| p.0);
    assert_eq!(actual, expected);
}

fn exhausted_charge_ids(
    node: &Node,
    port: &ReplyPort,
    address: ServiceAddress,
    key: u64,
    wire: WireContract,
) {
    // 只推进既有分配器的高水位。真实回调停在锁外，所有被接纳项都在发号耗尽后才投递。
    lock(&PROBE.get().unwrap().0)
        .released
        .remove("hold-callback");
    let held = send(port, address, wire, 14).unwrap();
    until("临界发号屏障", || count(key, "hold-callback") == 1);
    let core = &node.core;
    lock(&core.state).ids = Ids(u64::MAX - 9);
    let empty = Outbound {
        destination: address.bits(),
        wire,
        body: Body {
            representation: Representation::Bytes,
            bytes: [].as_slice().into(),
        },
        timeout_ms: 3000,
        reply: ReplyBudget {
            representation: Representation::Bytes,
            ceiling: 0,
        },
        entry: Entry::Service,
    };
    let first = invoke(key, HostOp::Request(empty), &mut RNone);
    assert_eq!(first.status, Status::OK);
    let (token, _) = req(&lock(&core.state), key).unwrap();
    assert_eq!(
        invoke(
            key,
            HostOp::Reply(token, Reply::Success(empty.body)),
            &mut RNone
        )
        .status,
        Status::OK
    );
    assert!(
        matches!(&lock(&core.state).pending[&token].stage, rpc::Stage::Ready(Ok(b)) if b.bytes.is_empty() && b.charge == 0)
    );
    let bytes = 19u64.to_le_bytes();
    let full = Outbound {
        body: Body {
            bytes: bytes.as_slice().into(),
            ..empty.body
        },
        reply: ReplyBudget {
            ceiling: 8,
            ..empty.reply
        },
        ..empty
    };
    let local = invoke(key, HostOp::Request(full), &mut RNone);
    assert_eq!(local.status, Status::OK);
    let ingress = send(port, address, wire, 19).unwrap();
    assert_eq!(lock(&core.state).ids.0, u64::MAX - 1);
    let before = node.usage();
    assert_eq!(lock(&core.state).actors[&key].queued, 3);
    // 还可发 token，却不能发非空入站字节凭据：回滚容量和字节，不入队，不回绕。
    assert_eq!(send(port, address, wire, 19), Err(Status::BUSY));
    assert_eq!(
        invoke(key, HostOp::Request(full), &mut RNone).status,
        Status::BUSY
    );
    assert_eq!(lock(&core.state).ids.0, u64::MAX - 1);
    assert_eq!(lock(&core.state).actors[&key].queued, 3);
    assert_eq!(node.usage(), before);
    // 拒绝未消耗最后一个 ID；空端口输入只需 token，无字节 loan。
    let last = SESSION.fetch_add(1, Ordering::Relaxed);
    port.request(InboundCall {
        source: *SOURCE,
        destination: address,
        session: last,
        wire,
        bytes: &[],
        max_response_bytes: 8,
        timeout_ms: 3000,
    })
    .unwrap();
    assert_eq!(lock(&core.state).ids.0, u64::MAX);
    let controls = count(key, "completion");
    let polls = count(key, "word-poll");
    release("hold-callback");
    read_replies(
        port,
        [
            (held, Ok(14)),
            (ingress, Ok(19)),
            (last, Err(Status::PROTOCOL)),
        ],
    );
    until("最后两个本机完成同步归还槽与字节", || {
        count(key, "completion") == controls + 2 && node.usage() == (1, 0, 0)
    });
    assert!(lock(&core.state).actors[&key].charges.is_empty());
    assert_eq!(count(key, "word-poll"), polls + 2);
    assert_eq!(
        invoke(key, HostOp::CompletionConsumed(local.value), &mut RNone).status,
        Status::NOT_FOUND
    );
    println!(
        "ID=u64::MAX：已接纳本机 Bytes 请求/回复、端口输入、空成功均完成；新预留 Busy，剩余 pending/charge/bytes=0"
    );
}
#[test]
#[ignore = "tools/smoke-abi.sh 先分别构建 release caller/echo cdylib"]
fn runtime_real_plugins() {
    start_trace();
    let dir = PathBuf::from(std::env::var_os("ACE_RUNTIME_LIBS").expect("需要两个已构建的实库"));
    let temp = tempfile::tempdir().unwrap();
    let sub = std::env::var_os("ACE_RUNTIME_ABORT").is_some();
    let m = manifest(
        &temp.path().join("manifest.toml"),
        if sub { "panic-cancel" } else { "timer-init" },
    );
    let ids = temp.path().join("identity");
    crate::identity::provision_new(&ids, m.node).unwrap();
    // SAFETY: 只装载 smoke 在测试目录构建的受信 fixture。
    let registry = unsafe { Implementations::load_startup(&m.library_paths(&dir)) }.unwrap();
    let d = registry
        .descriptors()
        .find(|d| d.implementation.as_str() == "test.caller")
        .unwrap();
    let wire = d.methods.as_slice()[0].wire;
    let mark_wire = d
        .methods
        .as_slice()
        .iter()
        .find(|m| m.wire.key == 105)
        .unwrap()
        .wire;
    let node = Node::start(&m, registry, &ids).unwrap();
    let c = &node.core;
    let echo = node.resolve("echo").unwrap();
    let caller = node.resolve("caller").unwrap();
    let migrant = node.resolve("handoff").unwrap();
    let (e, a, h) = {
        let s = lock(&c.state);
        (
            s.key(echo.bits()).unwrap(),
            s.key(caller.bits()).unwrap(),
            s.key(migrant.bits()).unwrap(),
        )
    };
    let port = node.reply_port(2, 4, 1024).unwrap();
    if sub {
        send(&port, echo, wire, 28).unwrap();
        until("再次 panic", || count(e, "retired") > 0);
        panic!("再次 panic 未 fail-stop")
    }
    let ask = |to, n| call(&port, to, wire, n).unwrap();
    assert_eq!(ask(echo, 29), u64::from(Status::DENIED.0));
    assert_eq!(
        invoke(h, HostOp::ServiceStop(caller.bits()), &mut RNone).status,
        Status::DENIED
    );
    let qualifications = || count(a, "select-declarations");
    let before = qualifications();
    for (idx, (client, server)) in [(0, 0), (1, 100), (102, 201)].into_iter().enumerate() {
        assert_eq!(ask(caller, idx as u64 + 1), 42);
        if idx == 0 {
            let first = qualifications();
            assert_eq!(ask(caller, 1), 42);
            let checks = (first - before, qualifications() - first);
            println!(
                "同一双向本地调用经 SDK Select 和 Request，首次／再次比较注册声明：{checks:?}"
            );
            assert_eq!(checks, (1, 0));
        }
        assert_eq!(ask(caller, 4), client);
        assert_eq!(ask(echo, 4), server);
    }
    let mixed = crate::loader::find_method(d, 102).unwrap().wire;
    let select = |key, dst: ServiceAddress, mode| {
        let r = invoke(key, HostOp::Select(dst.bits(), mixed, mode), &mut RNone);
        assert_eq!(r.status, Status::OK);
        r.paths
    };
    use Representation::{Bytes, LocalOwned};
    let after_mixed = qualifications();
    assert_eq!(select(a, migrant, ReplyMode::Typed), [LocalOwned, Bytes]);
    assert_eq!(qualifications(), after_mixed); // 同一 echo 库的另一实例。
    assert_eq!(select(a, caller, ReplyMode::Typed), [LocalOwned; 2]);
    assert_eq!(select(e, caller, ReplyMode::Typed), [LocalOwned, Bytes]);
    assert_eq!(select(a, caller, ReplyMode::RawBytes), [LocalOwned, Bytes]);
    assert_eq!(ask(caller, 12), 12);
    assert_eq!(ask(caller, 9), 9);
    assert_eq!(count(a, "wake-resumed"), 1);
    {
        let trace = lock(&PROBE.get().unwrap().0);
        let mut users = 0;
        let mut new_poll = false;
        for (_, label, _) in trace.events.iter().filter(|(k, ..)| *k == a) {
            match label.as_str() {
                "abi-poll" => new_poll = true,
                "user-poll" => {
                    assert!(new_poll, "同一预算 1 不得 poll 两次用户 Future");
                    new_poll = false;
                    users += 1;
                }
                _ => {}
            }
        }
        assert_eq!(users, 2);
    }
    // 在空队列已检查、Notify 尚未登记时入队；唯一消费者仍能被唤醒。
    hold_idle(c, h);
    let r = send(&port, migrant, wire, 4).unwrap();
    release("wait-gap");
    let f = port.recv(WAIT).unwrap();
    assert_eq!(f.session, r);
    assert!(read(f).is_ok());
    assert_eq!(ask(caller, 5), 1);
    until("typed send", || count(e, "oneway") == 1);
    assert_eq!(ask(caller, 6), u64::from(Status::TOO_SMALL.0));
    assert_eq!(ask(caller, 7), 1);
    until("丢弃 Future 后自然归零", || {
        node.usage() == (3, 0, 0)
    });
    let task = send(&port, echo, wire, 10).unwrap();
    until("第一个 handler Pending", || count(e, "pending") == 1);
    let next = send(&port, echo, wire, 11).unwrap();
    let mut pair = vec![];
    for _ in 0..2 {
        let r = port.recv(WAIT).unwrap();
        pair.push((r.session, read(r).unwrap()));
    }
    pair.sort_unstable();
    assert_eq!(pair, [(task, 10), (next, 11)]);
    assert_eq!(ask(caller, 35), 19); // raw 与 typed 共用同一 pending 权威。
    assert_eq!(ask(caller, 15), u64::from(Status::TIMEOUT.0));
    assert_eq!(ask(echo, 11), 11);
    let held = count(e, "holding-word");
    assert_eq!(ask(caller, 37), 37);
    until("丢弃后业务确已执行", || {
        count(e, "holding-word") > held
    });
    until("丢弃的超时调用自然终结", || {
        lock(&c.state)
            .pending
            .values()
            .all(|p| p.owner() != Some(a))
    });
    assert_eq!(ask(echo, 11), 11);
    // 实际持有 LocalOwned 或 Bytes 的接收任务仍 Pending，不能随发送者 destroy 归还额度。
    for command in [33, 38] {
        let source = ServiceAddress::try_from(ask(caller, 32)).unwrap();
        let saved = {
            let s = lock(&c.state);
            s.actors[&s.key(source.bits()).unwrap()].budget.clone()
        };
        let prior = ask(caller, 16);
        let base = count(e, "holding-input");
        send(&port, source, wire, command).unwrap();
        until("入站任务已持有值", || {
            count(e, "holding-input") > base
        });
        node.stop(source).unwrap();
        assert_eq!(read(port.recv(WAIT).unwrap()), Err(Status::STOPPED));
        assert!(saved.used() > 0);
        if command == 33 {
            assert!(saved.used() >= 128 * 4, "按递归容量而非 len 计费")
        }
        assert_eq!(ask(caller, 16), prior);
        assert_eq!(ask(echo, 11), 11);
        until("接收者释放源账户", || saved.used() == 0);
        assert_eq!(ask(caller, 16), prior + u64::from(command == 33));
    }
    // 接收库真的拒绝控制，而不是宿主延迟回调；负载、token、业务调用始终是原来那一份。
    for (command, busy) in [(1, "busy-local"), (3, "busy-bytes")] {
        assert_eq!(ask(caller, 42), 42);
        let controls = count(a, "completion");
        let handlers = count(e, "sample-poll");
        let session = send(&port, caller, wire, command).unwrap();
        let (mut pending, mut charge) = (0, 0);
        until("实库暂拒完成、账本可重试", || {
            let mut s = lock(&c.state);
            let Some((t, id)) = req(&s, a) else {
                return false;
            };
            let rpc::Stage::Ready(Ok(body)) = &s.pending[&t].stage else {
                return false;
            };
            pending = id;
            charge = body.charge;
            if count(a, &format!("暂拒完成 {id} charge={charge}")) == 0 {
                return false;
            }
            assert_eq!(charge == 0, command == 1);
            assert_eq!(body.local.is_some(), command == 1);
            assert!(!s.actors[&a].charges.contains_key(&charge));
            assert_eq!(s.consume(a, id), Err(Status::NOT_FOUND));
            true
        });
        assert!(node.usage().2 >= 1024);
        let attempts = count(a, busy);
        for _ in 0..3 {
            assert_eq!(ask(echo, 19), 19);
        }
        assert!(count(a, busy) <= attempts + 2, "拒绝本身不能驱动重试");
        let resume = send(&port, caller, wire, 43).unwrap();
        read_replies(&port, [(session, Ok(42)), (resume, Ok(43))]);
        until("完成与字节分别归零", || node.usage() == (3, 0, 0));
        assert_eq!(count(a, &format!("接纳完成 {pending} charge={charge}")), 1);
        assert_eq!(count(a, "completion"), controls + 1);
        assert_eq!(count(e, "sample-poll"), handlers + 1);
        if charge != 0 {
            assert_eq!(
                invoke(a, HostOp::ChargeReleased(charge), &mut RNone).status,
                Status::NOT_FOUND
            );
        }
    }
    // 正常 Stop 的有限 Cleanup 即使仍 Busy 也收尾；Faulted 在本地槽取走前/后都不能重投或双析构。
    for mode in [42, 46, 47] {
        let target = ServiceAddress::try_from(ask(caller, 32)).unwrap();
        let k = lock(&c.state).key(target.bits()).unwrap();
        let account = lock(&c.state).actors[&k].budget.clone();
        let drops = ask(echo, 16);
        assert_eq!(ask(target, mode), mode);
        if mode != 42 {
            lock(&PROBE.get().unwrap().0).released.remove("cancel");
        }
        let session = send(&port, target, wire, 1).unwrap();
        let cause = if mode == 42 {
            until("停止前暂拒本地完成", || count(k, "busy-local") > 0);
            node.stop(target).unwrap();
            assert_eq!(count(k, "busy-cleanup"), 1);
            Status::STOPPED
        } else {
            Status::FAULTED
        };
        let r = port.recv(WAIT).unwrap();
        assert_eq!(r.session, session);
        assert_eq!(read(r), Err(cause));
        if cause == Status::FAULTED {
            until("故障实例已进入专用 Cancel", || {
                count(k, "cancel") == 1
            });
            let during_cancel = call(&port, echo, wire, 16);
            release("cancel");
            let taken = mode == 47;
            println!(
                "普通 Completion 取走={taken}：原库析构基线={drops}，Cancel 内={during_cancel:?}"
            );
            assert_eq!(
                during_cancel,
                Ok(drops + u64::from(taken)),
                "未取走的回复须留到 Cancel 之后"
            );
        }
        until("完成故障/停止后唯一回收", || {
            count(k, "retired") == 1 && node.usage() == (3, 0, 0)
        });
        assert_eq!(
            (account.used(), count(k, "cancel"), count(k, "destroy")),
            (0, 1, 1)
        );
        let retired_drops = ask(echo, 16);
        assert_eq!(retired_drops, drops + 1);
        if cause == Status::FAULTED {
            let trace = lock(&PROBE.get().unwrap().0);
            let tail: Vec<_> = trace
                .events
                .iter()
                .filter(|(id, ..)| *id == k)
                .map(|(_, label, _)| label.as_str())
                .skip_while(|l| *l != "notice-refused")
                .filter(|l| *l != "turn")
                .collect();
            assert_eq!(tail, ["notice-refused", "cancel", "destroy", "retired"]);
            println!(
                "退役后原库析构={retired_drops}，原账户剩余={}，{tail:?}",
                account.used()
            );
        }
    }
    // 具体 SDK 编码失败在提交前返回，没有对端 handler。成功体的本地解码错误不同于 RPC 终态。
    let handlers = count(e, "sample-poll");
    assert_eq!(ask(caller, 44), 6); // 固定 ace 的 ResourceLimit。
    assert_eq!(count(a, "提交前编码失败：ResourceLimit"), 1);
    assert_eq!(count(e, "sample-poll"), handlers);
    let base = count(e, "holding-word");
    let session = send(&port, caller, wire, 45).unwrap();
    until("注入小于预留但不合 DTO 的成功体", || {
        count(e, "holding-word") > base
    });
    let (token, _) = req(&lock(&c.state), a).unwrap();
    assert_eq!(
        invoke(
            e,
            HostOp::Reply(
                token,
                Reply::Success(Body {
                    representation: Representation::Bytes,
                    bytes: [1u8].as_slice().into()
                })
            ),
            &mut RNone
        )
        .status,
        Status::OK
    );
    let r = port.recv(WAIT).unwrap();
    assert_eq!(r.session, session);
    assert_eq!(read(r), Ok(45));
    assert_eq!(count(a, "成功体本机解码失败：Invalid"), 1);
    assert_eq!(ask(echo, 11), 11);
    assert_eq!(ask(caller, 48), 48); // 对端响应编码失败只给本调用方无体 Protocol。
    assert_eq!(count(e, "方法 103 编解码失败：Ace(ResourceLimit)"), 1);
    port.request(InboundCall {
        source: *SOURCE,
        destination: echo,
        session: 9010,
        wire,
        bytes: &[],
        max_response_bytes: 8,
        timeout_ms: 3000,
    })
    .unwrap();
    assert_eq!(read(port.recv(WAIT).unwrap()), Err(Status::PROTOCOL));
    assert_eq!(count(e, "方法 100 编解码失败：Invalid"), 1);
    // 四个 typed call 已提交，caller 让出 worker，暂不消费任何结果。
    let session = send(&port, caller, wire, 17).unwrap();
    until("已提交，caller 受控停在回调中", || {
        count(a, "before-await") == 1
    });
    let frame = |body| Outbound {
        destination: echo.bits(),
        wire,
        body: Body {
            representation: Representation::Bytes,
            bytes: body,
        },
        entry: Entry::Service,
        timeout_ms: 2000,
        reply: ReplyBudget {
            representation: Representation::Bytes,
            ceiling: 8,
        },
    };
    let bytes = 19u64.to_le_bytes();
    let out = frame(bytes.as_slice().into());
    until("四个终态仍占有槽", || {
        lock(&c.state)
            .pending
            .values()
            .filter(|p| p.owner() == Some(a) && matches!(p.stage, rpc::Stage::Delivered))
            .count()
            == 4
    });
    assert_eq!(
        invoke(a, HostOp::Request(out), &mut RNone).status,
        Status::BUSY
    );
    let resume = send(&port, caller, wire, 11).unwrap();
    for _ in 0..2 {
        let r = port.recv(WAIT).unwrap();
        if r.session == resume {
            assert_eq!(read(r), Ok(11))
        } else {
            assert_eq!(r.session, session);
            assert_eq!(read(r), Ok(19));
        }
    }
    until("预留结束", || node.usage() == (3, 0, 0));
    assert_eq!(
        invoke(
            a,
            HostOp::Request(Outbound {
                entry: Entry::Player {
                    push_session: RNone
                },
                ..out
            }),
            &mut RNone
        )
        .status,
        Status::DENIED
    );
    assert_eq!(
        invoke(
            a,
            HostOp::Select(
                echo.bits(),
                WireContract {
                    response: [0; 32],
                    ..wire
                },
                ReplyMode::Typed
            ),
            &mut RNone
        )
        .status,
        Status::PROTOCOL
    );
    // 对仍在运行的实际 handler 验证宿主 TooSmall 裁决（不是 SDK 预检）和晚回复。
    let base = count(e, "holding-word");
    let session = send(&port, echo, wire, 34).unwrap();
    until("真实入站 token", || count(e, "holding-word") > base);
    let token = {
        let s = lock(&c.state);
        *s.pending
            .iter()
            .find(|(_, p)| matches!(p.caller,rpc::Caller::Port{session:i,..}if i==session))
            .unwrap()
            .0
    };
    let reply = Reply::Success(Body {
        representation: Representation::Bytes,
        bytes: [0u8; 9].as_slice().into(),
    });
    assert_eq!(
        invoke(e, HostOp::Reply(token, reply), &mut RNone).status,
        Status::TOO_SMALL
    );
    assert_eq!(read(port.recv(WAIT).unwrap()), Err(Status::TOO_SMALL));
    assert_eq!(
        invoke(e, HostOp::Reply(token, reply), &mut RNone).status,
        Status::NOT_FOUND
    );
    assert_eq!(ask(echo, 11), 11);
    // 两个真实回调必须同时进入再放行；若在全宿主锁下调插件会超时。
    send(&port, echo, wire, 8).unwrap();
    send(&port, caller, wire, 8).unwrap();
    until("双实例并行", || count(0, "parallel") == 2);
    release("parallel");
    for _ in 0..2 {
        assert_eq!(read(port.recv(WAIT).unwrap()), Ok(8));
    }
    assert_eq!(ask(migrant, 19), 19);
    // 真实出口已满，第二个 handler 已运行完；重复 writable 不重跑业务，不消费原 token。
    let small = node.reply_port(2, 1, 8).unwrap();
    let before = count(e, "word-poll");
    let one = send(&small, echo, wire, 19).unwrap();
    let two = send(&small, echo, wire, 19).unwrap();
    until("临时出口 Busy", || count(e, "reply-busy") != 0);
    let token = {
        let s = lock(&c.state);
        let (&t, p) = s
            .pending
            .iter()
            .find(|(_, p)| p.callee == e && p.write == rpc::WriteInterest::Waiting)
            .unwrap();
        assert!(matches!(p.stage, rpc::Stage::Open));
        t
    };
    assert_eq!(
        invoke(
            a,
            HostOp::Reply(token, Reply::Error(Status::BUSY)),
            &mut RNone
        )
        .status,
        Status::DENIED
    );
    let r = small.recv(WAIT).unwrap();
    assert_eq!(r.session, one);
    assert_eq!(read(r), Ok(19));
    let r = small.recv(WAIT).unwrap();
    assert_eq!(r.session, two);
    assert_eq!(read(r), Ok(19));
    assert_eq!(count(e, "word-poll") - before, 2);
    assert_eq!(
        invoke(
            e,
            HostOp::Reply(token, Reply::Error(Status::BUSY)),
            &mut RNone
        )
        .status,
        Status::NOT_FOUND
    );
    // 可写通知与原期限竞争；未出队的第一条不冒充第二个已送达。
    let base = count(e, "reply-busy");
    send(&small, echo, wire, 19).unwrap();
    small
        .request(InboundCall {
            source: *SOURCE,
            destination: echo,
            session: 9000,
            wire,
            bytes: &19u64.to_le_bytes(),
            max_response_bytes: 8,
            timeout_ms: 100,
        })
        .unwrap();
    until("期限前 Busy", || count(e, "reply-busy") > base);
    until("期限唤醒并清除结果", || {
        node.usage().1 == 0 && node.usage().2 == 0
    });
    assert_eq!(read(small.try_recv().unwrap()), Ok(19));
    assert!(small.try_recv().is_none());
    let base = count(e, "reply-busy");
    send(&small, echo, wire, 19).unwrap();
    send(&small, echo, wire, 19).unwrap();
    until("关闭前确实不可写", || count(e, "reply-busy") > base);
    drop(small);
    until("出口关闭释放已编码结果", || {
        node.usage() == (3, 0, 0)
    });
    // 未 poll 的 call Future 已收到终态，但完成槽和实际结果均不可提前归还。
    let base = count(a, "completion");
    let session = send(&port, caller, wire, 27).unwrap();
    until("已交付未消费", || count(a, "completion") > base);
    let (t, id) = {
        let s = lock(&c.state);
        let (t, id) = req(&s, a).unwrap();
        assert!(matches!(s.pending[&t].stage, rpc::Stage::Delivered));
        assert!(s.actors[&a].budget.used() >= 8);
        (t, id)
    };
    assert_ne!(t, id);
    assert_eq!(
        invoke(e, HostOp::CompletionConsumed(id), &mut RNone).status,
        Status::NOT_FOUND
    );
    assert_eq!(
        invoke(
            e,
            HostOp::Reply(t, Reply::Error(Status::TIMEOUT)),
            &mut RNone
        )
        .status,
        Status::NOT_FOUND
    );
    let n = send(&port, caller, wire, 11).unwrap();
    let r1 = port.recv(WAIT).unwrap();
    let r2 = port.recv(WAIT).unwrap();
    assert!(matches!((r1.session,r2.session),(p,q)if (p==session&&q==n)||(p==n&&q==session)));
    until("消费归零", || node.usage() == (3, 0, 0));
    // 多生产者、多轮环形槽复用，无重复/漏消费。
    for round in 0..8u64 {
        let gate = Barrier::new(4);
        thread::scope(|scope| {
            for i in 0..4 {
                let g = &gate;
                scope.spawn(move || {
                    g.wait();
                    assert_eq!(mark(c, a, echo, mark_wire, round * 4 + i), Status::OK);
                });
            }
        });
        until("四个唯一 send", || {
            count(e, "marked") == 4 * (round as usize + 1)
        });
    }
    assert_eq!(ask(echo, 31), 32);
    // 同一 timer 的迟到周期只计算一个 tick。把同一个生产记录的 at 置于可知的过去，
    // 仍由正式 Tokio driver 选择、交付和续期，未构造 Timer ABI 通知。
    let id = ask(echo, 40);
    let timer = ResourceHandle {
        id,
        generation: lock(&c.state).actors[&e].timers[&id].generation,
    };
    let wrong = ResourceHandle {
        id: timer.id,
        generation: timer.generation + 1,
    };
    assert_eq!(
        invoke(e, HostOp::TimerCancel(wrong), &mut RNone).status,
        Status::NOT_FOUND
    );
    let prior = count(e, "tick");
    {
        let mut s = lock(&c.state);
        let actor = s.actors.get_mut(&e).unwrap();
        actor.timers.get_mut(&timer.id).unwrap().at = Instant::now() - Duration::from_secs(50);
        actor.signal.notify_one();
    }
    until("50 秒迟到合并", || count(e, "tick") > prior);
    assert_eq!(
        invoke(
            e,
            HostOp::TimerCancel(ResourceHandle {
                id: timer.id,
                generation: timer.generation
            }),
            &mut RNone
        )
        .status,
        Status::OK
    );
    assert_eq!(ask(echo, 30), 1);
    let mut timers = vec![];
    for _ in 0..4 {
        let t = invoke(e, HostOp::TimerSet(3000, 0), &mut RNone);
        assert_eq!(t.status, Status::OK);
        timers.push(ResourceHandle {
            id: t.value,
            generation: t.generation,
        });
    }
    assert_eq!(
        invoke(e, HostOp::TimerSet(1, 0), &mut RNone).status,
        Status::BUSY
    );
    for t in timers {
        assert_eq!(
            invoke(e, HostOp::TimerCancel(t), &mut RNone).status,
            Status::OK
        )
    }
    // 当前回调停在可控屏障，占满队列后失败不留预留；停止胜出的后续提交不得成功。
    send(&port, migrant, wire, 14).unwrap();
    until("停止屏障", || count(h, "hold-callback") == 1);
    for i in 0..4 {
        assert_eq!(mark(c, a, migrant, mark_wire, i), Status::OK)
    }
    let used = node.usage();
    assert_eq!(mark(c, a, migrant, mark_wire, 5), Status::BUSY);
    assert_eq!(used, node.usage());
    let wake_token = lock(&c.state).actors[&h].wake;
    {
        let mut s = lock(&c.state);
        s.stop(h, Status::STOPPED);
    }
    assert_eq!(mark(c, a, migrant, mark_wire, 6), Status::STOPPED);
    wake(h, wake_token);
    assert_eq!(stage(c, h), Phase::Ending(Status::STOPPED));
    assert_eq!(
        invoke(a, HostOp::ServiceSubscribe(migrant.bits()), &mut RNone).status,
        Status::STOPPED
    );
    assert_eq!(count(h, "cancel"), 0);
    assert_eq!(count(h, "destroy"), 0); // 停止不会并发进入仍驻留的回调。
    release("hold-callback");
    assert_eq!(read(port.recv(WAIT).unwrap()), Err(Status::STOPPED));
    until("迁移析构", || count(h, "retired") == 1);
    assert_eq!(
        invoke(a, HostOp::ServiceSubscribe(migrant.bits()), &mut RNone).status,
        Status::NOT_FOUND
    );
    let t = lock(&PROBE.get().unwrap().0);
    let ids = ["create", "word-poll", "cancel", "destroy"].map(|label| {
        let observed = t
            .events
            .iter()
            .filter(|(k, l, _)| *k == h && l == label)
            .collect::<Vec<_>>();
        assert_eq!(observed.len(), 1, "{label}");
        observed[0].2
    });
    assert_eq!(ids.into_iter().collect::<HashSet<_>>().len(), 4);
    drop(t);
    // 管理方为预留/初始化/未认领的实例同样收费；超时释放子实例但保留创建结果。
    let timeout = invoke(
        a,
        HostOp::ServiceCreate(
            "test.echo".into(),
            b"never-ready".as_slice().into(),
            RSome("not-ready".into()),
        ),
        &mut RNone,
    );
    assert_eq!(timeout.status, Status::OK);
    let first = invoke(
        a,
        HostOp::ServiceCreate(
            "test.echo".into(),
            [].as_slice().into(),
            RSome("quota-a".into()),
        ),
        &mut RNone,
    );
    let second = invoke(
        a,
        HostOp::ServiceCreate(
            "test.echo".into(),
            [].as_slice().into(),
            RSome("quota-b".into()),
        ),
        &mut RNone,
    );
    assert_eq!((first.status, second.status), (Status::OK, Status::OK));
    assert_eq!(
        invoke(
            a,
            HostOp::ServiceCreate("test.echo".into(), [].as_slice().into(), RNone),
            &mut RNone
        )
        .status,
        Status::BUSY
    );
    for t in [first, second] {
        assert_eq!(
            invoke(a, HostOp::CreateAbandon(t.value), &mut RNone).status,
            Status::OK
        )
    }
    until("初始化超时", || {
        let s = lock(&c.state);
        s.tickets
            .get(&timeout.value)
            .is_some_and(|t| t.result == Some(Status::TIMEOUT) && t.reported)
    });
    assert_eq!(
        invoke(a, HostOp::CreateClaim(timeout.value), &mut RNone).status,
        Status::TIMEOUT
    );
    assert!(node.resolve("not-ready").is_none());
    // 已有两个 Pending 的实例，send 队首保持顺序并睡眠，不靠轮询等槽。
    let source = ServiceAddress::try_from(ask(caller, 32)).unwrap();
    assert_eq!(
        invoke(e, HostOp::ServiceSubscribe(source.bits()), &mut RNone).status,
        Status::OK
    );
    let held = send(&port, echo, wire, 34).unwrap();
    send(&port, echo, wire, 18).unwrap();
    until("两个驻留任务", || {
        let s = lock(&c.state);
        s.pending.len() == 2 && s.actors[&e].queued == 0
    });
    let full = send(&port, echo, wire, 19).unwrap();
    let r = port.recv(WAIT).unwrap();
    assert_eq!(r.session, full);
    assert_eq!(read(r), Err(Status::BUSY));
    assert_eq!(mark(c, a, echo, mark_wire, 100), Status::OK);
    until("full send 保留队首", || {
        let s = lock(&c.state);
        s.actors[&e].queued == 1 && s.actors[&e].mailbox.is_empty()
    });
    let turns = count(e, "turn");
    for _ in 0..3 {
        assert_eq!(ask(caller, 19), 19)
    }
    // 已登记的最后一次入队通知可以额外产生检查，但非空队首本身不能驱动轮转。
    assert!(count(e, "turn") <= turns + 2);
    assert_eq!(count(e, "marked"), 32);
    node.stop(source).unwrap();
    let r = port.recv(WAIT).unwrap();
    assert_eq!(r.session, held);
    assert_eq!(read(r), Ok(10));
    until(
        "独立控制唤醒 handler，留在队首的 send 继续",
        || count(e, "marked") == 33,
    );
    // 准备和管理结果槽、权限与待认领 alias。认领后创建者消失不销毁 child。
    let b = count(a, "create-ready");
    assert_eq!(ask(caller, 42), 42);
    assert_eq!(ask(caller, 21), 21);
    until("管理结果被暂拒", || count(a, "busy-create") > 0);
    assert!(
        lock(&c.state)
            .tickets
            .values()
            .any(|t| t.owner == a && t.result == Some(Status::OK) && !t.reported)
    );
    assert!(node.resolve("unclaimed").is_none());
    assert_eq!(ask(caller, 43), 43);
    until("未认领就绪", || count(a, "create-ready") > b);
    assert!(node.resolve("unclaimed").is_none());
    assert_eq!(ask(caller, 22), 22);
    until("作废销毁", || {
        !lock(&c.state).aliases.contains_key("unclaimed")
    });
    let t = invoke(
        a,
        HostOp::ServiceCreate(
            "test.echo".into(),
            [].as_slice().into(),
            RSome("race".into()),
        ),
        &mut RNone,
    );
    assert_eq!(t.status, Status::OK);
    until("竞争前确已待交付", || {
        lock(&c.state)
            .tickets
            .get(&t.value)
            .is_some_and(|t| t.result == Some(Status::OK))
    });
    assert_eq!(
        invoke(e, HostOp::CreateClaim(t.value), &mut RNone).status,
        Status::DENIED
    );
    let gate = Barrier::new(2);
    let claim = thread::scope(|scope| {
        let other = scope.spawn(|| {
            gate.wait();
            invoke(a, HostOp::CreateAbandon(t.value), &mut RNone)
        });
        gate.wait();
        let r = invoke(a, HostOp::CreateClaim(t.value), &mut RNone);
        assert_eq!(other.join().unwrap().status, Status::OK);
        r
    });
    if claim.status == Status::OK {
        let addr = node.resolve("race").unwrap();
        assert_eq!(claim.value, addr.bits());
        node.stop(addr).unwrap();
    } else {
        assert_eq!(claim.status, Status::NOT_FOUND)
    }
    until("竞争清账", || {
        !lock(&c.state).aliases.contains_key("race")
    });
    let raced = ServiceAddress::try_from(ask(caller, 32)).unwrap();
    let gate = Barrier::new(2);
    let accepted = thread::scope(|scope| {
        let producer = scope.spawn(|| {
            gate.wait();
            mark(c, a, raced, mark_wire, 200)
        });
        gate.wait();
        node.stop(raced).unwrap();
        producer.join().unwrap()
    });
    assert!(matches!(
        accepted,
        Status::OK | Status::STOPPED | Status::NOT_FOUND
    ));
    assert_eq!(mark(c, a, raced, mark_wire, 201), Status::NOT_FOUND);
    assert_eq!(ask(caller, 23), u64::from(Status::DENIED.0));
    assert!(!lock(&c.state).aliases.contains_key("rejected"));
    let child = ServiceAddress::try_from(ask(caller, 20)).unwrap();
    assert_eq!(node.resolve("child"), Some(child));
    assert_ne!(child, migrant);
    assert_eq!(
        invoke(
            e,
            HostOp::ServiceCreate("test.echo".into(), [].as_slice().into(), RNone),
            &mut RNone
        )
        .status,
        Status::DENIED
    );
    assert_eq!(
        invoke(a, HostOp::PeerListen("a".into()), &mut RNone).status,
        Status::DENIED
    );
    assert_eq!(
        invoke(a, HostOp::NetListen(Transport::Tcp, "a".into()), &mut RNone).status,
        Status::PROTOCOL
    );
    let listening = invoke(
        a,
        HostOp::NetListen(Transport::Tcp, "127.0.0.1:0".into()),
        &mut RNone,
    );
    assert_eq!(listening.status, Status::OK);
    assert_ne!(listening.generation, 0);
    until("实际 TCP 监听在唯一 driver 投递", || {
        count(a, "runtime-listen-ready") == 1
    });
    assert_eq!(
        std::net::TcpListener::bind(bound(a, Transport::Tcp))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AddrInUse
    );
    let ck = lock(&c.state).key(child.bits()).unwrap();
    let cancel_inflight = invoke(ck, HostOp::ServiceSubscribe(echo.bits()), &mut RNone);
    assert_eq!(cancel_inflight.status, Status::OK);
    assert_eq!(ask(child, 42), 42);
    let sub = ask(caller, 26);
    assert_eq!(ask(caller, 26), sub);
    assert_eq!(ask(caller, 42), 42);
    node.stop(echo).unwrap();
    until("正常停止终态被暂拒", || count(a, "busy-gone") > 0);
    assert!(lock(&c.state).actors[&a].subs[&sub].gone);
    until("另一实库停在同一终态的接收回调中", || {
        count(ck, "busy-gone") > 0
    });
    assert_eq!(
        invoke(
            ck,
            HostOp::ResourceCancel(cancel_inflight.value),
            &mut RNone
        )
        .status,
        Status::OK
    );
    release("busy-gone");
    assert_eq!(read(port.recv(WAIT).unwrap()), Err(Status::STOPPED));
    assert_eq!(ask(caller, 43), 43);
    assert_eq!(ask(child, 43), 43);
    assert_eq!(count(ck, "gone"), 0);
    // 插件的 gone 日志早于回调返回；宿主结算后才可断言订阅槽回收。
    until("可靠单次 ResourceGone 已接受并结算", || {
        count(a, "gone") == 1 && !lock(&c.state).actors[&a].subs.contains_key(&sub)
    });
    assert_eq!(count(e, "destroy"), 1);
    assert_eq!(count(e, "cancel"), 1);
    assert_eq!(send(&port, echo, wire, 19), Err(Status::NOT_FOUND));
    let watch = ServiceAddress::try_from(ask(caller, 32)).unwrap();
    let wk = lock(&c.state).key(watch.bits()).unwrap();
    assert_eq!(
        invoke(wk, HostOp::ServiceSubscribe(child.bits()), &mut RNone).status,
        Status::OK
    );
    let cancel = invoke(a, HostOp::ServiceSubscribe(child.bits()), &mut RNone);
    assert_eq!(cancel.status, Status::OK);
    assert_eq!(
        invoke(a, HostOp::ResourceCancel(cancel.value), &mut RNone).status,
        Status::OK
    );
    let b = count(a, "create-ready");
    assert_eq!(ask(caller, 21), 21);
    until("创建者停止前已入管理槽", || {
        count(a, "create-ready") > b
    });
    send(&port, caller, wire, 25).unwrap();
    assert_eq!(read(port.recv(WAIT).unwrap()), Err(Status::STOPPED));
    until("self-stop 连同未认领实例收尾", || {
        node.usage() == (2, 0, 0)
    });
    assert_eq!(node.resolve("child"), Some(child));
    assert_eq!(ask(child, 19), 19);
    // 让保留的第二个实际插件实例见证 Faulted 终态，而非从日志推断可靠通知。
    let released = ask(watch, 16);
    assert_eq!(ask(watch, 42), 42);
    let fault = send(&port, watch, wire, 41).unwrap();
    until("Faulted 终态和错误完成均被暂拒", || {
        count(wk, "busy-gone") > 0 && count(wk, "busy-error") > 0
    });
    assert!(
        lock(&c.state).actors[&wk]
            .subs
            .values()
            .any(|s| s.gone && s.target == child.bits())
    );
    let resume = send(&port, watch, wire, 43).unwrap();
    read_replies(&port, [(fault, Err(Status::FAULTED)), (resume, Ok(43))]);
    until("faulted 可靠通知已结算", || {
        count(wk, "gone") == 1 && lock(&c.state).actors[&wk].subs.is_empty()
    });
    assert_eq!(ask(watch, 16), released + 1);
    exhausted_charge_ids(&node, &port, watch, wk, wire);
    node.stop(watch).unwrap();
    until("faulted cancel/destroy", || node.usage() == (0, 0, 0));
    assert_eq!(count(ck, "cancel"), 1);
    assert_eq!(count(ck, "destroy"), 1);
    drop(node);
    assert_eq!(port.recv(Duration::ZERO).err(), Some(Status::STOPPED));
    assert_eq!(send(&port, watch, wire, 19), Err(Status::STOPPED));
    use std::os::unix::process::ExitStatusExt;
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "runtime::tests::runtime_real_plugins",
            "--nocapture",
        ])
        .env("ACE_RUNTIME_ABORT", "1")
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&child.stderr).contains("再次 panic 必须 fail-stop"));
    assert_eq!(child.status.signal(), Some(6));
    println!(
        "实际两个独立库：跨线程 create/poll/cancel/destroy 屏障、Actor 并行、typed/local/mixed/bytes、限额和出口、ticket、timer、订阅、故障收尾及 SIGABRT 已观测"
    );
}

#[test]
#[ignore = "tools/smoke-abi.sh 先构建 release caller/echo 实库"]
fn first_cleanup_fault_retires_instance() {
    start_trace();
    let dir = PathBuf::from(std::env::var_os("ACE_RUNTIME_LIBS").unwrap());
    let temp = tempfile::tempdir().unwrap();
    let mut m = manifest(&temp.path().join("manifest.toml"), "timer-init");
    for i in &mut m.instances {
        if i.alias != "echo" {
            i.config = "fault-cleanup".into();
        }
    }
    let ids = temp.path().join("identity");
    crate::identity::provision_new(&ids, m.node).unwrap();
    // SAFETY: 仅装载本次构建、由上述测试清单指定的两份受信实库。
    let libs = unsafe { Implementations::load_startup(&m.library_paths(&dir)) }.unwrap();
    let wire = libs.descriptors().next().unwrap().methods.as_slice()[0].wire;
    let mut node = Node::start(&m, libs, &ids).unwrap();
    let core = node.core.clone();
    let echo = node.resolve("echo").unwrap();
    let e = lock(&core.state).key(echo.bits()).unwrap();
    let port = node.reply_port(2, 4, 32).unwrap();
    let drops = call(&port, echo, wire, 16).unwrap();
    for alias in ["caller", "handoff"] {
        let dst = node.resolve(alias).unwrap();
        let (k, phase, budget) = {
            let s = lock(&core.state);
            let k = s.key(dst.bits()).unwrap();
            (
                k,
                s.actors[&k].phase.subscribe(),
                s.actors[&k].budget.clone(),
            )
        };
        assert_eq!(call(&port, dst, wire, 42), Ok(42)); // 正常运行时暂拒完成。
        assert_eq!(
            invoke(k, HostOp::TimerSet(4000, 0), &mut RNone).status,
            Status::OK
        );
        let sessions = [(); 2].map(|()| send(&port, dst, wire, 1).unwrap());
        until(
            "保留两份本地完成，清理首次故障后另一份不得重投",
            || {
                lock(&core.state)
                    .pending
                    .values()
                    .filter(|p| {
                        p.owner() == Some(k)
                            && matches!(&p.stage, rpc::Stage::Ready(Ok(b)) if b.local.is_some())
                    })
                    .count()
                    == 2
                    && count(k, "busy-local") != 0
            },
        );
        assert!(budget.used() > 0);
        if alias == "caller" {
            let sub = invoke(e, HostOp::ServiceSubscribe(dst.bits()), &mut RNone);
            assert_eq!(sub.status, Status::OK);
            thread::scope(|scope| {
                let stop = scope.spawn(|| node.stop(dst));
                until("原 driver 到达 Cancel", || {
                    count(k, "hold-callback") == 1
                });
                let timers = lock(&core.state).actors[&k].timers.len();
                let ending = *phase.borrow();
                read_replies(&port, sessions.map(|id| (id, Err(Status::FAULTED))));
                let during_cancel = call(&port, echo, wire, 16);
                release("hold-callback");
                stop.join().unwrap().unwrap(); // 等到 Gone，不改变管理 API。
                assert_eq!((ending, timers), (Phase::Ending(Status::FAULTED), 0));
                assert_eq!(
                    during_cancel,
                    Ok(drops),
                    "未取走的两份完成须留到 Cancel 之后"
                );
            });
            until("订阅者实际接纳一次终态", || {
                count(e, "gone") == 1 && !lock(&core.state).actors[&e].subs.contains_key(&sub.value)
            });
            assert_eq!(call(&port, echo, wire, 16), Ok(drops + 2));
            assert_eq!(
                call(&port, node.resolve("handoff").unwrap(), wire, 19),
                Ok(19)
            );
            assert_eq!(node.usage(), (2, 0, 0));
        } else {
            assert_eq!(node.finish(), Err(Status::FAULTED)); // 不是正常退出或经过协作期限。
            assert_eq!(node.usage(), (0, 0, 0));
        }
        assert_eq!(*phase.borrow(), Phase::Gone(Status::FAULTED));
        assert_eq!(budget.used(), 0);
        let trace = lock(&PROBE.get().unwrap().0);
        let tail = trace
            .events
            .iter()
            .filter(|(id, _, _)| *id == k)
            .map(|(_, label, _)| label.as_str())
            .skip_while(|l| *l != "fault-local-cleanup")
            .filter(|l| {
                matches!(
                    *l,
                    "fault-local-cleanup"
                        | "busy-cleanup"
                        | "busy-local"
                        | "abi-poll"
                        | "cancel"
                        | "destroy"
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(tail, ["fault-local-cleanup", "cancel", "destroy"]);
        println!(
            "{alias}: 两份 LocalOwned 完成，{tail:?}，终态 {:?}，额度 {}",
            *phase.borrow(),
            budget.used()
        );
    }
}

#[test]
#[ignore = "tools/smoke-abi.sh 的 release 实库；终态与认领的同账本屏障"]
fn shutdown_rejects_ready_ticket_and_new_entry() {
    start_trace();
    let dir = PathBuf::from(std::env::var_os("ACE_RUNTIME_LIBS").unwrap());
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("manifest.toml");
    std::fs::write(
        &file,
        include_str!("../../../../tests/fixtures/runtime.toml")
            .replace("[budgets]", "[shutdown]\nfirst=['echo']\n[budgets]"),
    )
    .unwrap();
    let m = Manifest::read(&file).unwrap();
    let ids = temp.path().join("identity");
    crate::identity::provision_new(&ids, m.node).unwrap();
    // SAFETY: 只取本次构建的受信 caller 和 echo 实库。
    let libs = unsafe { Implementations::load_startup(&m.library_paths(&dir)) }.unwrap();
    let wire = libs
        .descriptors()
        .find(|d| d.implementation == "test.echo")
        .unwrap()
        .methods
        .as_slice()[0]
        .wire;
    let node = Node::start(&m, libs, &ids).unwrap();
    let core = node.core.clone();
    let caller = node.resolve("caller").unwrap();
    let echo = node.resolve("echo").unwrap();
    let a = lock(&core.state).key(caller.bits()).unwrap();
    let e = lock(&core.state).key(echo.bits()).unwrap();
    let port = node.reply_port(2, 4, 32).unwrap();
    let create = || {
        invoke(
            a,
            HostOp::ServiceCreate("test.echo".into(), [].as_slice().into(), RNone),
            &mut RNone,
        )
    };
    let ticket = create();
    assert_eq!(ticket.status, Status::OK);
    until("初始化结果已送达但尚未认领", || {
        lock(&core.state).tickets[&ticket.value].reported
    });
    send(&port, echo, wire, 14).unwrap();
    until("真实回调占住首个关闭步骤", || {
        count(e, "hold-callback") == 1
    });
    let mut phase = lock(&core.state).actors[&e].phase.subscribe();
    let management = thread::spawn(move || drop(node));
    core.rt
        .block_on(phase.wait_for(|p| p.ending().is_some()))
        .unwrap();
    assert!(!lock(&core.state).published);
    assert_eq!(lock(&core.state).phase(a).unwrap(), Phase::Running);
    let ordinary_token = lock(&core.state).actors[&a].wake;
    wake(a, ordinary_token);
    assert!(!lock(&core.state).actors[&a].ordinary);
    assert_eq!(create().status, Status::STOPPED);
    assert_eq!(
        invoke(a, HostOp::CreateClaim(ticket.value), &mut RNone).status,
        Status::STOPPED
    );
    assert_eq!(send(&port, caller, wire, 19), Err(Status::STOPPED));
    release("hold-callback");
    management.join().unwrap();
    let s = lock(&core.state);
    assert!(s.actors.is_empty() && s.tickets.is_empty() && s.pending.is_empty());
    println!(
        "ready ticket 未认领时关闭：仍 Running 的管理者 create/claim/入站均 Stopped，最后无孤儿或 pending"
    );
}
