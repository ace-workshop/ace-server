use super::super::tests::{bound, count, net_events, release as open_barrier, start_trace, until};
use super::*;
use ace_net_proto::{
    Message as Kind,
    bootstrap::{Hello, KcpEnvelope as E, KcpSession},
};
use ace_transport::{MAX_UDP_PAYLOAD, Session, frame_head, random_nonce};
use std::{
    io::{Read, Write},
    net::{TcpStream, UdpSocket},
    path::PathBuf,
    process::Command,
    time::Instant as Clock,
};
const WAIT: Duration = Duration::from_secs(6);
fn op(key: u64, op: HostOp<'_>) -> Status {
    invoke(key, op, &mut RNone).status
}
fn tcp(addr: SocketAddr) -> TcpStream {
    let mut s = TcpStream::connect_timeout(&addr, WAIT).unwrap();
    s.set_read_timeout(Some(WAIT)).unwrap();
    s.set_write_timeout(Some(WAIT)).unwrap();
    s.write_all(&Hello.encode()).unwrap();
    let mut h = [0; 12];
    s.read_exact(&mut h).unwrap();
    Hello::decode(&h).unwrap();
    s
}
fn read(s: &mut TcpStream) -> Vec<u8> {
    let mut h = [0; 104];
    s.read_exact(&mut h).unwrap();
    let n = frame_head(&h, MAX_FRAME).unwrap().frame_len;
    let mut b = vec![0; n];
    b[..104].copy_from_slice(&h);
    s.read_exact(&mut b[104..]).unwrap();
    b
}
fn reserved(core: &Core, key: u64, peer: SocketAddr) -> Connection {
    lock(&core.state)
        .net
        .slots
        .values()
        .find_map(|s| match &s.state {
            Link::Live(_) if s.owner == key && s.peer == peer => Some(s.conn),
            _ => None,
        })
        .expect("握手已确认且本代未关闭")
}
fn conn(core: &Core, key: u64, peer: SocketAddr) -> Connection {
    let c = reserved(core, key, peer);
    until("实库接纳本代", || {
        lock(&core.state)
            .net
            .slot(c)
            .is_ok_and(|s| matches!(s.state, Link::Live(_)) && s.pending_accept.is_none())
    });
    c
}
fn unpause(key: u64) {
    assert_eq!(op(key, HostOp::TimerSet(1, 0)), Status::OK);
}
fn udp(addr: SocketAddr) -> UdpSocket {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(30))).unwrap();
    s.set_write_timeout(Some(WAIT)).unwrap();
    s
}
fn put(s: &UdpSocket, e: E<'_>) {
    let mut b = [0; MAX_UDP_PAYLOAD];
    let n = e.encode_into(&mut b, MAX_UDP_PAYLOAD).unwrap();
    assert_eq!(s.send(&b[..n]).unwrap(), n);
}
fn get<'a>(s: &UdpSocket, b: &'a mut [u8]) -> Option<E<'a>> {
    match s.recv(b) {
        Ok(n) => Some(E::decode(&b[..n], MAX_UDP_PAYLOAD).unwrap()),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) =>
        {
            None
        }
        Err(e) => panic!("回环接收失败：{e}"),
    }
}
fn offer(s: &UdpSocket, n: NonZeroU64) -> KcpSession {
    put(s, E::Hello(n));
    let mut b = [0; MAX_UDP_PAYLOAD + 1];
    match get(s, &mut b) {
        Some(E::Offer(e, id)) if e == n => id,
        r => panic!("未取得本候选：{r:?}"),
    }
}
fn confirmed(s: &UdpSocket, n: NonZeroU64, id: KcpSession) {
    put(s, E::Confirm(n, id));
    let end = Clock::now() + WAIT;
    let mut b = [0; MAX_UDP_PAYLOAD + 1];
    loop {
        assert!(Clock::now() < end);
        if let Some(E::Ready(e, o)) = get(s, &mut b) {
            assert_eq!((e, o), (n, id));
            return;
        }
    }
}
struct Client {
    s: UdpSocket,
    id: KcpSession,
    k: Session,
    born: Clock,
    packets: usize,
    loss: bool,
    reordered: bool,
    received: Vec<Vec<u8>>,
}
impl Client {
    fn new(s: UdpSocket, loss: bool) -> Self {
        let nonce = random_nonce().unwrap();
        let id = offer(&s, nonce);
        assert_eq!(offer(&s, nonce), id);
        confirmed(&s, nonce, id);
        confirmed(&s, nonce, id);
        s.set_read_timeout(Some(Duration::from_millis(2))).unwrap();
        let mut k = Session::new(id.conv, 5000, MAX_FRAME).unwrap();
        k.send(&Hello.encode()).unwrap();
        let mut c = Self {
            s,
            id,
            k,
            born: Clock::now(),
            packets: 0,
            loss,
            received: vec![],
            reordered: false,
        };
        c.through(1);
        assert_eq!(c.received.remove(0), Hello.encode());
        c
    }
    fn step(&mut self) {
        self.k.update(self.born.elapsed()).unwrap();
        let mut batch = vec![];
        while let Some(p) = self.k.packet() {
            self.packets += 1;
            if self.loss && self.packets % 11 == 1 {
                continue;
            }
            batch.push(p);
        }
        self.reordered |= self.loss && batch.len() > 1;
        for p in batch.iter().rev() {
            put(&self.s, E::Data(self.id, p));
            if self.loss {
                put(&self.s, E::Data(self.id, p));
            }
        }
        let mut b = [0; MAX_UDP_PAYLOAD + 1];
        if let Some(E::Data(id, p)) = get(&self.s, &mut b) {
            assert_eq!(id, self.id);
            self.k.input(self.born.elapsed(), p).unwrap();
        }
        while let Some(n) = self.k.peek().unwrap() {
            let mut b = vec![0; n];
            self.k.recv(&mut b).unwrap();
            self.received.push(b);
        }
    }
    fn through(&mut self, n: usize) {
        let end = Clock::now() + WAIT;
        while self.received.len() < n {
            assert!(
                Clock::now() < end,
                "KCP 交付超时：{}/{}",
                self.received.len(),
                n
            );
            self.step();
        }
    }
}
fn session(b: &[u8], id: u64) -> Vec<u8> {
    let mut f = Frame::decode(b, frame_limits(MAX_FRAME)).unwrap();
    let Kind::Request(mut r) = f.message else {
        panic!()
    };
    r.session = NonZeroU64::new(id).unwrap();
    f.message = Kind::Request(r);
    let mut data = vec![0; b.len()];
    f.encode_into(&mut data, frame_limits(MAX_FRAME)).unwrap();
    data
}
fn reply_id(b: &[u8]) -> u64 {
    match Frame::decode(b, frame_limits(MAX_FRAME)).unwrap().message {
        Kind::Response {
            session,
            error: None,
        } => session.get(),
        _ => panic!("不是成功应答"),
    }
}
fn release(core: &Core, owner: u64, conn: Connection) {
    until("关闭记录接纳", || {
        lock(&core.state).net.slot(conn).is_ok_and(|s| {
            matches!(
                s.state,
                Link::Closed {
                    delivered: true,
                    ..
                }
            )
        })
    });
    assert_eq!(op(owner, HostOp::NetReleaseClosed(conn)), Status::OK);
    assert_eq!(op(owner, HostOp::NetReleaseClosed(conn)), Status::OK);
}

#[test]
#[ignore = "smoke 先构建独立 owner 实库和纯客户端；各实库场景单进程启动一个正式节点"]
fn dual_transport_real_owner() {
    start_trace();
    let dir = PathBuf::from(std::env::var_os("ACE_NETWORK_LIBS").unwrap());
    let cli = PathBuf::from(std::env::var_os("ACE_NETWORK_CLIENT").unwrap());
    let mut manifest = Manifest::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/network.toml")
            .as_path(),
    )
    .unwrap();
    // 仅此验证进程启用可控闸门。
    for instance in &mut manifest.instances {
        instance.config = "network-probe".into();
    }
    let tmp = tempfile::tempdir().unwrap();
    let ids = tmp.path().join("identity");
    crate::identity::provision_new(&ids, manifest.node).unwrap();
    // SAFETY: 只装载当前脚本构建的受信回环 owner 夹具。
    let registry = unsafe { Implementations::load_startup(&manifest.library_paths(&dir)) }.unwrap();
    let node = Node::start(&manifest, registry, &ids).unwrap();
    let c = &node.core;
    let owner = node.resolve("owner").unwrap();
    let other = node.resolve("other").unwrap();
    let (a, o) = {
        let s = lock(&c.state);
        (s.key(owner.bits()).unwrap(), s.key(other.bits()).unwrap())
    };
    let (ta, ua) = (bound(a, Transport::Tcp), bound(a, Transport::Kcp));
    assert_eq!(
        op(
            a,
            HostOp::NetListen(Transport::Tcp, ta.to_string().as_str().into())
        ),
        Status::OK
    );
    until("异步绑定失败", || count(a, "net-listen-failed") == 1);
    // 32 个不同的真实回环源填满 pending；重试不扩表、不续期。纯客户端随后跨过候选 TTL 重试成功。
    let pending = (0..udp::HANDSHAKES)
        .map(|_| {
            let socket = udp(bound(o, Transport::Kcp));
            let n = random_nonce().unwrap();
            let id = offer(&socket, n);
            (socket, n, id)
        })
        .collect::<Vec<_>>();
    let (p0, n0, id0) = &pending[0];
    assert_eq!(offer(p0, *n0), *id0);
    let extra = udp(bound(o, Transport::Kcp));
    let mut hb = [0; MAX_UDP_PAYLOAD + 1];
    put(&extra, E::Hello(random_nonce().unwrap()));
    assert!(get(&extra, &mut hb).is_none());
    put(&extra, E::Confirm(*n0, *id0));
    assert!(get(&extra, &mut hb).is_none());
    assert_eq!(lock(&c.state).net.bytes.used(), 0);
    for (t, mode) in [(Transport::Tcp, "tcp"), (Transport::Kcp, "kcp")] {
        assert!(
            Command::new(&cli)
                .args([mode, &bound(o, t).to_string()])
                .status()
                .unwrap()
                .success()
        );
    }
    until("客户端 FIN 与 KCP idle 后独立归还", || {
        count(o, "net-closed") == 2 && lock(&c.state).net.bytes.used() == 0
    });
    put(p0, E::Confirm(*n0, *id0));
    assert!(get(p0, &mut hb).is_none());
    assert_ne!(offer(p0, *n0).token, id0.token);
    drop(pending);
    let sample = |name: &str| {
        let p = tmp.path().join(name);
        assert!(
            Command::new(&cli)
                .arg("sample-request")
                .arg(&p)
                .arg(name)
                .status()
                .unwrap()
                .success()
        );
        std::fs::read(p).unwrap()
    };
    let data = sample("ACE");
    let pause = sample("pause");
    let hold = sample("hold");
    let resume = sample("resume");
    let fault = sample("fault");
    let mut stream = tcp(ta);
    let first = conn(c, a, stream.local_addr().unwrap());
    assert_eq!(op(o, HostOp::NetClose(first)), Status::DENIED);
    assert_eq!(
        op(o, HostOp::NetSend(first, data.as_slice().into())),
        Status::DENIED
    );
    assert_eq!(op(a, HostOp::NetReleaseClosed(first)), Status::NOT_READY);
    stream.write_all(&data[..7]).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(30)))
        .unwrap();
    assert!(stream.read(&mut [0]).is_err());
    stream.set_read_timeout(Some(WAIT)).unwrap();
    // 头部切分，紧接第二帧；按原有 body_len/请求扩展分帧而非新增长度字。
    stream
        .write_all(&[&data[7..], &session(&data, 2)].concat())
        .unwrap();
    assert_ne!(reply_id(&read(&mut stream)), 2);
    assert_eq!(reply_id(&read(&mut stream)), 2);
    stream.write_all(&pause).unwrap();
    read(&mut stream);
    until("SDK 任务无容量", || {
        !lock(&c.state).actors[&a].net_capacity
    });
    let offered = count(a, "net-data");
    stream.write_all(&data).unwrap();
    // 业务读暂停时仍能关闭；Closed 不挤同一条 Data 或任务槽。
    assert_eq!(op(a, HostOp::NetClose(first)), Status::OK);
    assert_eq!(op(a, HostOp::NetClose(first)), Status::OK);
    until("Closed 单独预留且可暂拒", || {
        count(a, "net-refused-closed") == 1
    });
    assert_eq!(count(a, "net-data"), offered);
    let token = lock(&c.state).actors[&a].wake;
    wake(a, token);
    until("同一关闭令牌再次接纳", || {
        count(a, "net-closed") == 1
    });
    assert_eq!(op(o, HostOp::NetReleaseClosed(first)), Status::DENIED);
    release(c, a, first);
    drop(stream);
    unpause(a);
    until("SDK 任务可再次接纳", || {
        lock(&c.state).actors[&a].net_capacity
    });
    let mut truncated = tcp(ta);
    let tc = conn(c, a, truncated.local_addr().unwrap());
    truncated.write_all(&data[..104]).unwrap();
    until("已准入的请求头在等扩展", || {
        count(a, &format!("net-request-prefix {}", tc.handle.generation)) == 1
    });
    truncated.shutdown(std::net::Shutdown::Write).unwrap();
    until("收到半个请求后的 FIN 为协议关闭", || {
        lock(&c.state).net.slot(tc).is_ok_and(|s| {
            matches!(
                s.state,
                Link::Closed {
                    reason: Status::PROTOCOL,
                    ..
                }
            )
        })
    });
    release(c, a, tc);
    // 在真实 Accepted 回调内分别跨过物理关闭后返回 OK/Busy；只有后者应再收到同一代的 Accepted。
    let mut control = tcp(ta);
    let control_conn = conn(c, a, control.local_addr().unwrap());
    let control_bytes = Network::weight(c.b, Transport::Tcp) as u64;
    for command in ["accept-ok", "accept-busy"] {
        control.write_all(&sample(command)).unwrap();
        read(&mut control);
        let delayed = tcp(ta);
        let d = reserved(c, a, delayed.local_addr().unwrap());
        until("Accepted 已进入实库回调", || count(a, command) == 1);
        let listener = lock(&c.state).net.slot(d).unwrap().pending_accept.unwrap();
        let offer = format!(
            "net-offer {} {}:{} {}",
            d.handle.generation,
            listener.id,
            listener.generation,
            delayed.local_addr().unwrap()
        );
        assert_eq!(op(a, HostOp::NetClose(d)), Status::OK);
        until("socket 先结束，同一回调仍在途", || {
            count(a, &format!("net-finished {}", d.handle.generation)) == 1
        });
        assert_eq!(op(a, HostOp::NetReleaseClosed(d)), Status::NOT_READY);
        assert_eq!(lock(&c.state).net.bytes.used(), control_bytes);
        open_barrier(command);
        release(c, a, d);
        let expected = if command == "accept-ok" {
            vec!["accepted", "closed"]
        } else {
            vec!["refused", "accepted", "closed"]
        };
        assert_eq!(
            net_events(a, d),
            expected,
            "同代 Accepted/Closed 接纳顺序：{command}"
        );
        assert_eq!(
            count(a, &offer),
            expected.len() - 1,
            "每次尝试都携原 listener 和 peer"
        );
    }
    // Data 在途被明确暂拒，下一份完整帧留在 socket；全实例 task_capacity 也保持关闭。
    let mut fin = tcp(ta);
    let fc = conn(c, a, fin.local_addr().unwrap());
    fin.write_all(&hold).unwrap();
    until("本代 Data 被暂拒", || count(a, "net-held") > 0);
    let paused = count(a, "net-task-paused");
    control.write_all(&pause).unwrap();
    read(&mut control);
    until("实库已确认持续暂停", || {
        count(a, "net-task-paused") > paused && !lock(&c.state).actors[&a].net_capacity
    });
    let mut expected = net_events(a, fc);
    assert_eq!(expected.first().map(String::as_str), Some("accepted"));
    assert!(!expected[1..].is_empty() && expected[1..].iter().all(|s| s == "data"));
    fin.write_all(&data).unwrap();
    let ordinary = format!("net-paused-readable {}", fin.local_addr().unwrap());
    until("读暂停时确已观测普通可读而不是 FIN", || {
        count(0, &ordinary) > 0
    });
    assert!(lock(&c.state).net.live(fc).is_ok());
    fin.shutdown(std::net::Shutdown::Write).unwrap();
    until(
        "业务持续暂停时由对端 FIN 独立生成正常 Closed",
        || {
            lock(&c.state).net.slot(fc).is_ok_and(|s| {
                matches!(
                    s.state,
                    Link::Closed {
                        reason: Status::OK,
                        delivered: true,
                        ..
                    }
                )
            })
        },
    );
    assert_eq!(
        lock(&c.state).net.bytes.used(),
        control_bytes,
        "业务仍暂停且未归还凭据，FIN 已回收该连接的字节"
    );
    release(c, a, fc);
    expected.push("closed".into());
    assert_eq!(net_events(a, fc), expected);
    assert!(
        !lock(&c.state).actors[&a].net_capacity,
        "FIN 与 Closed 不能靠重新开放业务"
    );
    // 同一暂停期另有一代在 Accepted 首次投递前断开。物理内存已还，不能先领 Closed 凭据。
    let silent = tcp(ta);
    let before_first = reserved(c, a, silent.local_addr().unwrap());
    silent.shutdown(std::net::Shutdown::Write).unwrap();
    until("未投递 Accepted 时的物理关闭", || {
        count(
            a,
            &format!("net-finished {}", before_first.handle.generation),
        ) == 1
    });
    assert!(net_events(a, before_first).is_empty());
    assert_eq!(
        op(a, HostOp::NetReleaseClosed(before_first)),
        Status::NOT_READY
    );
    unpause(a);
    release(c, a, before_first);
    assert_eq!(net_events(a, before_first), ["accepted", "closed"]);
    assert_eq!(op(a, HostOp::NetClose(control_conn)), Status::OK);
    release(c, a, control_conn);
    let offered = count(a, "net-data");
    let held = count(a, "net-held");
    let mut x = Client::new(udp(ua), true);
    let old = x.id;
    let xc = conn(c, a, x.s.local_addr().unwrap());
    assert_ne!(first.handle.generation, xc.handle.generation);
    assert_eq!(op(a, HostOp::NetClose(first)), Status::NOT_FOUND);
    let mut y = Client::new(udp(ua), false);
    let yc = conn(c, a, y.s.local_addr().unwrap());
    {
        let s = lock(&c.state);
        assert!(s.net.room(c.b, Transport::Tcp));
        assert!(!s.net.room(c.b, Transport::Kcp));
    }
    let full = udp(ua);
    let n = random_nonce().unwrap();
    let id = offer(&full, n);
    put(&full, E::Confirm(n, id));
    assert!(get(&full, &mut hb).is_none());
    let p = tmp.path().join("fragmented");
    assert!(
        Command::new(&cli)
            .arg("sample-request")
            .arg(&p)
            .arg("abcdefgh".repeat(240))
            .status()
            .unwrap()
            .success()
    );
    let fragmented = std::fs::read(&p).unwrap();
    assert!(fragmented.len() > ace_transport::MSS);
    x.k.send(&hold).unwrap();
    for i in 1..=12 {
        x.k.send(&session(&fragmented, i)).unwrap();
    }
    let end = Clock::now() + WAIT;
    while count(a, "net-held") == held {
        assert!(Clock::now() < end);
        x.step();
    }
    assert!(x.received.is_empty());
    y.k.send(&resume).unwrap();
    y.through(1);
    x.through(13);
    assert_eq!(
        x.received[1..]
            .iter()
            .map(|b| reply_id(b))
            .collect::<Vec<_>>(),
        (1..=12).collect::<Vec<_>>()
    );
    assert!(x.reordered && x.packets > 11);
    until("所有受损链路回复均已记录", || {
        count(a, "net-data") >= offered + 14
    });
    assert_eq!(count(a, "net-data"), offered + 14);
    // 停止从此对端收/ACK；经真实 NetSend 将本地应用/核心出口推到 Busy。其它 peer 仍可前进。
    let mut large = vec![0; MAX_FRAME];
    let source = Frame::decode(&data, frame_limits(MAX_FRAME)).unwrap();
    let bytes = vec![9; MAX_FRAME - 116];
    Frame {
        body: &bytes,
        ..source
    }
    .encode_into(&mut large, frame_limits(MAX_FRAME))
    .unwrap();
    let mut oversized = vec![0; MAX_FRAME + 1];
    let body = vec![9; MAX_FRAME + 1 - 116];
    Frame {
        body: &body,
        ..source
    }
    .encode_into(&mut oversized, frame_limits(MAX_FRAME + 1))
    .unwrap();
    assert_eq!(
        op(a, HostOp::NetSend(xc, oversized.as_slice().into())),
        Status::TOO_SMALL
    );
    let mut busy = false;
    for _ in 0..12 {
        match op(a, HostOp::NetSend(xc, large.as_slice().into())) {
            Status::OK => {}
            Status::BUSY => {
                busy = true;
                break;
            }
            e => panic!("本地出口 {e:?}"),
        }
    }
    assert!(busy);
    let end = Clock::now() + WAIT;
    while count(a, "net-writable") == 0 {
        assert!(Clock::now() < end);
        x.step();
    }
    y.k.send(&session(&data, 55)).unwrap();
    y.through(2);
    assert_eq!(reply_id(&y.received[1]), 55);
    // 正确 UDP peer 但错误外层、正确 token 但不同 peer 都不能续命。在等待上界内反复发送自身合法段。
    let mut inner = Session::new(old.conv, 5000, MAX_FRAME).unwrap();
    inner.send(&Hello.encode()).unwrap();
    inner.update(Duration::from_millis(20)).unwrap();
    let packet = inner.packet().unwrap();
    let wrong = KcpSession {
        token: random_nonce().unwrap(),
        ..old
    };
    let wrong_peer = udp(ua);
    let end = Clock::now() + WAIT;
    while !lock(&c.state).net.slot(xc).is_ok_and(|s| {
        matches!(
            s.state,
            Link::Closed {
                reason: Status::TIMEOUT,
                ..
            }
        )
    }) {
        assert!(Clock::now() < end);
        put(&x.s, E::Data(wrong, &packet));
        put(&wrong_peer, E::Data(old, &packet));
        y.step();
    }
    release(c, a, xc);
    assert_eq!(op(a, HostOp::NetClose(yc)), Status::OK);
    release(c, a, yc);
    // 同端点重新握手必须有新 token；旧数据和 Confirm 均不复活旧代次。
    x.s.set_read_timeout(Some(Duration::from_millis(2)))
        .unwrap();
    let mut datagram = [0; MAX_UDP_PAYLOAD + 1];
    while x.s.recv(&mut datagram).is_ok() {}
    put(&x.s, E::Data(old, &packet));
    assert!(get(&x.s, &mut datagram).is_none());
    let mut fresh = Client::new(x.s, false);
    put(&fresh.s, E::Data(old, &packet));
    fresh.k.send(&session(&data, 88)).unwrap();
    fresh.through(1);
    assert_eq!(reply_id(&fresh.received[0]), 88);
    assert_ne!(fresh.id.token, old.token);
    let active = conn(c, a, fresh.s.local_addr().unwrap());
    assert_ne!(active.handle.generation, xc.handle.generation);
    assert_eq!(op(a, HostOp::NetReleaseClosed(xc)), Status::NOT_FOUND);
    let mut stopping = tcp(bound(o, Transport::Tcp));
    let stopping_conn = conn(c, o, stopping.local_addr().unwrap());
    stopping.write_all(&pause).unwrap();
    read(&mut stopping);
    until("待停 owner 已暂停", || {
        count(o, "net-task-paused") > 0 && !lock(&c.state).actors[&o].net_capacity
    });
    let unoffered = tcp(bound(o, Transport::Tcp));
    let closing = reserved(c, o, unoffered.local_addr().unwrap());
    assert_eq!(op(a, HostOp::NetClose(active)), Status::OK);
    until("关闭仍占槽", || {
        lock(&c.state).net.slot(active).is_ok_and(|s| {
            matches!(
                s.state,
                Link::Closed {
                    delivered: true,
                    ..
                }
            )
        })
    });
    let accepts = count(a, "net-accepted");
    let refused = udp(ua);
    let n = random_nonce().unwrap();
    let id = offer(&refused, n);
    put(&refused, E::Confirm(n, id));
    assert!(get(&refused, &mut datagram).is_none());
    assert_eq!(count(a, "net-accepted"), accepts);
    release(c, a, active);
    let replacement = Client::new(refused, false);
    conn(c, a, replacement.s.local_addr().unwrap());
    node.stop(other).unwrap();
    assert_eq!(
        net_events(o, closing),
        ["cleanup"],
        "Stopping 不补发普通连接事件"
    );
    assert_eq!(
        net_events(o, stopping_conn),
        ["accepted", "data", "cleanup"]
    );
    assert_eq!(count(o, "net-cleanup"), 2);
    assert_eq!(count(o, "cancel"), 1);
    assert_eq!(count(o, "destroy"), 1);
    let mut faulting = tcp(ta);
    conn(c, a, faulting.local_addr().unwrap());
    faulting.write_all(&sample("defer-accept")).unwrap();
    read(&mut faulting);
    let unseen = tcp(ta);
    let rejected = reserved(c, a, unseen.local_addr().unwrap());
    until("故障前确有暂拒的 Accepted", || {
        !net_events(a, rejected).is_empty()
    });
    faulting.write_all(&fault).unwrap();
    until("Faulted 回收 UDP 与 TCP", || {
        !lock(&c.state).actors.contains_key(&a) && count(a, "destroy") == 1
    });
    assert!(
        net_events(a, rejected).iter().all(|s| s == "refused"),
        "Faulted 不补发 Accepted/Closed"
    );
    assert_eq!(net_events(a, fc), expected, "恢复业务后也不再投旧代 Data");
    assert_eq!(count(a, "net-cleanup"), 0);
    assert_eq!(count(a, "cancel"), 1);
    assert!(TcpStream::connect_timeout(&ta, Duration::from_millis(100)).is_err());
    assert_eq!(lock(&c.state).net.bytes.used(), 0);
    assert_eq!(node.usage(), (0, 0, 0));
    println!("同宿主双传输、实库回调、损耗/重复、跨会话进展、容量/旧代次及停止清账通过");
}

#[test]
#[ignore = "smoke 单进程装载正式三角色及玩家样例，并观测真实资源账本"]
fn gateway_close_keeps_slot_until_watchdog_accepts() {
    use super::super::tests::{hold_idle, listener_endpoint};
    start_trace();
    let dir = PathBuf::from(std::env::var_os("ACE_NETWORK_LIBS").unwrap());
    let cli = dir.join("ace-test-client");
    let mut m =
        Manifest::read(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/server.toml"))
            .unwrap();
    m.budgets.net_connections = 1.try_into().unwrap();
    m.budgets.workers = 4.try_into().unwrap();
    let tmp = tempfile::tempdir_in(&dir).unwrap().keep();
    let ids = tmp.join("identity");
    crate::identity::provision_new(&ids, m.node).unwrap();
    // SAFETY: 只装载本次 release 构建的清单库。
    let registry = unsafe { Implementations::load_startup(&m.library_paths(&dir)) }.unwrap();
    let node = Node::start(&m, registry, &ids).unwrap();
    let core = &node.core;
    let (gate, wd, target) = (
        node.resolve("gate").unwrap(),
        node.resolve("sessions").unwrap(),
        node.resolve("player").unwrap(),
    );
    let (g, w) = {
        let s = lock(&core.state);
        (s.key(gate.bits()).unwrap(), s.key(wd.bits()).unwrap())
    };
    // 这对实例、权限和实际订阅完全有效；同一个入口在发布后不能作为角色查询使用。
    assert_eq!(
        (startup_peer::API.check)(
            g,
            wd.bits(),
            StartupPeer {
                role: "watchdog".into(),
                send_methods: (&[][..]).into(),
                receive_methods: (&[][..]).into(),
                require_player_entry: false,
                require_subscription: true,
            }
        ),
        Status::NOT_READY
    );
    let ta = listener_endpoint(g, "gate-listen", Transport::Tcp);
    let sample = |file: &str, op: &str, text: &str| {
        let path = tmp.join(file);
        assert!(
            Command::new(&cli)
                .arg(op)
                .arg(&path)
                .arg(text)
                .status()
                .unwrap()
                .success()
        );
        std::fs::read(path).unwrap()
    };
    let login = sample("login", "sample-admission", "demo");
    let deny = sample("deny", "sample-admission", "deny");
    let echo = sample("echo", "sample-request", "ACE");
    let timers = || lock(&core.state).actors[&w].timers.len();
    let released = |c| {
        lock(&core.state)
            .net
            .slot(c)
            .is_ok_and(|s| matches!(s.state, Link::Released))
    };
    let mut stream = tcp(ta);
    let first = conn(core, g, stream.local_addr().unwrap());
    until("Accepted 后有本代独立准入 timer", || timers() == 2);
    stream.write_all(&[&login[..], &echo].concat()).unwrap();
    assert_eq!(reply_id(&read(&mut stream)), request_session(&echo));
    until(
        "Bind 成功只剩重试 timer，pending 和受管字节归零",
        || timers() == 1 && node.usage() == (4, 0, 0),
    );
    // 目标声明了该服务方法但未标 client-callable。使用实际描述，不把未知方法当作权限验证。
    let private =
        crate::loader::find_method(core.descriptor("example.player").unwrap(), 515).unwrap();
    assert_eq!(
        core.select(g, target.bits(), private.wire, ReplyMode::RawBytes)
            .err(),
        Some(Status::DENIED)
    );
    let mut req = Frame::decode(&echo, frame_limits(MAX_FRAME)).unwrap();
    req.wire = ace_net_proto::Contract {
        key: private.wire.key,
        codec: private.wire.codec,
        request: private.wire.request,
        response: private.wire.response,
    };
    let mut frame = vec![0; echo.len()];
    req.encode_into(&mut frame, frame_limits(MAX_FRAME))
        .unwrap();
    stream.write_all(&frame).unwrap();
    let r = read(&mut stream);
    let r = Frame::decode(&r, frame_limits(MAX_FRAME)).unwrap();
    assert!(
        matches!(
            r.message,
            Kind::Response {
                error: Some(ace_net_proto::RpcError::Protocol),
                ..
            }
        ) && r.body.is_empty()
    );
    let limit = core.b.payload_bytes.get();
    let target_budget = {
        let s = lock(&core.state);
        s.actors[&s.key(target.bits()).unwrap()].budget.clone()
    };
    let full = target_budget.reserve(limit, limit).unwrap();
    stream.write_all(&echo).unwrap();
    let busy = read(&mut stream);
    let busy = Frame::decode(&busy, frame_limits(MAX_FRAME)).unwrap();
    assert!(
        matches!(busy.message, Kind::Response { error:Some(ace_net_proto::RpcError::Busy), session } if session.get() == request_session(&echo))
            && busy.body.is_empty()
    );
    drop(full);
    let permit = lock(&core.state).actors[&w]
        .budget
        .reserve(limit, limit)
        .unwrap();
    let refused = count(g, "notice-refused");
    drop(stream);
    until(
        "真实 Watchdog 配额满，Close 多轮暂拒但 socket/缓冲已收回",
        || {
            count(g, "notice-refused") >= refused + 3 && {
                let s = lock(&core.state);
                !s.net.room(core.b, Transport::Tcp)
                    && s.net.bytes.used() == 0
                    && matches!(
                        s.net.slot(first).unwrap().state,
                        Link::Closed {
                            delivered: false,
                            ..
                        }
                    )
            }
        },
    );
    drop(permit);
    until("Watchdog 真接纳才释放同代 conn 槽", || {
        released(first)
            && count(
                w,
                &format!(
                    "watchdog-close conn={}:{} remaining=0",
                    first.handle.id, first.handle.generation
                ),
            ) == 1
    });
    for _ in 0..2 {
        assert_eq!(op(g, HostOp::NetReleaseClosed(first)), Status::OK);
    }
    println!("Close 多轮 Busy：socket/bytes=0，唯一 conn 槽仍占用，Watchdog 接纳后幂等归还");
    let mut denied = tcp(ta);
    let dc = conn(core, g, denied.local_addr().unwrap());
    until("被拒前已登记 timer", || timers() == 2);
    denied.write_all(&deny).unwrap();
    assert_eq!(denied.read(&mut [0]).unwrap(), 0);
    until("拒绝且 Kick 后无准入 timer", || {
        released(dc) && timers() == 1
    });

    let pending = tcp(ta);
    let pc = conn(core, g, pending.local_addr().unwrap());
    until("未握手连接有独立准入 timer", || timers() == 2);
    drop(pending);
    until("未绑定即 Close 也撤销 timer", || {
        released(pc) && timers() == 1
    });

    // 复用上面的有限损耗客户端，不另建代理。通过默认 Gate/Watchdog 的分片业务消息只交付一次。
    let ka = listener_endpoint(g, "gate-listen", Transport::Kcp);
    let mut impaired = Client::new(udp(ka), true);
    let ic = conn(core, g, impaired.s.local_addr().unwrap());
    impaired.k.send(&login).unwrap();
    let fragmented = sample("fragmented", "sample-request", &"abcdefgh".repeat(240));
    assert!(fragmented.len() > ace_transport::MSS);
    let fragment_body = Frame::from_player(&fragmented, frame_limits(MAX_FRAME))
        .unwrap()
        .body;
    let echoes = count(0, &format!("player-echo gate={}", gate.bits()));
    for id in 1..=6 {
        impaired.k.send(&session(&fragmented, id)).unwrap();
    }
    impaired.through(6);
    assert!(impaired.reordered && impaired.packets > 11);
    assert_eq!(impaired.received.len(), 6);
    for (id, bytes) in (1..=6).zip(&impaired.received) {
        assert_eq!(reply_id(bytes), id);
        assert_eq!(
            Frame::decode(bytes, frame_limits(MAX_FRAME)).unwrap().body,
            fragment_body
        );
    }
    assert_eq!(
        count(0, &format!("player-echo gate={}", gate.bits())),
        echoes + 6
    );
    assert_eq!(op(g, HostOp::NetClose(ic)), Status::OK);
    until("有限损耗会话本地终结后归还", || released(ic));
    println!("默认 Gate：有限丢包、乱序、重复下六个分片请求在非等待 Echo 上有序且无重复");
    // 真实 KCP 端停止 ACK，两份最大帧充满核心、两份充满 host 出口。用有限屏障让普通回复恰在满时返回。
    let mut client = Client::new(udp(ka), false);
    let k = conn(core, g, client.s.local_addr().unwrap());
    client.k.send(&login).unwrap();
    client.k.send(&echo).unwrap();
    client.through(1);
    let held = sample("held", "sample-request", "held");
    client.k.send(&held).unwrap();
    let end = Clock::now() + WAIT;
    while count(0, "hold-callback") == 0 {
        assert!(Clock::now() < end);
        client.step();
    }
    let mut large = vec![0; MAX_FRAME];
    Frame {
        body: &vec![0; MAX_FRAME - 116],
        ..Frame::decode(&echo, frame_limits(MAX_FRAME)).unwrap()
    }
    .encode_into(&mut large, frame_limits(MAX_FRAME))
    .unwrap();
    for _ in 0..4 {
        until("有限 KCP 出口接纳", || {
            match op(g, HostOp::NetSend(k, large.as_slice().into())) {
                Status::OK => true,
                Status::BUSY => false,
                e => panic!("未预期出口 {e:?}"),
            }
        });
    }
    assert_eq!(
        op(g, HostOp::NetSend(k, echo.as_slice().into())),
        Status::BUSY
    );
    open_barrier("hold-callback");
    until("普通自动回复在出口满时本地关闭 KCP", || {
        count(
            g,
            &format!(
                "gate-egress conn={}:{} status=1",
                k.handle.id, k.handle.generation
            ),
        ) == 1
            && released(k)
    });
    println!("KCP 出口已满：同一受控回复出口 Busy，未另建字节队列，显式本地终结后归还");

    let mut local = tcp(ta);
    let lc = conn(core, g, local.local_addr().unwrap());
    local.write_all(&[&login[..], &echo].concat()).unwrap();
    read(&mut local);
    node.stop(target).unwrap();
    assert_eq!(local.read(&mut [0]).unwrap(), 0);
    until("已订阅本机 target 终结，清理连接", || {
        released(lc)
    });
    let old = tcp(ta);
    until("未绑定时仍有准入 timer", || timers() == 2);
    // 此时把 Watchdog 留在普通邮箱取出前。旧 Close 和下一代 Open 均已入队，Gate 先 Faulted。
    hold_idle(core, w);
    drop(old);
    let other = tcp(ta);
    let oc = conn(core, g, other.local_addr().unwrap());
    until("未消费的两条角色单向消息", || {
        lock(&core.state).actors[&w].queued >= 2
    });
    lock(&core.state).stop(g, Status::FAULTED);
    until(
        "故障 Gate 的真实 socket、槽和订阅均终结",
        || {
            let s = lock(&core.state);
            !s.actors.contains_key(&g)
                && s.net.occupied() == 0
                && s.net.bytes.used() == 0
                && s.actors[&w].subs.values().any(|t| t.gone)
        },
    );
    assert_eq!(
        count(
            g,
            &format!(
                "gate-cleanup conn={}:{} release=0",
                oc.handle.id, oc.handle.generation
            )
        ),
        0
    );
    open_barrier("wait-gap");
    until(
        "ResourceGone 后旧 Open 不建立会话，未消费 timer 已撤销",
        || {
            count(w, "watchdog-gate-gone") == 1
                && timers() == 0
                && node.usage() == (2, 0, 0)
                && lock(&core.state).actors[&w].queued == 0
        },
    );
    assert_eq!(
        count(
            w,
            &format!(
                "watchdog-open gate={} conn={}:{}",
                gate.bits(),
                oc.handle.id,
                oc.handle.generation
            )
        ),
        0
    );
    println!(
        "本地 target 停止触发关闭；Gate 故障不恢复业务。Watchdog ResourceGone 先于晚 Open，准入及重试 timer、连接、pending 均归零"
    );
}
fn request_session(bytes: &[u8]) -> u64 {
    let Kind::Request(q) = Frame::from_player(bytes, frame_limits(MAX_FRAME))
        .unwrap()
        .message
    else {
        panic!("不是请求")
    };
    q.session.get()
}
