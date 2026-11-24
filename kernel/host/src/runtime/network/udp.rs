use super::*;
use ace_net_proto::bootstrap::{Hello, KcpEnvelope as E, KcpSession};
use ace_transport::{Error, MAX_UDP_PAYLOAD, Session, random_nonce};
use std::{io::ErrorKind, num::NonZeroU32};
use tokio::net::UdpSocket;

pub(super) const HANDSHAKES: usize = 32;
const BATCH: usize = 16;
#[derive(Clone, Copy)]
struct Offer {
    nonce: NonZeroU64,
    session: KcpSession,
    until: Instant,
}
struct Peer {
    offer: Offer,
    conn: Connection,
    core: Session,
    born: Instant,
    next: Instant,
    hello: bool,
}
struct ListenerIo<'a> {
    c: &'a Core,
    key: u64,
    h: ResourceHandle,
    wake: Arc<Notify>,
    socket: UdpSocket,
    offers: HashMap<SocketAddr, Offer>,
    peers: HashMap<SocketAddr, Peer>,
    turns: VecDeque<SocketAddr>,
    drops: u64,
}
// 一个非阻塞出口，发送要么整包、要么按本地丢失计数。WouldBlock 不拖住共享 listener。
fn write(socket: &UdpSocket, drops: &mut u64, to: SocketAddr, e: E<'_>) -> Result<(), Status> {
    let mut buf = [0; MAX_UDP_PAYLOAD];
    let n = e
        .encode_into(&mut buf, MAX_UDP_PAYLOAD)
        .map_err(|_| Status::PROTOCOL)?;
    match socket.try_send_to(&buf[..n], to) {
        Ok(k) if k == n => Ok(()),
        Err(e) if e.kind() == ErrorKind::WouldBlock => {
            *drops = drops.saturating_add(1);
            Ok(())
        }
        _ => Err(Status::UNREACHABLE),
    }
}
fn terminated(e: Error) -> Status {
    match e {
        Error::Expired => Status::TIMEOUT,
        Error::Disconnected => Status::UNREACHABLE,
        _ => Status::PROTOCOL,
    }
}
impl ListenerIo<'_> {
    async fn receive(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let (n, peer) = self.socket.recv_from(buf).await?;
        self.input(peer, &buf[..n], Instant::now());
        for _ in 1..BATCH {
            match self.socket.try_recv_from(buf) {
                Ok((n, peer)) => self.input(peer, &buf[..n], Instant::now()),
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    fn input(&mut self, from: SocketAddr, bytes: &[u8], now: Instant) {
        let Ok(e) = E::decode(bytes, MAX_UDP_PAYLOAD) else {
            return;
        };
        if let Some(p) = self.peers.get_mut(&from) {
            let r = match e {
                E::Hello(n) if n == p.offer.nonce => write(
                    &self.socket,
                    &mut self.drops,
                    from,
                    E::Offer(n, p.offer.session),
                ),
                E::Confirm(n, s) if n == p.offer.nonce && s == p.offer.session => {
                    write(&self.socket, &mut self.drops, from, E::Ready(n, s))
                }
                E::Data(s, b) if s == p.offer.session => match p.core.input(now - p.born, b) {
                    Ok(()) => {
                        p.next = now;
                        Ok(())
                    }
                    Err(Error::Invalid | Error::Limit) => Ok(()),
                    Err(e) => Err(terminated(e)),
                },
                _ => return,
            };
            if let Err(reason) = r {
                self.remove(from, reason);
            }
            return;
        }
        match e {
            E::Hello(n) => {
                if !lock(&self.c.state).accepting(self.key) {
                    return;
                }
                let offer = if let Some(o) = self.offers.get(&from) {
                    if o.nonce != n || o.until <= now {
                        return;
                    }
                    *o
                } else {
                    if self.offers.len() >= HANDSHAKES {
                        return;
                    }
                    let Ok(token) = random_nonce() else { return };
                    let Ok(bits) = random_nonce() else { return };
                    let Some(conv) = NonZeroU32::new(bits.get() as u32) else {
                        return;
                    };
                    let Some(epoch) = NonZeroU64::new(self.c.boot) else {
                        return;
                    };
                    let offer = Offer {
                        nonce: n,
                        session: KcpSession { epoch, token, conv },
                        until: now + HELLO_TIMEOUT,
                    };
                    self.offers.insert(from, offer);
                    offer
                };
                if write(
                    &self.socket,
                    &mut self.drops,
                    from,
                    E::Offer(n, offer.session),
                )
                .is_err()
                {
                    self.offers.remove(&from);
                }
            }
            E::Confirm(n, id) => {
                let Some(&offer) = self.offers.get(&from) else {
                    return;
                };
                if offer.nonce != n || offer.session != id || offer.until <= now {
                    return;
                }
                self.offers.remove(&from);
                let conn = {
                    let mut s = lock(&self.c.state);
                    self.c.reserve_conn(
                        &mut s,
                        self.key,
                        self.h,
                        Transport::Kcp,
                        from,
                        IoWake::shared(&self.wake),
                    )
                };
                let Ok(conn) = conn else { return }; // 所有权、初始字节和终态槽均已预留才建核心。
                let session = Session::new(
                    id.conv,
                    self.c.b.net_idle_ms.get(),
                    Network::cap(self.c.b, Transport::Kcp),
                )
                .and_then(|mut s| {
                    s.send(&Hello.encode())?;
                    Ok(s)
                });
                match session {
                    Ok(core) => {
                        self.turns.push_back(from);
                        self.peers.insert(
                            from,
                            Peer {
                                offer,
                                conn,
                                core,
                                born: now,
                                next: now,
                                hello: false,
                            },
                        );
                        if let Err(e) = write(&self.socket, &mut self.drops, from, E::Ready(n, id))
                        {
                            self.remove(from, e);
                        }
                    }
                    Err(e) => self.c.finish_conn(conn, terminated(e)),
                }
            }
            _ => {} // 陌生 Data、过期 Confirm 不建核心，也不替换任何会话。
        }
    }
    fn remove(&mut self, peer: SocketAddr, reason: Status) {
        if let Some(p) = self.peers.remove(&peer) {
            self.turns.retain(|&k| k != peer);
            let c = p.conn;
            drop(p);
            self.c.finish_conn(c, reason);
        }
    }
    fn advance(&mut self, now: Instant) -> Instant {
        self.offers.retain(|_, o| o.until > now);
        let mut next = self
            .offers
            .values()
            .map(|o| o.until)
            .min()
            .unwrap_or(now + HELLO_TIMEOUT);
        // 每轮每会话至多一个应用读/写和一小批完整报文；轮换首个出口，不让单个大帧占满共享 socket。
        for _ in 0..self.turns.len() {
            let addr = self.turns.pop_front().expect("非空轮转槽");
            let p = self.peers.get_mut(&addr).expect("会话与轮转槽同时登记");
            let r = (|| {
                if !p.hello && now - p.born >= HELLO_TIMEOUT {
                    return Err(Status::TIMEOUT);
                }
                if p.next <= now {
                    p.next = now + p.core.update(now - p.born).map_err(terminated)?;
                }
                if let Some(b) = self.c.output(p.conn)?
                    && p.hello
                {
                    match p.core.send(&b) {
                        Ok(()) => {
                            self.c.sent(p.conn);
                            p.next = now;
                        }
                        Err(Error::Busy) => {}
                        Err(e) => return Err(terminated(e)),
                    }
                }
                let cap = Network::cap(self.c.b, Transport::Kcp);
                if (!p.hello || self.c.can_read(self.key, p.conn))
                    && let Some(n) = p.core.peek().map_err(terminated)?
                {
                    if n > cap || !p.hello && n != 12 {
                        return Err(Status::PROTOCOL);
                    }
                    let mut buf = vec![0; n];
                    p.core.recv(&mut buf).map_err(terminated)?;
                    if !p.hello {
                        Hello::decode(&buf).map_err(|_| Status::PROTOCOL)?;
                        p.hello = true;
                    } else {
                        Frame::from_player(&buf, frame_limits(cap))
                            .map_err(|_| Status::PROTOCOL)?;
                        self.c.data(self.key, p.conn, buf)?;
                    }
                }
                for i in 0..8 {
                    let Some(packet) = p.core.packet() else {
                        break;
                    };
                    if i == 7 {
                        p.next = now;
                    }
                    write(
                        &self.socket,
                        &mut self.drops,
                        addr,
                        E::Data(p.offer.session, &packet),
                    )?;
                }
                Ok(())
            })();
            match r {
                Ok(()) => {
                    next = next.min(p.next);
                    self.turns.push_back(addr);
                }
                Err(e) => {
                    let conn = p.conn;
                    self.peers.remove(&addr);
                    self.c.finish_conn(conn, e);
                }
            }
        }
        if self.turns.len() > 1 {
            self.turns.rotate_left(1);
        }
        next
    }
}
pub(super) async fn listen(
    c: Arc<Core>,
    key: u64,
    h: ResourceHandle,
    addr: SocketAddr,
    wake: Arc<Notify>,
) {
    let socket = match UdpSocket::bind(addr).await {
        Ok(s) => s,
        Err(_) => {
            c.listen_result(key, h, Err(Status::UNREACHABLE));
            return;
        }
    };
    let Ok(bound) = socket.local_addr() else {
        c.listen_result(key, h, Err(Status::UNREACHABLE));
        return;
    };
    // 初始化可写就绪（try_send_to 未就绪返回 WouldBlock）；后续满按有界本地丢包。
    if socket.writable().await.is_err() {
        c.listen_result(key, h, Err(Status::UNREACHABLE));
        return;
    }
    c.listen_result(key, h, Ok(bound));
    while !lock(&c.state).accepting(key) {
        wake.notified().await;
    }
    let mut io = ListenerIo {
        c: &c,
        key,
        h,
        wake: wake.clone(),
        socket,
        offers: HashMap::new(),
        peers: HashMap::new(),
        turns: VecDeque::new(),
        drops: 0,
    };
    let mut buf = [0; MAX_UDP_PAYLOAD + 1];
    loop {
        let next = io.advance(Instant::now());
        let result = tokio::select! {
            result = io.receive(&mut buf) => result,
            () = wake.notified() => continue,
            () = tokio::time::sleep_until(next) => continue,
        };
        if let Err(error) = result {
            // 无来源的临时 UDP/ICMP 错误不归咎于所有 peer；对应会话仍受 idle/deadlink 限制。
            if matches!(
                error.kind(),
                ErrorKind::Interrupted
                    | ErrorKind::WouldBlock
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::HostUnreachable
                    | ErrorKind::NetworkUnreachable
            ) {
                continue;
            }
            eprintln!("KCP listener {} I/O: {}", h.id, error.kind());
            break;
        }
    }
    // 共享 socket 本身失效才同时终结所属会话；单个 peer 的写错误只关本代。
    for (_, p) in io.peers.drain() {
        let conn = p.conn;
        drop(p);
        c.finish_conn(conn, Status::UNREACHABLE);
    }
    drop(io);
    c.retired_listener(key, h);
}
