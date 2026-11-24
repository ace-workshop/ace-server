//! 同步测试工具的有期限交换；KCP 字段检查、分片、ACK 和重传使用与宿主同一适配器。
use ace_net_proto::bootstrap::{Hello, KcpEnvelope as E, KcpSession};
use ace_transport::{MAX_UDP_PAYLOAD, PUBLIC_HEADER, Session, frame_head, random_nonce};
use std::{
    error::Error,
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    time::{Duration, Instant},
};
const TIMEOUT: Duration = Duration::from_secs(5);
type Result<T> = std::result::Result<T, Box<dyn Error>>;
// 协调两个独立 CLI：传输握手完成后才放行二者的应用帧。手工使用无需屏障。
fn start(barrier: bool) -> Result<()> {
    if barrier {
        println!("transport-ready");
        std::io::stdout().flush()?;
        std::io::stdin().read_exact(&mut [0])?;
    }
    Ok(())
}
const QUIET: Duration = Duration::from_millis(180);
pub(super) fn tcp(
    addr: SocketAddr,
    request: &[u8],
    admission: Option<&[u8]>,
    barrier: bool,
) -> Result<Vec<u8>> {
    let mut s = TcpStream::connect_timeout(&addr, TIMEOUT)?;
    s.set_write_timeout(Some(TIMEOUT))?;
    let end = Instant::now() + TIMEOUT;
    s.write_all(&Hello.encode())?;
    let mut hello = [0; 12];
    exact(&mut s, &mut hello, end)?;
    Hello::decode(&hello)?;
    start(barrier)?;
    if let Some(q) = admission {
        s.write_all(q)?;
    }
    s.write_all(request)?;
    let mut h = [0; PUBLIC_HEADER];
    exact(&mut s, &mut h, end)?;
    let head = frame_head(&h, 256)?;
    head.check_response(
        ace_example_protocol::echo::<super::Selected>().wire(),
        super::CALL,
    )?;
    let mut frame = vec![0; head.frame_len];
    frame[..h.len()].copy_from_slice(&h);
    exact(&mut s, &mut frame[h.len()..], end)?;
    if admission.is_some() {
        s.set_read_timeout(Some(QUIET))?;
        match s.read(&mut [0]) {
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            _ => return Err("TCP 关联响应后还有字节或提前关闭".into()),
        }
        println!("player-response count=1 session={}", super::CALL.session);
    }
    Ok(frame)
}
fn exact(s: &mut TcpStream, mut b: &mut [u8], end: Instant) -> io::Result<()> {
    while !b.is_empty() {
        s.set_read_timeout(Some(
            end.checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or(io::ErrorKind::TimedOut)?,
        ))?;
        let n = s.read(b)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        b = &mut b[n..];
    }
    Ok(())
}
fn send(s: &UdpSocket, e: E<'_>) -> Result<()> {
    let mut b = [0; MAX_UDP_PAYLOAD];
    let n = e.encode_into(&mut b, MAX_UDP_PAYLOAD)?;
    if s.send(&b[..n])? != n {
        return Err("UDP 未整包发送".into());
    }
    Ok(())
}
fn recv<'a>(s: &UdpSocket, b: &'a mut [u8], wait: Duration) -> Result<Option<E<'a>>> {
    s.set_read_timeout(Some(wait.max(Duration::from_millis(1))))?;
    match s.recv(b) {
        Ok(n) => Ok(E::decode(&b[..n], MAX_UDP_PAYLOAD).ok()),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}
fn handshake(s: &UdpSocket, b: &mut [u8], end: Instant) -> Result<KcpSession> {
    let nonce = random_nonce()?;
    let mut offered = None;
    let mut retry = Instant::now();
    loop {
        let now = Instant::now();
        if now >= end {
            return Err("KCP 建立超时".into());
        }
        if now >= retry {
            send(s, E::Hello(nonce))?;
            if let Some(id) = offered {
                send(s, E::Confirm(nonce, id))?;
            }
            retry = now + Duration::from_millis(100);
        }
        match recv(s, b, retry.min(end).saturating_duration_since(now))? {
            Some(E::Offer(n, id)) if n == nonce => {
                offered = Some(id);
                send(s, E::Confirm(nonce, id))?;
            }
            Some(E::Ready(n, id)) if n == nonce && offered == Some(id) => return Ok(id),
            _ => {}
        }
    }
}
pub(super) fn kcp(
    addr: SocketAddr,
    request: &[u8],
    admission: Option<&[u8]>,
    barrier: bool,
) -> Result<Vec<u8>> {
    let s = UdpSocket::bind(if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    })?;
    s.connect(addr)?;
    s.set_write_timeout(Some(TIMEOUT))?;
    let mut buf = [0; MAX_UDP_PAYLOAD + 1];
    let end = Instant::now() + TIMEOUT;
    let id = handshake(&s, &mut buf, end)?;
    let born = Instant::now();
    let mut k = Session::new(id.conv, 5000, 256)?;
    k.send(&Hello.encode())?;
    let mut hello = false;
    let mut received = None;
    let mut end = end;
    loop {
        let now = Instant::now();
        if now >= end {
            if let Some(reply) = received {
                println!("player-response count=1 session={}", super::CALL.session);
                return Ok(reply);
            }
            return Err("KCP 应答超时".into());
        }
        let delay = k.update(now - born)?;
        while let Some(p) = k.packet() {
            send(&s, E::Data(id, &p))?;
        }
        if let Some(n) = k.peek()? {
            if n > 256 {
                return Err("应答超过测试预算".into());
            }
            let mut b = vec![0; n];
            k.recv(&mut b)?;
            if hello {
                if admission.is_none() {
                    return Ok(b);
                }
                if received.replace(b).is_some() {
                    return Err("KCP 收到重复应用消息".into());
                }
                end = now + QUIET;
                continue;
            }
            Hello::decode(&b)?;
            start(barrier)?;
            if let Some(q) = admission {
                k.send(q)?;
            }
            k.send(request)?;
            hello = true;
            continue;
        }
        if let Some(E::Data(got, p)) = recv(&s, &mut buf, delay.min(end - now))?
            && got == id
        {
            match k.input(born.elapsed(), p) {
                Ok(()) | Err(ace_transport::Error::Invalid | ace_transport::Error::Limit) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
}
