use super::super::network::tcp::read_error;
use super::*;
use ace_net_proto::{Frame, Message as Kind, NodeEntry, Route, RpcError};
use ace_transport::PUBLIC_HEADER;
use std::num::NonZeroU16;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpSocket, TcpStream,
        tcp::{ReadHalf, WriteHalf},
    },
};
fn unreachable(_: std::io::Error) -> Status {
    Status::UNREACHABLE
}
pub(super) async fn listen(c: Arc<Core>, owner: u64, h: ResourceHandle, addr: SocketAddr) {
    let bind = TcpListener::bind(addr).await;
    {
        let mut s = lock(&c.state);
        let Some(l) = s.peers.listener.as_mut().filter(|l| l.h == h) else {
            return;
        };
        l.result = Some(bind.as_ref().map(|_| addr).map_err(|_| Status::UNREACHABLE));
        s.actors[&owner].signal.notify_one();
    }
    let Ok(listener) = bind else { return };
    loop {
        let Ok((socket, addr)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let mut s = lock(&c.state);
        let Some(l) = s
            .peers
            .listener
            .as_ref()
            .filter(|l| l.owner == owner && l.h == h)
        else {
            return;
        };
        // 入站候选继承监听原动作；不是按到达时刻把 Prepare 所属事件冒充成 Start 的事件。
        let action = l.action;
        let Ok((h, io)) = c.reserve_link(&mut s, owner, addr, action) else {
            continue;
        };
        let task =
            c.rt.spawn(connection(c.clone(), h, io, Socket::Accepted(socket)));
        s.peers.links.get_mut(&h.id).expect("本锁内预留").task = Some(task);
    }
}
pub(super) enum Socket {
    Dial(u16, SocketAddr),
    Accepted(TcpStream),
}
async fn handshake(
    c: &Core,
    h: ResourceHandle,
    socket: Socket,
) -> Result<(TcpStream, PeerHello, SocketAddr), Status> {
    let node = NonZeroU16::new(c.node).ok_or(Status::PROTOCOL)?;
    let boot = NonZeroU64::new(c.boot).ok_or(Status::PROTOCOL)?;
    let mut local = PeerHello {
        node,
        boot,
        dialer: node,
        dialer_boot: boot,
        attempt: NonZeroU64::new(h.generation).ok_or(Status::PROTOCOL)?,
    };
    let (mut stream, expected) = match socket {
        Socket::Dial(n, addr) => {
            let socket = if addr.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(unreachable)?;
            socket
                .bind(SocketAddr::new(c.deployment()?.advertised.ip(), 0))
                .map_err(unreachable)?;
            let mut stream = socket.connect(addr).await.map_err(unreachable)?;
            stream
                .write_all(&local.encode().map_err(|_| Status::PROTOCOL)?)
                .await
                .map_err(unreachable)?;
            (stream, Some(n))
        }
        Socket::Accepted(stream) => (stream, None),
    };
    stream.set_nodelay(true).map_err(unreachable)?;
    let addr = stream.peer_addr().map_err(unreachable)?;
    let mut buf = [0; 36];
    stream.read_exact(&mut buf).await.map_err(unreachable)?;
    let v = PeerHello::decode(&buf).map_err(|_| Status::PROTOCOL)?;
    if v.node == node
        || !c
            .deployment()?
            .permits(v.node.get(), addr, expected.is_some())
    {
        return Err(Status::DENIED);
    }
    if let Some(n) = expected {
        if n != v.node.get()
            || (v.dialer, v.dialer_boot, v.attempt)
                != (local.dialer, local.dialer_boot, local.attempt)
        {
            return Err(Status::PROTOCOL);
        }
    } else {
        if (v.node, v.boot) != (v.dialer, v.dialer_boot) {
            return Err(Status::PROTOCOL);
        }
        local.dialer = v.dialer;
        local.dialer_boot = v.dialer_boot;
        local.attempt = v.attempt;
        stream
            .write_all(&local.encode().map_err(|_| Status::PROTOCOL)?)
            .await
            .map_err(unreachable)?;
    }
    Ok((stream, v, addr))
}
pub(super) async fn connection(c: Arc<Core>, h: ResourceHandle, io: Signals, socket: Socket) {
    let run = async {
        let (mut stream, v, addr) = tokio::time::timeout(HANDSHAKE, handshake(&c, h, socket))
            .await
            .map_err(|_| Status::TIMEOUT)??;
        {
            let mut s = lock(&c.state);
            s.peers.link(h)?;
            if let Err(e) = s.peers.check_boot(v.node.get(), v.boot.get()) {
                drop(s);
                eprintln!(
                    "peer-stale node={} boot={} link={}",
                    v.node, v.boot, h.generation
                );
                return Err(e);
            }
            let l = s.peers.links.get_mut(&h.id).ok_or(Status::UNREACHABLE)?;
            l.hello = Some(v);
            l.observed = addr;
            l.connected = true;
            let owner = l.owner;
            s.actors[&owner].signal.notify_one();
        }
        let (mut r, mut w) = stream.split();
        let expiry = async {
            match tokio::time::timeout(HANDSHAKE, io.admitted.notified()).await {
                Ok(()) => std::future::pending().await,
                Err(_) => Err(Status::TIMEOUT),
            }
        };
        tokio::select! {r=read(&c,h,&io,&mut r)=>r,r=write(&c,h,&io,&mut w)=>r,r=expiry=>r}
    };
    let r = tokio::select! {r=run=>r,()=io.close.notified()=>Err(Status::STOPPED)};
    let mut s = lock(&c.state);
    s.close_link(h, r.err().unwrap_or(Status::OK));
    s.finish_peer(h);
}
async fn read(
    c: &Core,
    h: ResourceHandle,
    io: &Signals,
    r: &mut ReadHalf<'_>,
) -> Result<(), Status> {
    let mut prefix = [0; 140];
    let limits = FrameLimits {
        response_bytes: u32::MAX,
        timeout_ms: u64::MAX,
        ..c.limits()
    };
    loop {
        // 前一个控制尚未消费时暂不抽取下一帧；关闭通知不依赖普通读取恢复。
        let capacity = async {
            loop {
                let ready = {
                    let s = lock(&c.state);
                    s.peers.link(h)?.control.is_none()
                };
                if ready {
                    return Ok(());
                }
                io.read.notified().await;
            }
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        capacity.await?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        tokio::select! {biased;res=capacity=>res?,end=super::super::network::tcp::paused_eof(r.as_ref())=>return end.map_err(unreachable)}
        if r.read(&mut prefix[..1]).await.map_err(read_error)? == 0 {
            return Ok(());
        }
        r.read_exact(&mut prefix[1..8]).await.map_err(read_error)?;
        if PeerReady::decode(&prefix[..8]).is_ok() {
            let mut s = lock(&c.state);
            if s.closing {
                continue;
            }
            let l = s.peers.links.get_mut(&h.id).ok_or(Status::UNREACHABLE)?;
            if !l.remote_ready {
                l.remote_ready = true;
                l.data_notice = l.active();
                let owner = l.owner;
                s.actors[&owner].signal.notify_one();
            }
            continue;
        }
        r.read_exact(&mut prefix[8..PUBLIC_HEADER])
            .await
            .map_err(read_error)?;
        let kind = u16::from_le_bytes([prefix[6], prefix[7]]);
        let len = PUBLIC_HEADER
            + 12 * usize::from(kind == 0x102)
            + 16 * usize::from(kind >> 8 == 1)
            + 8 * usize::from(prefix[4] & 1 != 0);
        r.read_exact(&mut prefix[PUBLIC_HEADER..len])
            .await
            .map_err(read_error)?;
        let (route, head) =
            NodeFrame::inspect(&prefix[..len], limits).map_err(|_| Status::PROTOCOL)?;
        if head.frame_len > c.b.net_frame_bytes.get()
            || head.frame_len != len + head.body_len as usize
        {
            return Err(Status::PROTOCOL);
        }
        let checked = c.preflight(&lock(&c.state), h, route, head)?;
        let b = if checked.is_ok() {
            let mut b = vec![0; head.body_len as usize];
            r.read_exact(&mut b).await.map_err(read_error)?;
            b
        } else {
            let n = tokio::io::copy(
                &mut (&mut *r).take(head.body_len.into()),
                &mut tokio::io::sink(),
            )
            .await
            .map_err(read_error)?;
            if n != head.body_len as u64 {
                return Err(Status::PROTOCOL);
            }
            vec![]
        };
        if matches!(head.message, Kind::Request(_)) {
            // 入站 token 各有错误槽；额外保留一个立即拒收用的错误槽，不能被无数 Busy 回包挤爆。
            let room = async {
                loop {
                    let ready = {
                        let s = lock(&c.state);
                        s.peers.link(h)?;
                        s.inbound_room(h.id, c.b.control_slots.get() + 1)
                    };
                    if ready {
                        return Ok(());
                    }
                    io.read.notified().await;
                }
            };
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            room.await?;
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            tokio::select! {biased;res=room=>res?,end=super::super::network::tcp::paused_eof(r.as_ref())=>return end.map_err(read_error)}
        }
        let mut s = lock(&c.state);
        s.peers.link(h)?;
        if let Some(route) = route {
            let result = c.route_input(&mut s, h, route, head, &b, checked);
            drop(s);
            if head.message == Kind::Send
                && let Err(e) = result
            {
                eprintln!(
                    "peer-send-rejected source={} target={} status={}",
                    route.source.bits(),
                    route.destination.bits(),
                    e.0
                );
            } else {
                result?;
            }
        } else {
            let l = s.peers.links.get_mut(&h.id).ok_or(Status::UNREACHABLE)?;
            l.control = Some((l.stage(), native(head.wire), b.into()));
            let owner = l.owner;
            s.actors[&owner].signal.notify_one();
        }
    }
}
async fn write(
    c: &Core,
    h: ResourceHandle,
    io: &Signals,
    w: &mut WriteHalf<'_>,
) -> Result<(), Status> {
    let mut favor_reply = true;
    loop {
        let next = {
            let mut s = lock(&c.state);
            let queued = s.peers.link(h)?.out.front().map(|o| o.bytes.clone());
            let reply = if favor_reply || queued.is_none() {
                s.pop_port(h.id)
            } else {
                None
            };
            if let Some(p) = reply {
                io.read.notify_one();
                let mut b = vec![0; 120 + p.body.as_ref().map_or(0, |b| b.len())];
                let route = Route {
                    source: p.source.try_into().map_err(|_| Status::PROTOCOL)?,
                    destination: p.destination.try_into().map_err(|_| Status::PROTOCOL)?,
                    entry: NodeEntry::Service,
                };
                let error = p
                    .body
                    .as_ref()
                    .err()
                    .map(|s| RpcError::try_from(s.0))
                    .transpose()
                    .map_err(|_| Status::PROTOCOL)?;
                let response = Frame {
                    wire: contract(p.wire),
                    message: Kind::Response {
                        session: NonZeroU64::new(p.session).ok_or(Status::PROTOCOL)?,
                        error,
                    },
                    body: p.body.as_deref().unwrap_or(&[]),
                };
                NodeFrame::Routed(route, response)
                    .encode_into(&mut b, c.limits())
                    .map_err(|_| Status::PROTOCOL)?;
                Some((Arc::from(b), false))
            } else {
                queued.map(|b| (b, true))
            }
        };
        if let Some((b, queued)) = next {
            w.write_all(&b).await.map_err(unreachable)?;
            favor_reply = queued;
            if queued {
                let mut s = lock(&c.state);
                let l = s.peers.links.get_mut(&h.id).ok_or(Status::UNREACHABLE)?;
                l.out.pop_front();
                l.queued_bytes -= b.len();
                if l.writable == rpc::WriteInterest::Waiting {
                    l.writable.notify();
                    let owner = l.owner;
                    s.actors[&owner].signal.notify_one();
                }
            }
        } else {
            io.write.notified().await;
        }
    }
}
