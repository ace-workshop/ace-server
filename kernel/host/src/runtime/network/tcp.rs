use super::*;
use ace_net_proto::{FrameError, bootstrap::Hello};
use ace_transport::{PUBLIC_HEADER, frame_head};
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{ReadHalf, WriteHalf},
    },
};

pub(super) async fn listen(
    c: Arc<Core>,
    key: u64,
    h: ResourceHandle,
    addr: SocketAddr,
    wake: Arc<Notify>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(_) => {
            c.listen_result(key, h, Err(Status::UNREACHABLE));
            return;
        }
    };
    let Ok(bound) = listener.local_addr() else {
        c.listen_result(key, h, Err(Status::UNREACHABLE));
        return;
    };
    c.listen_result(key, h, Ok(bound));
    loop {
        let room = {
            let s = lock(&c.state);
            s.accepting(key) && s.net.room(c.b, Transport::Tcp)
        };
        if !room {
            wake.notified().await;
            continue;
        }
        let Ok((socket, peer)) = listener.accept().await else {
            // 临时描述符/系统压力不忙循环。
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let mut s = lock(&c.state);
        let io = IoWake::default();
        let Ok(conn) = c.reserve_conn(&mut s, key, h, Transport::Tcp, peer, io.clone()) else {
            continue;
        };
        let task = c.rt.spawn(connection(c.clone(), key, conn, socket, io));
        if let Link::Live(l) = &mut s
            .net
            .slots
            .get_mut(&conn.handle.id)
            .expect("同锁内已预留")
            .state
        {
            l.task = Some(task);
        }
    }
}
pub(in crate::runtime) fn read_error(e: io::Error) -> Status {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        Status::PROTOCOL
    } else {
        Status::UNREACHABLE
    }
}
// 停在帧间时只窥视一字节。无待读字节的 FIN 直接返回 EOF；普通数据不消费，也不是 FIN。
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(in crate::runtime) async fn paused_eof(socket: &TcpStream) -> io::Result<()> {
    if socket.peek(&mut [0]).await? != 0 {
        read_end_with_unread_data(socket).await?;
    }
    socket.take_error()?.map_or(Ok(()), Err)
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn read_end_with_unread_data(socket: &TcpStream) -> io::Result<()> {
    use mio::{Events, Poll, Token, unix::SourceFd};
    use std::os::fd::AsRawFd;
    use tokio::io::{Interest, unix::AsyncFd};
    // Tokio 的 READABLE 混合了普通可读和读端终态；未消费的数据会让同一 readiness 一直就绪。
    // 单独订阅该 socket 的内核边缘；runtime 等 selector fd，非阻塞取走的是事件，不是业务字节。
    let poll = Poll::new()?;
    poll.registry().register(
        &mut SourceFd(&socket.as_raw_fd()),
        Token(0),
        mio::Interest::READABLE,
    )?;
    // SAFETY: AsyncFd 独占新建 Poll，其 selector fd 始终打开且不变；闭包仅取事件，
    // 不交换 Poll。退出或取消时 AsyncFd 先注销再 drop Poll，业务 socket 在此期间仍被借用。
    let mut selector = unsafe { AsyncFd::register_with_interest(poll, Interest::READABLE) }?;
    let mut events = Events::with_capacity(1);
    loop {
        let ready = selector
            .async_io_mut(Interest::READABLE, |poll| {
                poll.poll(&mut events, Some(Duration::ZERO))?;
                if events.is_empty() {
                    Err(io::ErrorKind::WouldBlock.into())
                } else {
                    Ok(())
                }
            })
            .await;
        match ready {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            r => r?,
        }
        for ev in &events {
            if ev.is_error() {
                return Err(socket
                    .take_error()?
                    .unwrap_or_else(|| io::Error::other("TCP 读端错误")));
            }
            if ev.is_read_closed() {
                return Ok(());
            }
            #[cfg(test)]
            if let Ok(peer) = socket.peer_addr() {
                super::super::tests::trace(0, &format!("net-paused-readable {peer}"));
            }
        }
    }
}
async fn read_frames(
    c: &Core,
    key: u64,
    conn: Connection,
    r: &mut ReadHalf<'_>,
    io: &IoWake,
) -> Result<(), Status> {
    let cap = Network::cap(c.b, Transport::Tcp);
    let mut prefix = [0; MAX_PREFIX];
    loop {
        let capacity = async {
            while !c.can_read(key, conn) {
                io.read.notified().await;
            }
        };
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        tokio::select! {
            biased;
            () = capacity => {},
            end = paused_eof(r.as_ref()) => return end.map_err(read_error),
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        capacity.await;
        if r.read(&mut prefix[..1]).await.map_err(read_error)? == 0 {
            return Ok(());
        }
        r.read_exact(&mut prefix[1..PUBLIC_HEADER])
            .await
            .map_err(read_error)?;
        let (frame, head) = match frame_head(&prefix[..PUBLIC_HEADER], cap) {
            Ok(n) => (n, PUBLIC_HEADER),
            Err(FrameError::Truncated) => {
                #[cfg(test)]
                super::super::tests::trace(
                    key,
                    &format!("net-request-prefix {}", conn.handle.generation),
                );
                r.read_exact(&mut prefix[PUBLIC_HEADER..])
                    .await
                    .map_err(read_error)?;
                (
                    frame_head(&prefix, cap).map_err(|_| Status::PROTOCOL)?,
                    MAX_PREFIX,
                )
            }
            Err(_) => return Err(Status::PROTOCOL),
        };
        if matches!(frame.message, ace_net_proto::Message::Response { .. }) {
            return Err(Status::PROTOCOL);
        }
        let mut data = vec![0; frame.frame_len];
        data[..head].copy_from_slice(&prefix[..head]);
        r.read_exact(&mut data[head..]).await.map_err(read_error)?;
        Frame::from_player(&data, frame_limits(cap)).map_err(|_| Status::PROTOCOL)?;
        c.data(key, conn, data)?;
    }
}
async fn write_frames(
    c: &Core,
    conn: Connection,
    w: &mut WriteHalf<'_>,
    io: &IoWake,
) -> Result<(), Status> {
    loop {
        if let Some(b) = c.output(conn)? {
            w.write_all(&b).await.map_err(|_| Status::UNREACHABLE)?;
            c.sent(conn);
        } else {
            io.write.notified().await;
        }
    }
}
async fn connection(c: Arc<Core>, key: u64, conn: Connection, mut socket: TcpStream, io: IoWake) {
    let run = async {
        let hello = async {
            let mut peer = [0; 12];
            socket
                .write_all(&Hello.encode())
                .await
                .map_err(|_| Status::UNREACHABLE)?;
            socket.read_exact(&mut peer).await.map_err(read_error)?;
            Hello::decode(&peer).map_err(|_| Status::PROTOCOL)
        };
        tokio::time::timeout(HELLO_TIMEOUT, hello)
            .await
            .map_err(|_| Status::TIMEOUT)??;
        let (mut r, mut w) = socket.split();
        tokio::select! {r=read_frames(&c,key,conn,&mut r,&io)=>r,r=write_frames(&c,conn,&mut w,&io)=>r}
    };
    let reason =
        tokio::select! {r=run=>r.err().unwrap_or(Status::OK),()=io.close.notified()=>Status::OK};
    drop(socket);
    c.finish_conn(conn, reason);
}
