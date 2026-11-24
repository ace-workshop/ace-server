use ace_net_proto::bootstrap::KcpEnvelope;
use kcp::{KCP_OVERHEAD, Kcp};
use std::{
    fmt, io,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    time::Duration,
};

const MTU: usize = 1200;
/// 单片数据字节；不含核心段头及框架外层。
pub const MSS: usize = MTU - KCP_OVERHEAD;
/// UDP payload 的上限（非 IP MTU），包括外层和全部核心段。
pub const MAX_UDP_PAYLOAD: usize = KcpEnvelope::DATA_HEADER + MTU;
/// 单个完整 KCP 消息（含应用头）的最大字节数。
pub const MAX_FRAME: usize = 127 * MSS;
/// 使用相对单调时钟的一代会话寿命上限，毫秒。
pub const MAX_AGE_MS: u32 = 86_400_000;
const WINDOW: usize = 128;
const FLIGHT: usize = 2 * WINDOW;
pub(super) const RESERVED_BYTES: usize =
    (FLIGHT + 2 * WINDOW) * MSS + (OUTPUT_PACKETS + 2) * MTU + 3 * (1400 + KCP_OVERHEAD);

const ACK_BUDGET: usize = 96;
const OUTPUT_PACKETS: usize = 32;
const INTERVAL: u32 = 20;
const TRANSMISSIONS: u32 = 12;
const SEQUENCES: u32 = 100_000_000;
const INPUT_SEGMENTS: u32 = 200_000_000;
// RTT 平滑项 7*srtt+rtt；超时重传走 nodelay 的 rto += rto/2，达到 dead_link 的那次也会计算。
const _: () = {
    assert!(8 * (MAX_AGE_MS as u64) <= u32::MAX as u64);
    let mut rto = 60_000;
    let mut xmit = 1;
    while xmit < TRANSMISSIONS {
        rto += rto / 2;
        xmit += 1;
    }
    assert!(MAX_AGE_MS + rto < i32::MAX as u32);
    assert!(MAX_AGE_MS + 120_000 + INTERVAL < i32::MAX as u32);
    assert!(SEQUENCES + (FLIGHT as u32) < i32::MAX as u32);
    assert!((SEQUENCES as u64) * (TRANSMISSIONS as u64) < u32::MAX as u64);
    assert!(MTU / KCP_OVERHEAD <= ACK_BUDGET);
};

/// 一份报文的拒绝与必须终结整个代次的原因分开。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Error {
    /// 入参、形状、字段或消息边界不符；不刷新 idle。
    Invalid,
    /// 单帧、单数据报或配置超出尺寸上限。
    Limit,
    /// 暂时没有容纳本次完整消息的槽和字节。
    Busy,
    /// 会话寿命、idle、序号或累计工作量达到支持域，必须换新会话。
    Expired,
    /// 达到核心重传上限或核心操作失败，必须关闭本代。
    Disconnected,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KCP 传输：{self:?}")
    }
}
impl std::error::Error for Error {}

// 输入预检和输出高水位读取同一发布版 24 字节段头；验证的是封装支持域，不接管协议收发窗。
struct Header {
    conv: u32,
    cmd: u8,
    frg: u8,
    wnd: u16,
    ts: u32,
    sn: u32,
    una: u32,
    len: usize,
}
impl Header {
    fn take(packet: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (h, body) = packet
            .split_first_chunk::<KCP_OVERHEAD>()
            .ok_or(Error::Invalid)?;
        let field = |i| u32::from_le_bytes([h[i], h[i + 1], h[i + 2], h[i + 3]]);
        let len = field(20) as usize;
        let rest = body.get(len..).ok_or(Error::Invalid)?;
        Ok((
            Self {
                conv: field(0),
                cmd: h[4],
                frg: h[5],
                wnd: u16::from_le_bytes([h[6], h[7]]),
                ts: field(8),
                sn: field(12),
                una: field(16),
                len,
            },
            rest,
        ))
    }
}
#[derive(Default)]
struct Counters {
    drops: AtomicU64,
    emitted: AtomicU32,
}
struct Output {
    packets: SyncSender<Box<[u8]>>,
    counters: Arc<Counters>,
}
impl io::Write for Output {
    fn write(&mut self, packet: &[u8]) -> io::Result<usize> {
        if packet.is_empty() || packet.len() > MTU {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let mut rest = packet;
        while !rest.is_empty() {
            let (h, tail) = Header::take(rest).map_err(|_| io::ErrorKind::InvalidData)?;
            rest = tail;
            if h.cmd == 81 {
                self.counters.emitted.fetch_max(h.sn + 1, Ordering::Relaxed);
            }
        }
        if self.packets.try_send(packet.into()).is_err() {
            self.counters.drops.fetch_add(1, Ordering::Relaxed);
        }
        Ok(packet.len()) // 满即本地整包丢失；不能部分写后让核心重复半份报文。
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// 固定配置的消息模式核心。传入的时间都是**本次建会话以来**的单调时长。
/// `Expired` 或 `Disconnected` 后调用者必须丢弃整个对象、撤销外层 token，不回卷原核心。
pub struct Session {
    frame_limit: usize,
    kcp: Kcp<Output>,
    packets: Receiver<Box<[u8]>>,
    counters: Arc<Counters>,
    idle_ms: u32,
    last_input: u32,
    now: u32,
    sent: u32,
    consumed: u32,
    input_segments: u32,
    since_drain: usize,
    // 只核对相邻分片声明，不保存数据、不决定 ACK/重传。rcv_queue 与 rcv_buf 各最多一窗。
    fragments: [Option<(u8, u16)>; FLIGHT],
}
impl Session {
    /// 建立消息模式并在首次 input 之前初始化核心时钟。
    /// # Errors
    /// idle 超出会话寿命或完整帧上限不在 1..=MAX_FRAME 时返回 Limit。
    pub fn new(conv: NonZeroU32, idle_ms: u32, frame_limit: usize) -> Result<Self, Error> {
        if !(1..=MAX_AGE_MS).contains(&idle_ms) || !(1..=MAX_FRAME).contains(&frame_limit) {
            return Err(Error::Limit);
        }
        let (tx, packets) = sync_channel(OUTPUT_PACKETS);
        let counters = Arc::new(Counters::default());
        let mut kcp = Kcp::new(
            conv.get(),
            Output {
                packets: tx,
                counters: counters.clone(),
            },
        );
        kcp.set_mtu(MTU).map_err(|_| Error::Limit)?;
        kcp.set_wndsize(32, WINDOW as u16);
        kcp.set_nodelay(true, INTERVAL as i32, 2, false);
        kcp.set_maximum_resend_times(TRANSMISSIONS);
        kcp.update(0).map_err(|_| Error::Disconnected)?;
        Ok(Self {
            kcp,
            frame_limit,
            packets,
            counters,
            idle_ms,
            last_input: 0,
            now: 0,
            sent: 0,
            consumed: 0,
            input_segments: 0,
            since_drain: 0,
            fragments: [None; FLIGHT],
        })
    }
    fn clock(&mut self, elapsed: Duration) -> Result<u32, Error> {
        let now = u32::try_from(elapsed.as_millis()).map_err(|_| Error::Expired)?;
        if self.consumed >= SEQUENCES
            || self.input_segments >= INPUT_SEGMENTS
            || now < self.now
            || now >= MAX_AGE_MS
            || now - self.last_input >= self.idle_ms
        {
            return Err(Error::Expired);
        }
        self.now = now;
        Ok(now)
    }
    /// 送一份完整的应用记录，不是流式追加，不承诺对端已收到。
    /// # Errors
    /// 空帧、超过 127 片、等候队列满或代次将越界时在 core.send 之前拒绝。
    pub fn send(&mut self, frame: &[u8]) -> Result<(), Error> {
        if frame.is_empty() || frame.len() > self.frame_limit {
            return Err(Error::Limit);
        }
        let n = frame.len().div_ceil(MSS);
        let next = self.sent + n as u32;
        if next > SEQUENCES {
            return Err(Error::Expired);
        }
        // 槽按整个 MSS 预留，保留字节最多 FLIGHT*MSS，包括等待窗口的 snd_queue。
        if self.kcp.wait_snd() + n > FLIGHT {
            return Err(Error::Busy);
        }
        self.kcp.send(frame).map_err(|_| Error::Disconnected)?;
        self.sent = next;
        Ok(())
    }
    /// 从通过外层 peer/token/conv 核验的一份数据报提交全部内层段。全包先验再进核心。
    /// # Errors
    /// 超限、尾随残片、时钟/序号/窗口/片长或相邻分片矛盾时拒绝整包；Invalid/Limit 不延长 idle。
    pub fn input(&mut self, elapsed: Duration, packet: &[u8]) -> Result<(), Error> {
        let now = self.clock(elapsed)?;
        if packet.len() > MTU {
            return Err(Error::Limit);
        }
        if packet.is_empty() {
            return Err(Error::Invalid);
        }
        let mut rest = packet;
        let mut count = 0;
        let mut fragments = self.fragments;
        let mut next = self.consumed;
        // 已经排进 snd_queue 但尚未首次输出的 SN 不能被 ACK 或 UNA 提前确认。
        let emitted = self.counters.emitted.load(Ordering::Relaxed);
        while !rest.is_empty() {
            let (
                Header {
                    conv,
                    cmd,
                    frg,
                    wnd,
                    ts,
                    sn,
                    una,
                    len,
                },
                tail,
            ) = Header::take(rest)?;
            rest = tail;
            if conv != self.kcp.conv()
                || wnd as usize > WINDOW
                || ts >= MAX_AGE_MS
                || una > emitted
                || sn >= SEQUENCES
                || len > MSS
            {
                return Err(Error::Invalid);
            }
            match cmd {
                81 if len > 0 && frg < 127 && (frg == 0 || len == MSS) => {
                    if sn + frg as u32 >= SEQUENCES {
                        return Err(Error::Expired);
                    }
                    if frg as usize * MSS + 1 > self.frame_limit || len > self.frame_limit {
                        return Err(Error::Limit);
                    }
                    if sn >= self.consumed {
                        while next < self.consumed + WINDOW as u32
                            && fragments[next as usize % FLIGHT].is_some()
                        {
                            next += 1;
                        }
                        if sn >= next + WINDOW as u32 {
                            return Err(Error::Invalid);
                        }
                        let k = sn as usize % FLIGHT;
                        if fragments[k].is_some_and(|old| old != (frg, len as u16))
                            || sn > self.consumed
                                && fragments[(k + FLIGHT - 1) % FLIGHT]
                                    .is_some_and(|(prev, _)| prev != 0 && prev != frg + 1)
                            || sn + 1 < self.consumed + FLIGHT as u32
                                && frg != 0
                                && fragments[(k + 1) % FLIGHT]
                                    .is_some_and(|(next, _)| next + 1 != frg)
                        {
                            return Err(Error::Invalid);
                        }
                        // 仅最大可能片数的尾片长度还未确定。首/尾无论哪片先到，都在后到片进入核心前核对。
                        let last = sn + u32::from(frg);
                        let full = (self.frame_limit / MSS) as u32;
                        fragments[k] = Some((frg, len as u16));
                        if last < self.consumed + FLIGHT as u32
                            && let Some(first) = last.checked_sub(full)
                            && first >= self.consumed
                            && fragments[first as usize % FLIGHT]
                                .is_some_and(|(f, _)| u32::from(f) == full)
                            && fragments[last as usize % FLIGHT].is_some_and(|(f, n)| {
                                f == 0 && usize::from(n) > self.frame_limit % MSS
                            })
                        {
                            return Err(Error::Limit);
                        }
                    }
                }
                82 if sn < emitted && ts <= now && frg == 0 && len == 0 => {}
                83 | 84 if frg == 0 && len == 0 => {}
                _ => return Err(Error::Invalid),
            }
            count += 1;
        }
        if self.input_segments + count as u32 > INPUT_SEGMENTS {
            return Err(Error::Expired);
        }
        if self.since_drain + count > ACK_BUDGET {
            self.kcp.flush_ack().map_err(|_| Error::Disconnected)?;
            // flush_ack 会把最多一份 MTU 留在核心 buf。flush 真的交给有界出口（或整包丢弃）才归零。
            self.kcp.flush().map_err(|_| Error::Disconnected)?;
            if self.kcp.is_dead_link() {
                return Err(Error::Disconnected);
            }
            self.since_drain = 0;
        }
        self.kcp.input(packet).map_err(|_| Error::Disconnected)?;
        self.fragments = fragments;
        self.since_drain += count;
        self.input_segments += count as u32;
        self.last_input = now;
        Ok(())
    }
    /// 推进应到期的重传及输出；返回距离下次驱动的时长，不是绝对时间戳。
    /// # Errors
    /// 超时、核心死链或算术域到界必须关闭；update 未必 flush，不能据此归零 ACK 账。
    pub fn update(&mut self, elapsed: Duration) -> Result<Duration, Error> {
        let now = self.clock(elapsed)?;
        self.kcp.update(now).map_err(|_| Error::Disconnected)?;
        if self.kcp.is_dead_link() {
            return Err(Error::Disconnected);
        }
        if self.kcp.check(now) == 0 {
            self.kcp.flush().map_err(|_| Error::Disconnected)?;
        }
        if self.kcp.is_dead_link() {
            return Err(Error::Disconnected);
        }
        let delay = self
            .kcp
            .check(now)
            .min(self.idle_ms - (now - self.last_input))
            .min(MAX_AGE_MS - now);
        Ok(Duration::from_millis(delay.into()))
    }
    /// 不消费消息。调用者先取得足额的应用队列槽及字节，才能调用 recv。
    /// # Errors
    /// 已完整的记录超过单帧上限或核心失败时拒绝。
    pub fn peek(&self) -> Result<Option<usize>, Error> {
        match self.kcp.peeksize() {
            Ok(n) if (1..=self.frame_limit).contains(&n) => Ok(Some(n)),
            Ok(_) => Err(Error::Limit),
            Err(kcp::Error::RecvQueueEmpty | kcp::Error::ExpectingFragment) => Ok(None),
            Err(_) => Err(Error::Disconnected),
        }
    }
    /// 将一份完整消息复制到已预留的应用内存，归还接收窗。
    /// # Errors
    /// 没有完整消息或空间不足时不消费。
    pub fn recv(&mut self, dst: &mut [u8]) -> Result<usize, Error> {
        let n = self.peek()?.ok_or(Error::Busy)?;
        if dst.len() < n {
            return Err(Error::Limit);
        }
        self.kcp.recv(dst).map_err(|_| Error::Disconnected)?;
        let end = self.consumed + n.div_ceil(MSS) as u32;
        for sn in self.consumed..end {
            self.fragments[sn as usize % FLIGHT] = None;
        }
        self.consumed = end;
        Ok(n)
    }
    /// 取一份要加上会话外层的完整核心报文。
    pub fn packet(&self) -> Option<Box<[u8]>> {
        self.packets.try_recv().ok()
    }
    /// 有界同步输出满时按本地整包丢失计数。
    pub fn output_drops(&self) -> u64 {
        self.counters.drops.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_net_proto::{Contract, Frame, FrameLimits, Message, bootstrap::KcpSession};
    use std::num::NonZeroU64;
    fn make() -> Session {
        Session::new(NonZeroU32::MIN, MAX_AGE_MS, MAX_FRAME).unwrap()
    }
    fn ms(n: u32) -> Duration {
        Duration::from_millis(n.into())
    }
    fn record(n: usize, byte: u8) -> Vec<u8> {
        let mut b = vec![0; n];
        let body = vec![byte; n - 104];
        let f = Frame {
            wire: Contract {
                key: 1,
                codec: [1, 1, 1],
                request: [1; 32],
                response: [0; 32],
            },
            message: Message::Send,
            body: &body,
        };
        assert_eq!(
            f.encode_into(
                &mut b,
                FrameLimits {
                    body_bytes: n as u32,
                    response_bytes: 0,
                    timeout_ms: 1
                }
            )
            .unwrap(),
            n
        );
        b
    }
    fn own_push() -> Box<[u8]> {
        let mut a = make();
        a.send(b"A").unwrap();
        a.update(ms(20)).unwrap();
        a.packet().unwrap()
    }
    #[test]
    fn maximum_complete_frame_and_loss_reorder_duplicate_deliver_in_order() {
        let (mut a, mut b) = (make(), make());
        let frames = [record(MAX_FRAME, 3), record(3000, 6), record(110, 9)];
        assert_eq!(a.send(&record(MAX_FRAME + 1, 4)), Err(Error::Limit));
        for f in &frames {
            a.send(f).unwrap();
        }
        assert_eq!(a.send(&frames[0]), Err(Error::Busy));
        let (mut count, mut index) = (0, 0);
        let mut full_packet = false;
        for t in (20..30_000).step_by(20) {
            a.update(ms(t)).unwrap();
            b.update(ms(t)).unwrap();
            let mut batch = Vec::new();
            while let Some(p) = a.packet() {
                count += 1;
                if count % 7 == 0 {
                    continue;
                }
                full_packet |= p.len() == MTU;
                batch.push(p);
            }
            for p in batch.iter().rev() {
                b.input(ms(t), p).unwrap();
                b.input(ms(t), p).unwrap();
            }
            while let Some(p) = b.packet() {
                a.input(ms(t), &p).unwrap();
            }
            while let Some(n) = b.peek().unwrap() {
                let mut data = vec![0; n];
                b.recv(&mut data).unwrap();
                assert_eq!(data, frames[index]);
                index += 1;
            }
        }
        assert_eq!(index, 3);
        assert!(full_packet);
        let id = KcpSession {
            epoch: NonZeroU64::MIN,
            token: NonZeroU64::MIN,
            conv: NonZeroU32::MIN,
        };
        let inner = [0; MTU];
        let data = KcpEnvelope::Data(id, &inner);
        let mut udp = [0; MAX_UDP_PAYLOAD];
        assert_eq!(
            data.encode_into(&mut udp, MAX_UDP_PAYLOAD).unwrap(),
            udp.len()
        );
        assert!(data.encode_into(&mut udp, MAX_UDP_PAYLOAD - 1).is_err());
    }
    #[test]
    fn ack_budget_spans_updates_and_flushes_the_residual_datagram() {
        let p = own_push();
        let mut b = make();
        for i in 0..ACK_BUDGET {
            b.input(ms(1), &p).unwrap();
            if i % 3 == 0 {
                assert_eq!(b.update(ms(1)), Ok(ms(19)));
            }
        }
        assert!(b.packet().is_none());
        assert_eq!(b.since_drain, ACK_BUDGET);
        b.input(ms(1), &p).unwrap();
        assert_eq!(b.since_drain, 1);
        // flush_ack 交出的第一包不足以排空；最后一包只能由 flush 写出。
        let mut drained = 0;
        while let Some(p) = b.packet() {
            drained += p.len() / KCP_OVERHEAD;
        }
        assert_eq!(drained, ACK_BUDGET);
        for _ in 0..ACK_BUDGET * 40 {
            b.input(ms(1), &p).unwrap();
        }
        assert!(b.output_drops() > 0);
        assert_eq!(std::iter::from_fn(|| b.packet()).count(), OUTPUT_PACKETS);
        assert_eq!(b.peek(), Ok(Some(1))); // 重复包未变成新的应用记录。
    }
    #[test]
    fn domain_guards_precede_core_and_only_valid_input_renews_idle() {
        let p = own_push();
        let mut s = Session::new(NonZeroU32::MIN, 100, MAX_FRAME).unwrap();
        for (i, bytes) in [
            (0, &[0, 0, 0, 0][..]),
            (4, &[0]),
            (5, &[127]),
            (6, &[129, 0]),
            (8, &MAX_AGE_MS.to_le_bytes()),
            (12, &SEQUENCES.to_le_bytes()),
            (16, &[1, 0, 0, 0]),
            (20, &[2, 0, 0, 0]),
        ] {
            let mut bad = p.to_vec();
            bad[i..i + bytes.len()].copy_from_slice(bytes);
            let mut batch = p.to_vec();
            batch.extend(bad);
            assert_eq!(s.input(ms(99), &batch), Err(Error::Invalid));
        }
        assert_eq!(s.input(ms(99), &[0; MTU + 1]), Err(Error::Limit));
        assert_eq!(s.peek(), Ok(None));
        assert_eq!(s.since_drain, 0);
        assert_eq!(s.update(ms(100)), Err(Error::Expired));
        let mut sender = make();
        sender.send(b"queued").unwrap();
        let mut peer = make();
        peer.input(ms(20), &p).unwrap();
        peer.update(ms(40)).unwrap();
        let ack = peer.packet().unwrap();
        assert_eq!(sender.input(ms(40), &ack), Err(Error::Invalid)); // 本代只排队，尚无可确认的 SN。
        let mut s = make();
        s.sent = SEQUENCES - 1;
        assert_eq!(s.send(b"A"), Ok(()));
        assert_eq!(s.send(b"B"), Err(Error::Expired));
        let mut s = make();
        s.input_segments = INPUT_SEGMENTS - 1;
        s.input(ms(1), &p).unwrap();
        assert_eq!(s.update(ms(2)), Err(Error::Expired));
        let mut s = make();
        s.consumed = SEQUENCES - 1;
        let mut edge = p.to_vec();
        edge[12..16].copy_from_slice(&(SEQUENCES - 1).to_le_bytes());
        s.input(ms(1), &edge).unwrap();
        // 此处只注入适配器的临界计数；不伪造核心的私有序号。
        s.consumed = SEQUENCES;
        assert_eq!(s.update(ms(2)), Err(Error::Expired));
    }
    #[test]
    fn configured_frame_budget_checks_fragment_pair_in_either_arrival_order() {
        let cap = MSS + 17;
        let mut from = make();
        // 从真实发送核心收集两片（第一轮的拥塞窗为一），不访问依赖的内部状态。
        let data = record(cap + 1, 5);
        from.send(&data).unwrap();
        from.update(ms(20)).unwrap();
        let first = from.packet().unwrap();
        let mut peer = make();
        peer.input(ms(20), &first).unwrap();
        peer.update(ms(40)).unwrap();
        from.input(ms(40), &peer.packet().unwrap()).unwrap();
        from.update(ms(60)).unwrap();
        let tail = from.packet().unwrap();
        for (one, two) in [(&first, &tail), (&tail, &first)] {
            let mut limited = Session::new(NonZeroU32::MIN, 1000, cap).unwrap();
            limited.input(ms(60), one).unwrap();
            assert_eq!(limited.input(ms(60), two), Err(Error::Limit));
            assert_eq!(limited.since_drain, 1);
            assert_eq!(limited.peek(), Ok(None));
        }
        let mut exact = Session::new(NonZeroU32::MIN, 1000, cap + 1).unwrap();
        exact.input(ms(60), &tail).unwrap();
        exact.input(ms(60), &first).unwrap();
        assert_eq!(exact.peek(), Ok(Some(cap + 1)));
        assert_eq!(exact.recv(&mut vec![0; cap]), Err(Error::Limit));
        let mut buf = vec![0; cap + 1];
        exact.recv(&mut buf).unwrap();
        assert_eq!(buf, data);
    }
    #[test]
    fn relative_clock_rtt_retries_probe_and_check_at_supported_edge() {
        let (mut a, mut b) = (make(), make());
        a.send(b"A").unwrap();
        a.update(ms(20)).unwrap();
        b.input(ms(20), &a.packet().unwrap()).unwrap();
        b.update(ms(40)).unwrap();
        let ack = b.packet().unwrap();
        // 产生合法但很迟的 ACK，在最大 rx_rto 上重复更新 estimator；不执行未预检的核心输入。
        let base = MAX_AGE_MS - 20_000_000;
        a.update(ms(base)).unwrap();
        for _ in 0..8 {
            a.input(ms(base), &ack).unwrap();
        }
        while a.packet().is_some() {}
        a.send(b"B").unwrap();
        let mut now = base + 20;
        let mut rto = 60_000;
        for xmit in 1..=TRANSMISSIONS {
            let step = a.update(ms(now));
            assert!(a.packet().is_some(), "xmit={xmit}");
            if xmit == TRANSMISSIONS {
                assert_eq!(step, Err(Error::Disconnected));
                break;
            }
            assert!(step.unwrap() <= ms(INTERVAL));
            if xmit > 1 {
                rto += rto / 2;
            }
            now += rto;
        }
        let mut s = make();
        let mut p = own_push();
        p[6..8].copy_from_slice(&0u16.to_le_bytes());
        s.input(ms(base), &p).unwrap();
        s.update(ms(base)).unwrap();
        while s.packet().is_some() {}
        let mut now = base;
        let mut wait = 7000;
        for _ in 0..12 {
            now += wait;
            s.update(ms(now)).unwrap();
            assert_eq!(s.packet().unwrap()[4], 83);
            wait = (wait + wait / 2).min(120_000);
        }
        s.update(ms(MAX_AGE_MS - 1)).unwrap();
        s.input(ms(MAX_AGE_MS - 1), &p).unwrap();
        assert_eq!(s.update(ms(MAX_AGE_MS - 1)), Ok(ms(1)));
        assert_eq!(s.input(ms(MAX_AGE_MS), &p), Err(Error::Expired));
        assert_eq!(s.update(Duration::MAX), Err(Error::Expired));
    }
}
