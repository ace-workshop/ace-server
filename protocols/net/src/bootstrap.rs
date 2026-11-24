//! 固定引导记录。协议握手、节点候选与 KCP 外层分别解析，均不调用业务 codec。
use crate::{
    FrameError, ServiceAddress,
    bytes::{Reader, Writer},
};
use core::num::{NonZeroU16, NonZeroU32, NonZeroU64};

const HELLO: u16 = 0x10;
const PEER: u16 = 0x110;
// 首版要求逐方法双摘要。该位不是连接全局 codec，也不授予业务准入。
const METHOD_CONTRACTS: u32 = 1;
/// 当前修订的公共能力握手。两端各发相同的固定记录，不携带业务准入 DTO。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hello;
impl Hello {
    /// 产生当前修订与必需能力的固定 12 字节。
    pub fn encode(self) -> [u8; 12] {
        let mut b = [0; 12];
        b[..4].copy_from_slice(&crate::FRAME_REV.to_le_bytes());
        b[6..8].copy_from_slice(&HELLO.to_le_bytes());
        b[8..].copy_from_slice(&METHOD_CONTRACTS.to_le_bytes());
        b
    }
    /// 接受完全匹配的框架版本与能力，不比较构建 SHA 或 CPU。
    /// # Errors
    /// 记录不完整或含未知能力、标志、类型时拒绝。
    pub fn decode(src: &[u8]) -> Result<Self, FrameError> {
        let mut r = Reader(src);
        if r.prefix()? != (0, HELLO) || r.u32()? != METHOD_CONTRACTS {
            return Err(FrameError::Protocol);
        }
        r.end()?;
        Ok(Self)
    }
}
/// 连接两端交换的节点候选身份；本机观测的地址和资源代次不取自此记录。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerHello {
    /// 发送方声明的节点。
    pub node: NonZeroU16,
    /// 本次预留号段的起点。
    pub boot: NonZeroU64,
    /// 发起连接的一端。
    pub dialer: NonZeroU16,
    /// 发起端启动身份。
    pub dialer_boot: NonZeroU64,
    /// 同一发起端启动期内的连接尝试号。
    pub attempt: NonZeroU64,
}
impl PeerHello {
    fn validate(&self) -> Result<(), FrameError> {
        if self.boot.get() > ServiceAddress::MAX_SEQUENCE
            || self.dialer_boot.get() > ServiceAddress::MAX_SEQUENCE
            || (self.node == self.dialer && self.boot != self.dialer_boot)
        {
            Err(FrameError::Protocol)
        } else {
            Ok(())
        }
    }
    /// 编码节点握手，不代表已经安装路由。
    /// # Errors
    /// 无效启动身份或同一节点自相矛盾时失败。
    pub fn encode(&self) -> Result<[u8; 36], FrameError> {
        self.validate()?;
        let mut b = [0; 36];
        let mut w = Writer(&mut b);
        w.prefix(0, PEER)?;
        w.put(&self.node.get().to_le_bytes())?;
        w.put(&self.boot.get().to_le_bytes())?;
        w.put(&self.dialer.get().to_le_bytes())?;
        w.put(&self.dialer_boot.get().to_le_bytes())?;
        w.put(&self.attempt.get().to_le_bytes())?;
        Ok(b)
    }
    /// 解码候选；部署身份、对端节点、实际发起端与启动高水位由宿主另外核验。
    /// # Errors
    /// 错域、无效字段或长度不符时失败。
    pub fn decode(src: &[u8]) -> Result<Self, FrameError> {
        let mut r = Reader(src);
        if r.prefix()? != (0, PEER) {
            return Err(FrameError::Protocol);
        }
        let v = Self {
            node: NonZeroU16::new(r.u16()?).ok_or(FrameError::Protocol)?,
            boot: nz(r.u64()?)?,
            dialer: NonZeroU16::new(r.u16()?).ok_or(FrameError::Protocol)?,
            dialer_boot: nz(r.u64()?)?,
            attempt: nz(r.u64()?)?,
        };
        r.end()?;
        v.validate()?;
        Ok(v)
    }
}
/// 节点在当前已准入连接上通告本端普通业务已就绪，不带服务地址或 codec。
/// 接收方仍须在自己的启动条件和对端就绪条件都满足后激活这一代路由。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerReady;
impl PeerReady {
    /// 编码只在本连接代次内有效的就绪信号。
    pub fn encode(self) -> [u8; 8] {
        let mut b = [0; 8];
        b[..4].copy_from_slice(&crate::FRAME_REV.to_le_bytes());
        b[6..].copy_from_slice(&0x111u16.to_le_bytes());
        b
    }
    /// 核对固定记录，不从成功解析推断对方已获准入。
    /// # Errors
    /// 版本、类型、标志或精确长度不匹配时失败。
    pub fn decode(src: &[u8]) -> Result<Self, FrameError> {
        let mut r = Reader(src);
        if r.prefix()? != (0, 0x111) {
            return Err(FrameError::Protocol);
        }
        r.end()?;
        Ok(Self)
    }
}
fn nz(n: u64) -> Result<NonZeroU64, FrameError> {
    NonZeroU64::new(n).ok_or(FrameError::Protocol)
}
/// 服务器签发的 KCP 会话标识；不代替核对实际 peer，不提供加密认证。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KcpSession {
    /// 服务器启动域。
    pub epoch: NonZeroU64,
    /// 当前会话令牌。
    pub token: NonZeroU64,
    /// KCP 核心会话号。
    pub conv: NonZeroU32,
}
/// 一份 UDP payload 的框架外层；Data 内是 KCP 报文，不是应用帧。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KcpEnvelope<'a> {
    /// 客户端请求分配候选。
    Hello(NonZeroU64),
    /// 服务器回显 nonce 并签发候选。
    Offer(NonZeroU64, KcpSession),
    /// 客户端确认候选。
    Confirm(NonZeroU64, KcpSession),
    /// 服务器确认已建立会话。
    Ready(NonZeroU64, KcpSession),
    /// 已建立会话的一份 KCP 报文。
    Data(KcpSession, &'a [u8]),
}
impl<'a> KcpEnvelope<'a> {
    /// 每个 KCP 报文另计的头长，不属于 KCP_MTU。
    pub const DATA_HEADER: usize = 28;
    /// 在 UDP payload 总预算和目标切片内编码，不调用 KCP。
    /// # Errors
    /// 空数据报文、溢出或空间不够时拒绝。
    pub fn encode_into(&self, dst: &mut [u8], max_udp_payload: usize) -> Result<usize, FrameError> {
        let (kind, nonce, session, body) = match *self {
            Self::Hello(n) => (0x301, Some(n), None, &[][..]),
            Self::Offer(n, s) => (0x302, Some(n), Some(s), &[][..]),
            Self::Confirm(n, s) => (0x303, Some(n), Some(s), &[][..]),
            Self::Ready(n, s) => (0x304, Some(n), Some(s), &[][..]),
            Self::Data(_, []) => return Err(FrameError::Protocol),
            Self::Data(s, b) => (0x305, None, Some(s), b),
        };
        let len = (8 + 8 * usize::from(nonce.is_some()) + 20 * usize::from(session.is_some()))
            .checked_add(body.len())
            .filter(|&n| n <= max_udp_payload)
            .ok_or(FrameError::Limit)?;
        let mut w = Writer(dst.get_mut(..len).ok_or(FrameError::Limit)?);
        w.prefix(0, kind)?;
        if let Some(n) = nonce {
            w.put(&n.get().to_le_bytes())?;
        }
        if let Some(s) = session {
            w.put(&s.epoch.get().to_le_bytes())?;
            w.put(&s.token.get().to_le_bytes())?;
            w.put(&s.conv.get().to_le_bytes())?;
        }
        w.put(body)?;
        Ok(len)
    }
    /// 先检查 UDP 总长再读取外层字段；不接纳会话也不验证内层 segment。
    /// # Errors
    /// 未知字段、令牌为零、超限或握手尾随数据时失败。
    pub fn decode(src: &'a [u8], max_udp_payload: usize) -> Result<Self, FrameError> {
        if src.len() > max_udp_payload {
            return Err(FrameError::Limit);
        }
        let mut r = Reader(src);
        let (0, k @ 0x301..=0x305) = r.prefix()? else {
            return Err(FrameError::Protocol);
        };
        let nonce = if k != 0x305 {
            Some(nz(r.u64()?)?)
        } else {
            None
        };
        if let (0x301, Some(n)) = (k, nonce) {
            r.end()?;
            return Ok(Self::Hello(n));
        }
        let s = KcpSession {
            epoch: nz(r.u64()?)?,
            token: nz(r.u64()?)?,
            conv: NonZeroU32::new(r.u32()?).ok_or(FrameError::Protocol)?,
        };
        if k == 0x305 && !r.0.is_empty() {
            return Ok(Self::Data(s, r.0));
        }
        r.end()?;
        match (k, nonce) {
            (0x302, Some(n)) => Ok(Self::Offer(n, s)),
            (0x303, Some(n)) => Ok(Self::Confirm(n, s)),
            (0x304, Some(n)) => Ok(Self::Ready(n, s)),
            _ => Err(FrameError::Protocol),
        }
    }
}
