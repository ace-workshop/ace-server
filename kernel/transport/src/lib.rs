//! 原生传输两端共享的有限 KCP 消息适配。没有 socket、插件 ABI 或业务 codec。
mod session;
pub use session::{Error, MAX_AGE_MS, MAX_FRAME, MAX_UDP_PAYLOAD, MSS, Session};

use ace_net_proto::{Frame, FrameError, FrameHead, FrameLimits};
use std::num::NonZeroU64;

/// TCP 先读这一段固定公共头；请求才另读 12 字节扩展。没有额外的 TCP 长度字。
pub const PUBLIC_HEADER: usize = 104;
/// 固定公共头和最大请求扩展合计的前缀容量。
pub const MAX_PREFIX: usize = PUBLIC_HEADER + 12;
/// 在有限工作区预读头；仅在合法请求还缺扩展时要求补读，不按未验证长度申请 body。
/// # Errors
/// 帧字段、作用域或含头的总长超限时失败。
pub fn frame_head(prefix: &[u8], cap: usize) -> Result<FrameHead, FrameError> {
    let h = Frame::inspect(prefix, frame_limits(cap))?;
    if h.frame_len > cap {
        return Err(FrameError::Limit);
    }
    Ok(h)
}
/// 两端公共传输头使用的预检限额；方法与该次调用的更小预算由接收方另验。
pub fn frame_limits(cap: usize) -> FrameLimits {
    let bytes = u32::try_from(cap).unwrap_or(u32::MAX);
    FrameLimits {
        body_bytes: bytes,
        response_bytes: bytes,
        timeout_ms: MAX_AGE_MS.into(),
    }
}
/// 用系统随机源签发非零 nonce 或 token；不承诺产品身份认证。
/// # Errors
/// 随机源失败或取到保留零值时拒绝本次握手，不循环等待熵。
pub fn random_nonce() -> Result<NonZeroU64, Error> {
    getrandom::u64()
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(Error::Disconnected)
}
/// 核心收发窗口、有限报文出口及输出暂存的保守负载字节预留，不含另限条数的固定元数据。
pub const SESSION_BUFFER_BYTES: usize = session::RESERVED_BYTES;
