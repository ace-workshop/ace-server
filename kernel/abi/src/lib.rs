//! 服务动态库与宿主的调用边界。
#![cfg_attr(not(feature = "native"), no_std)]

/// 启用 native 时才链接稳定布局库。
#[cfg(feature = "native")]
pub mod native;
/// 两张函数表协商的主版本。
pub const ABI_VERSION: u32 = 1;
/// 框架帧修订，独立于业务编解码器。
pub use ace_net_proto::FRAME_REV;

/// C 边界使用整数状态，不将未知状态强转 Rust 枚举。
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "native", derive(abi_stable::StableAbi))]
pub struct Status(pub u32);
impl Status {
    /// 操作成功。
    pub const OK: Self = Self(0);
    /// 当前容量不足。
    pub const BUSY: Self = Self(1);
    /// 契约或输入不符。
    pub const PROTOCOL: Self = Self(2);
    /// 插件回调 panic 或实例故障。
    pub const FAULTED: Self = Self(3);
    /// 能力尚未就绪。
    pub const NOT_READY: Self = Self(4);
    /// 目标已经停止。
    pub const STOPPED: Self = Self(5);
    /// 已登记，等待异步完成。
    pub const PENDING: Self = Self(6);
    /// 回复大于预留容量。
    pub const TOO_SMALL: Self = Self(7);
    /// 目标不存在。
    pub const NOT_FOUND: Self = Self(8);
    /// 权限不足。
    pub const DENIED: Self = Self(9);
    /// 有限截止时间已到。
    pub const TIMEOUT: Self = Self(10);
    /// 目标所在节点暂不可达。
    pub const UNREACHABLE: Self = Self(11);
    /// 目标未声明该方法键。
    pub const UNKNOWN_METHOD: Self = Self(12);
    /// 本次请求的处理发生 panic；与实例已故障不同。
    pub const PANICKED: Self = Self(13);

    /// 已知的失败状态，包括只适用于本机能力检查的错误；Pending 不是失败。
    pub const fn is_failure(self) -> bool {
        self.is_reply_error() || matches!(self, Self::NOT_READY | Self::DENIED)
    }

    /// RPC 可发送的无体终态错误；Pending、NotReady、权限操作结果和未知数值不是业务响应。
    pub const fn is_reply_error(self) -> bool {
        matches!(
            self,
            Self::BUSY
                | Self::TOO_SMALL
                | Self::TIMEOUT
                | Self::STOPPED
                | Self::UNREACHABLE
                | Self::UNKNOWN_METHOD
                | Self::PROTOCOL
                | Self::PANICKED
                | Self::FAULTED
                | Self::NOT_FOUND
        )
    }
}
