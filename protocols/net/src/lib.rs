//! 独立于运行时与业务 codec 的公共协议。
#![no_std]

use core::{fmt, num::NonZeroU16};

/// 框架帧修订；与业务 codec 版本分别比较。
pub const FRAME_REV: u32 = 1;

/// 一个服务实例的地址；不是别名、角色、连接 ID 或业务的持久主键。
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServiceAddress(u64);
const _: () = assert!(size_of::<ServiceAddress>() == 8);

/// 节点为零、流水号为零或超出 48 位。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InvalidAddress;
impl fmt::Display for InvalidAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("节点或服务流水号超出地址范围")
    }
}
impl core::error::Error for InvalidAddress {}

impl ServiceAddress {
    /// 节点内最后一个可分配的服务流水号；不能回绕。
    pub const MAX_SEQUENCE: u64 = (1u64 << 48) - 1;
    /// 先验证再组合，避免溢出的低位污染节点编号。
    ///
    /// # Errors
    /// 流水号不在 1..=MAX_SEQUENCE 时拒绝。
    pub const fn new(node: NonZeroU16, sequence: u64) -> Result<Self, InvalidAddress> {
        if sequence == 0 || sequence > Self::MAX_SEQUENCE {
            return Err(InvalidAddress);
        }
        Ok(Self(((node.get() as u64) << 48) | sequence))
    }
    /// 固定线上槽位的整数值，传输时仍须逐字段转小端。
    pub const fn bits(self) -> u64 {
        self.0
    }
    /// 已知非零的节点编号。
    pub const fn node(self) -> NonZeroU16 {
        NonZeroU16::new((self.0 >> 48) as u16).expect("ServiceAddress 构造已验证节点非零")
    }
    /// 已知非零的低 48 位流水号。
    pub const fn sequence(self) -> u64 {
        self.0 & Self::MAX_SEQUENCE
    }
}
impl TryFrom<u64> for ServiceAddress {
    type Error = InvalidAddress;
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        Self::new(
            NonZeroU16::new((v >> 48) as u16).ok_or(InvalidAddress)?,
            v & Self::MAX_SEQUENCE,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn address_rejects_spill_and_reserved_parts() {
        let node = NonZeroU16::MAX;
        let last = ServiceAddress::new(node, ServiceAddress::MAX_SEQUENCE).unwrap();
        assert_eq!(last.bits(), u64::MAX);
        assert_eq!(ServiceAddress::try_from(last.bits()), Ok(last));
        assert_eq!(last.node(), node);
        assert!(ServiceAddress::new(node, 1 << 48).is_err());
        for v in [0, 1, 1 << 48] {
            assert!(ServiceAddress::try_from(v).is_err());
        }
    }
}

pub mod bootstrap;
mod bytes;
mod frame;
pub use frame::{
    Call, Contract, Frame, FrameError, FrameHead, FrameLimits, Message, NodeEntry, NodeFrame,
    Route, RpcError,
};
