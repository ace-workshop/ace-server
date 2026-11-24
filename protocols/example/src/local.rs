//! native 服务侧可选的回声表示，客户端无须链接本模块。
use super::{Echo, EchoReply};
use abi_stable::{StableAbi, std_types::RVec};
use ace_server_sdk::{LocalDto, StableMessage};
macro_rules! local_echo {
    ($dto:ty,$name:ident,$key:expr) => {
        /// 使用稳定字节容器的本地表示；生命周期不依赖创建它的 Actor。
        #[repr(C)]
        #[derive(StableAbi)]
        pub struct $name {
            /// 原始数据，只读访问，拥有容量随对象计费。
            pub data: RVec<u8>,
        }
        // SAFETY: 唯一字段 RVec<u8> 具有完整稳定布局；纯字节无隐藏借用及业务析构。
        unsafe impl StableMessage for $name {
            const KEY: [u8; 32] = *$key;
            fn heap_bytes(&self) -> Option<usize> {
                Some(self.data.capacity())
            }
        }
        impl ace_server_sdk::FromLocal for $dto {
            fn from_stable(v: &$name) -> Self {
                Self {
                    data: v.data.as_slice().into(),
                }
            }
        }
        impl LocalDto for $dto {
            type Stable = $name;
            fn into_stable(self) -> $name {
                $name {
                    data: self.data.into(),
                }
            }
        }
    };
}
local_echo!(Echo, LocalEcho, b"ace.demo.echo.input/local/1.....");
local_echo!(
    EchoReply,
    LocalEchoReply,
    b"ace.demo.echo.reply/local/1....."
);

#[cfg(test)]
mod tests {
    use super::*;
    use ace_server_sdk::LocalError;
    use alloc::vec::Vec;
    #[test]
    fn echo_local_charges_capacity_and_rejects_a_reply_view() {
        let mut data = Vec::with_capacity(128);
        data.push(7);
        let capacity = data.capacity();
        let limit = size_of::<LocalEcho>() + capacity;
        assert!(matches!(
            Echo {
                data: Vec::with_capacity(capacity)
            }
            .into_local(limit - 1),
            Err(LocalError::TooLarge)
        ));
        let owned = Echo { data }.into_local(limit).unwrap();
        assert_eq!(owned.owned_bytes(), limit);
        assert_eq!(owned.view::<LocalEcho>().unwrap().data.as_slice(), &[7]);
        assert!(matches!(
            owned.view::<LocalEchoReply>(),
            Err(LocalError::Incompatible)
        ));
    }
}
