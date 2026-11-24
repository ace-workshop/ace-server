//! 无堆的控制消息可直接移作本地稳定表示；不适用于普通 Rust 容器。
use super::{Ack, Bind, Kick};
use ace_server_sdk::{LocalDto, StableMessage};
macro_rules! inline_message {
    ($name:ty,$key:expr) => {
        // SAFETY: 完整 repr(C) 布局仅有稳定枚举和定宽整数（或无字段），无借用/堆/业务析构。
        unsafe impl StableMessage for $name {
            const KEY: [u8; 32] = *$key;
            fn heap_bytes(&self) -> Option<usize> {
                Some(0)
            }
        }
        impl ace_server_sdk::FromLocal for $name {
            fn from_stable(v: &Self) -> Self {
                v.clone()
            }
        }
        impl LocalDto for $name {
            type Stable = Self;
            fn into_stable(self) -> Self {
                self
            }
        }
    };
}
inline_message!(Bind, b"ace.gateway.bind.local/1........");
inline_message!(Kick, b"ace.gateway.kick.local/1........");
inline_message!(Ack, b"ace.gateway.ack.local/1.........");
