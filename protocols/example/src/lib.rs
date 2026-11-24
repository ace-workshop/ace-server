//! 测试客户端和示例服务的 DTO；不依赖角色契约或动态库 ABI。
#![no_std]
extern crate alloc;
use alloc::{string::String, vec::Vec};
/// 示例方法键；与测试夹具及三个服务角色的键域不同。
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Method {
    /// 演示准入单向握手。
    Admission = 0x401,
    /// 请求回声。
    Echo = 0x402,
}

use ace_codec::{Codec, NoReply, TypedMethod};
#[cfg(feature = "local")]
pub mod local;
mod schema;

/// 演示准入握手；令牌不是认证或可用于内部路由的来源。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    /// 演示令牌。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub token: String,
}
/// 客户端可请求的回声输入。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Echo {
    /// 保序的原始字节。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub data: Vec<u8>,
}
/// 回显原始数据的成功响应。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoReply {
    /// 保序的原始字节。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub data: Vec<u8>,
}

/// 以双方选定的 codec 绑定 Admission 的键、DTO 和摘要。
pub const fn admission<C: Codec<Admission>>() -> TypedMethod<C, Admission, NoReply> {
    TypedMethod::one_way(Method::Admission as u32)
}
/// 以双方选定的 codec 绑定 Echo 的键、DTO 和摘要。
pub const fn echo<C: Codec<Echo> + Codec<EchoReply>>() -> TypedMethod<C, Echo, EchoReply> {
    TypedMethod::call(Method::Echo as u32, 64 * 1024)
}

#[cfg(all(test, feature = "codec_ace"))]
mod tests {
    use super::*;
    use ace_codec::{AceCodec, Limits, MethodError};
    #[test]
    fn packed_echo_checks_destination_scratch_and_decode_limits() {
        let m = echo::<AceCodec>();
        let l = Limits::default();
        let q = Echo {
            data: b"ACE".into(),
        };
        let mut dst = [0; 128];
        let mut scratch = [0; 2048];
        assert!(m.encode_input(&q, &mut dst, &mut [], l).is_err());
        assert!(m.encode_input(&q, &mut dst[..1], &mut scratch, l).is_err());
        assert!(
            m.encode_input(&q, &mut dst, &mut scratch, Limits { wire_bytes: 1, ..l })
                .is_err()
        );
        let b = m.encode_input(&q, &mut dst, &mut scratch, l).unwrap();
        assert_eq!(m.decode_input(m.wire(), b, l), Ok(q));
        for small in [
            Limits { items: 2, ..l },
            Limits {
                wire_bytes: b.len() - 1,
                ..l
            },
            Limits {
                decoded_bytes: 16,
                ..l
            },
            Limits { depth: 0, ..l },
        ] {
            assert!(matches!(
                m.decode_input(m.wire(), b, small),
                Err(MethodError::Codec(_))
            ));
        }
        assert!(matches!(
            m.decode_input(m.wire(), &b[..b.len() - 1], l),
            Err(MethodError::Codec(_))
        ));
    }
}
