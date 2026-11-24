//! 业务编解码接口及可选的 ace 适配器。
#![no_std]

/// 编译期格式身份；同一逻辑方法双方选择相同三元组。
pub trait Format {
    /// 固定顺序为 codec_id、codec_rev、options_id。
    const ID: [u32; 3];
}
/// 业务调用者提供的解码限额，不能把自定义 codec 当作内存沙箱。
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// 实际输入或输出 body 字节上限，不是原始 DTO 大小。
    pub wire_bytes: usize,
    /// 解包后的原始消息及累计物化字节各自的上限，不是任意解码器的堆内存沙箱。
    pub decoded_bytes: usize,
    /// 最大嵌套深度。
    pub depth: usize,
    /// 单集合元素个数上限。
    pub items: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            wire_bytes: 64 * 1024,
            decoded_bytes: 1024 * 1024,
            depth: 32,
            items: 4096,
        }
    }
}
/// 统一向 SDK 报告的失败；失败的输出不允许发送。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodecError {
    /// 格式或消息内容无效。
    Invalid,
    /// 固定 ace 实现的原始失败原因；仅存在于选择 ace 的 Rust 调用方。
    #[cfg(feature = "ace")]
    Ace(ace_serialize::error::Error),
    /// 输入、输出、工作区或解码预算不足。
    Limit,
}
impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            #[cfg(feature = "ace")]
            Self::Ace(e) => write!(f, "ace 编解码失败：{e}"),
            Self::Invalid => f.write_str("消息无效"),
            Self::Limit => f.write_str("编解码预算不足"),
        }
    }
}
impl core::error::Error for CodecError {}
/// 共享 DTO 的静态 codec；使用者可以只为所需类型实现，不需要运行时注册。
pub trait Codec<T>: Format {
    /// 编码到有界输出与临时工作区，返回写入长度。
    ///
    /// # Errors
    /// 编码或容量检查失败时整个输出作废。
    fn encode_into(
        value: &T,
        dst: &mut [u8],
        scratch: &mut [u8],
        limits: Limits,
    ) -> Result<usize, CodecError>;
    /// 按同一格式解码。
    ///
    /// # Errors
    /// 内容无效或超预算时不返回部分构造值。
    fn decode(src: &[u8], limits: Limits) -> Result<T, CodecError>;
}
/// 使用固定修订的 ace packed 格式；只对显式提供 ace schema 的类型实现。
#[cfg(feature = "ace")]
pub struct AceCodec;
#[cfg(feature = "ace")]
impl Format for AceCodec {
    const ID: [u32; 3] = [1, 1, 1];
}

#[cfg(feature = "ace")]
fn options(l: Limits) -> ace_serialize::limits::Options {
    use ace_serialize::limits::Options;
    Options {
        packed: true,
        max_input_bytes: l.wire_bytes,
        max_raw_bytes: l.decoded_bytes,
        max_decoded_bytes: l.decoded_bytes,
        max_depth: l.depth,
        max_build_nesting: l.depth,
        max_collection_len: l.items,
        ..Options::default()
    }
}
#[cfg(feature = "ace")]
impl<T: ace_serialize::schema::AceEncode + ace_serialize::schema::AceDecode> Codec<T> for AceCodec {
    fn encode_into(
        value: &T,
        dst: &mut [u8],
        scratch: &mut [u8],
        limits: Limits,
    ) -> Result<usize, CodecError> {
        let cap = dst.len().min(limits.wire_bytes);
        ace_serialize::encode_into(value, &mut dst[..cap], scratch, &options(limits))
            .map_err(CodecError::Ace)
    }
    fn decode(src: &[u8], limits: Limits) -> Result<T, CodecError> {
        if src.len() > limits.wire_bytes {
            return Err(CodecError::Limit);
        }
        ace_serialize::decode(src, &options(limits)).map_err(CodecError::Ace)
    }
}
mod method;
pub use method::{MethodError, NoReply, Schema, TypedMethod};
