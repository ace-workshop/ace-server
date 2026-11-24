use crate::{Codec, CodecError, Format, Limits};
use ace_net_proto::{Call, Contract, Frame, FrameError, RpcError};
use core::{fmt, marker::PhantomData};

/// DTO 的逻辑 wire 语义。实际表示还要比较独立的 codec 三元组。
pub trait Schema {
    /// 作者提交的规范字节（UTF-8，不隐式追加换行）。
    const CANONICAL: &'static str;
    /// CANONICAL 的 SHA-256；不是本地内存布局身份。
    const DIGEST: [u8; 32];
}
/// 单向方法没有可解码的成功响应。
pub enum NoReply {}
/// 按一个具体编译期 codec 绑定的请求和响应，不使用运行时注册或试解码。
pub struct TypedMethod<C, Q, R> {
    wire: Contract,
    max_response_bytes: u32,
    types: PhantomData<fn(C, Q) -> R>,
}
impl<C, Q, R> TypedMethod<C, Q, R> {
    /// 本方法允许预留的最大成功响应字节数，单向为零。
    pub const fn max_response_bytes(&self) -> u32 {
        self.max_response_bytes
    }
    /// 供公共帧和服务 descriptor 使用同一身份。
    pub const fn wire(&self) -> Contract {
        self.wire
    }
}
impl<C: Format, Q: Schema, R> TypedMethod<C, Q, R> {
    const fn new(key: u32, response: [u8; 32], max_response_bytes: u32) -> Self {
        Self {
            wire: Contract {
                key,
                codec: C::ID,
                request: Q::DIGEST,
                response,
            },
            max_response_bytes,
            types: PhantomData,
        }
    }
}
impl<C: Codec<Q>, Q: Schema> TypedMethod<C, Q, NoReply> {
    /// 绑定单向方法，响应摘要始终为全零。
    pub const fn one_way(key: u32) -> Self {
        Self::new(key, [0; 32], 0)
    }
}
impl<C: Codec<Q> + Codec<R>, Q: Schema, R: Schema> TypedMethod<C, Q, R> {
    /// 只允许能真实处理请求与成功响应两种 DTO 的 codec。
    pub const fn call(key: u32, max_response_bytes: u32) -> Self {
        Self::new(key, R::DIGEST, max_response_bytes)
    }
    /// 仅交出成功编码、经过长度复核的响应切片。
    /// # Errors
    /// codec、目标或工作区不接受时失败，不得发送失败时的缓冲内容。
    pub fn encode_response<'a>(
        &self,
        v: &R,
        dst: &'a mut [u8],
        scratch: &mut [u8],
        limits: Limits,
    ) -> Result<&'a [u8], CodecError> {
        let limits = Limits {
            wire_bytes: limits.wire_bytes.min(self.max_response_bytes as usize),
            ..limits
        };
        encode::<C, R>(v, dst, scratch, limits)
    }
    /// 先核对原请求的关联号、方法、两个摘要与容量，无体错误绝不经过 decoder。
    /// # Errors
    /// 分别报告帧、远端终态与具体 codec 错误。
    pub fn decode_response(
        &self,
        frame: &Frame<'_>,
        call: Call,
        limits: Limits,
    ) -> Result<R, MethodError> {
        if call.max_response_bytes > self.max_response_bytes {
            return Err(MethodError::Frame(FrameError::Limit));
        }
        let body = frame
            .check_response(self.wire, call)
            .map_err(MethodError::Frame)?
            .map_err(MethodError::Reply)?;
        decode::<C, R>(body, limits)
    }
}
impl<C: Codec<Q>, Q, R> TypedMethod<C, Q, R> {
    /// 编码请求或单向输入；不把编码失败后的局部缓冲交给传输。
    /// # Errors
    /// 实际长度超限或 codec 拒绝时失败。
    pub fn encode_input<'a>(
        &self,
        v: &Q,
        dst: &'a mut [u8],
        scratch: &mut [u8],
        limits: Limits,
    ) -> Result<&'a [u8], CodecError> {
        encode::<C, Q>(v, dst, scratch, limits)
    }
    /// 只凭公共或内部输入头核对方法、方向及方法允许的响应预留；无需先取得 body 空间。
    /// # Errors
    /// 未知方法、任一身份不符、响应误作输入或预留越界时失败。
    pub fn check_input(
        &self,
        wire: Contract,
        message: ace_net_proto::Message,
    ) -> Result<(), FrameError> {
        use ace_net_proto::Message;
        wire.check(self.wire)?;
        match message {
            Message::Send if self.wire.response == [0; 32] => Ok(()),
            Message::Request(c) if self.wire.response != [0; 32] => {
                if c.max_response_bytes > self.max_response_bytes {
                    Err(FrameError::Limit)
                } else {
                    Ok(())
                }
            }
            _ => Err(FrameError::Protocol),
        }
    }
    /// 公共帧的入站 typed 边界；可用于已经过 transport 长度检查的帧。
    /// # Errors
    /// 同 `check_input`；输入超限或 codec 失败也不返回部分 DTO。
    pub fn decode_frame_input(&self, f: &Frame<'_>, limits: Limits) -> Result<Q, MethodError> {
        self.check_input(f.wire, f.message)
            .map_err(MethodError::Frame)?;
        decode::<C, Q>(f.body, limits)
    }
    /// 处理已分帧且已确定是输入方向的 body；也适用于受信宿主交付的 Bytes。
    /// # Errors
    /// 方法、codec、任一摘要或长度不符时在业务解码前失败。
    pub fn decode_input(
        &self,
        wire: Contract,
        body: &[u8],
        limits: Limits,
    ) -> Result<Q, MethodError> {
        wire.check(self.wire).map_err(MethodError::Frame)?;
        decode::<C, Q>(body, limits)
    }
}
fn encode<'a, C: Codec<T>, T>(
    v: &T,
    dst: &'a mut [u8],
    scratch: &mut [u8],
    limits: Limits,
) -> Result<&'a [u8], CodecError> {
    let cap = dst.len().min(limits.wire_bytes);
    let dst = &mut dst[..cap];
    let n = C::encode_into(v, dst, scratch, limits)?;
    dst.get(..n).ok_or(CodecError::Limit)
}
fn decode<C: Codec<T>, T>(bytes: &[u8], limits: Limits) -> Result<T, MethodError> {
    if bytes.len() > limits.wire_bytes {
        return Err(MethodError::Codec(CodecError::Limit));
    }
    C::decode(bytes, limits).map_err(MethodError::Codec)
}
/// 方法级错误只在当前 Rust 模块使用，不让第三方错误对象跨 ABI。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MethodError {
    /// 帧与预期调用不匹配。
    Frame(FrameError),
    /// 业务 body 编解码或预算失败。
    Codec(CodecError),
    /// 匹配的远端无体错误终态。
    Reply(RpcError),
}
impl fmt::Display for MethodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frame(e) => e.fmt(f),
            Self::Codec(e) => e.fmt(f),
            Self::Reply(e) => write!(f, "远端终态：{e:?}"),
        }
    }
}
impl core::error::Error for MethodError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Frame(e) => Some(e),
            Self::Codec(e) => Some(e),
            Self::Reply(_) => None,
        }
    }
}
