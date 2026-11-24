//! 玩家连接路由的逻辑角色契约；默认不链接 SDK 或具体编解码器。
#![no_std]

/// 启动配置声明的逻辑角色名。
pub const ROLE: &str = "gateway";
/// 角色接口修订；与 codec 修订分别比较。
pub const REVISION: u32 = 1;
/// 逻辑方法键，wire 描述与所选 codec 一起绑定；列在这里不代表服务已就绪。
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Method {
    /// Bind 的稳定逻辑键。
    Bind = 257,
    /// Kick 的稳定逻辑键。
    Kick = 258,
    /// SendToSession 的稳定逻辑键。
    SendToSession = 259,
    /// Online 的稳定逻辑键。
    Online = 260,
}

extern crate alloc;
use alloc::vec::Vec;

use ace_codec::{Codec, TypedMethod};
#[cfg(feature = "local")]
mod local;
mod schema;

/// 连接所属的传输。两种表示不共享资源代次。
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "local", derive(abi_stable::StableAbi))]
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
pub enum Transport {
    /// TCP 连接。
    Tcp = 1,
    /// KCP 会话。
    Kcp = 2,
}
/// 服务间使用的连接身份，不出现在公共业务头。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
#[cfg_attr(feature = "local", derive(abi_stable::StableAbi))]
pub struct Connection {
    /// 实际传输。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub transport: Transport,
    /// 宿主资源号。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub id: u64,
    /// 该资源的代次。
    #[cfg_attr(feature = "codec_ace", ace(order = 2))]
    pub generation: u64,
}
/// 将尚待准入的连接绑定到目标实例。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
#[cfg_attr(feature = "local", derive(abi_stable::StableAbi))]
pub struct Bind {
    /// Gate 所有的带代次连接。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub conn: Connection,
    /// 目标地址的线上整数；使用前须验证地址和调用权限，不是玩家 ID。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub target: u64,
}
/// 关闭指定代次的连接。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
#[cfg_attr(feature = "local", derive(abi_stable::StableAbi))]
pub struct Kick {
    /// Gate 所有的带代次连接。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub conn: Connection,
}
/// 仅供绑定目标推送；外层使用角色 codec，内层是未经转码的完整公共单向帧。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendToSession {
    /// 源 Gate 签发的推送令牌，不是 RPC session。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub push_session: u64,
    /// 保留业务 codec/契约的完整公共单向帧。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub frame: Vec<u8>,
}
/// 查询该 Gate 的在线数。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Online {}
/// Gate 报告的在线数。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnlineCount {
    /// 当前在线连接数。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub count: u32,
}
/// 角色操作成功，不携带额外数据；失败走无体框架错误。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
#[cfg_attr(feature = "local", derive(abi_stable::StableAbi))]
pub struct Ack {}

/// 以双方选定的 codec 绑定 Bind 的键、DTO 和摘要。
pub const fn bind<C: Codec<Bind> + Codec<Ack>>() -> TypedMethod<C, Bind, Ack> {
    TypedMethod::call(Method::Bind as u32, 1024)
}
/// 以双方选定的 codec 绑定 Kick 的键、DTO 和摘要。
pub const fn kick<C: Codec<Kick> + Codec<Ack>>() -> TypedMethod<C, Kick, Ack> {
    TypedMethod::call(Method::Kick as u32, 1024)
}
/// 以双方选定的 codec 绑定 SendToSession 的键、DTO 和摘要。
pub const fn send_to_session<C: Codec<SendToSession> + Codec<Ack>>()
-> TypedMethod<C, SendToSession, Ack> {
    TypedMethod::call(Method::SendToSession as u32, 1024)
}
/// 以双方选定的 codec 绑定 Online 的键、DTO 和摘要。
pub const fn online<C: Codec<Online> + Codec<OnlineCount>>() -> TypedMethod<C, Online, OnlineCount>
{
    TypedMethod::call(Method::Online as u32, 1024)
}
/// 该角色的完整方法编译期约束，不要求非默认实现提供 ace trait。
pub trait GatewayCodec:
    Codec<Bind> + Codec<Ack> + Codec<Kick> + Codec<SendToSession> + Codec<Online> + Codec<OnlineCount>
{
}
impl<
    C: Codec<Bind>
        + Codec<Ack>
        + Codec<Kick>
        + Codec<SendToSession>
        + Codec<Online>
        + Codec<OnlineCount>,
> GatewayCodec for C
{
}
