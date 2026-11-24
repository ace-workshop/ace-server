//! 节点成员与远端发现的逻辑角色契约；默认不链接 SDK 或具体编解码器。
#![no_std]

/// 启动配置声明的逻辑角色名。
pub const ROLE: &str = "cluster";
/// 角色接口修订；与 codec 修订分别比较。
pub const REVISION: u32 = 1;
/// 逻辑方法键，wire 描述与所选 codec 一起绑定；列在这里不代表服务已就绪。
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Method {
    /// Join 的稳定逻辑键。
    Join = 769,
    /// JoinResult 的稳定逻辑键。
    JoinResult = 770,
    /// Members 的稳定逻辑键。
    Members = 771,
    /// Leave 的稳定逻辑键。
    Leave = 772,
    /// Heartbeat 的稳定逻辑键。
    Heartbeat = 773,
    /// Resolve 的稳定逻辑键。
    Resolve = 774,
}

extern crate alloc;
use alloc::{string::String, vec::Vec};

use ace_codec::{Codec, NoReply, TypedMethod};
mod schema;

/// 受管成员声明，不等于路由已经就绪。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// 受管节点号。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub node: u16,
    /// 节点本次启动身份。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub boot: u64,
    /// 通告的节点端点。
    #[cfg_attr(feature = "codec_ace", ace(order = 2))]
    pub endpoint: String,
    /// 该节点负责远端 alias 查询的控制服务地址。
    #[cfg_attr(feature = "codec_ace", ace(order = 3))]
    pub cluster: u64,
}
/// 发送完整的入会声明。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    /// 请求加入的节点。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub member: Member,
}
/// 可整体替换的完整目录，不截断成员。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    /// 目录所属 seed 启动身份。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub seed_boot: u64,
    /// 该 seed 启动期的目录版本。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub version: u64,
    /// 完整成员集合。
    #[cfg_attr(feature = "codec_ace", ace(order = 2))]
    pub members: Vec<Member>,
}
/// 在候选连接上回复加入结果，不需要业务路由或 RPC session。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinResult {
    /// 成功时返回完整快照。
    #[cfg_attr(feature = "codec_ace", ace(tag = 1))]
    Joined {
        /// 获准的目录快照。
        #[cfg_attr(feature = "codec_ace", ace(order = 0))]
        snapshot: Members,
    },
    /// 拒绝加入。
    #[cfg_attr(feature = "codec_ace", ace(tag = 2))]
    Rejected {
        /// 拒绝原因，由策略解释。
        #[cfg_attr(feature = "codec_ace", ace(order = 0))]
        code: u32,
    },
}
/// 退出指定启动身份。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leave {
    /// 受管节点号。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub node: u16,
    /// 节点本次启动身份。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub boot: u64,
}
/// 报告已持有的目录版本。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    /// 目录所属 seed 启动身份。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub seed_boot: u64,
    /// 该 seed 启动期的目录版本。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub version: u64,
}
/// 查询明确节点上的一个服务别名。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolve {
    /// 受管节点号。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub node: u16,
    /// 待解析的节点内别名。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub alias: String,
}
/// 一次解析得到的服务实例（失败走无体返回）。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// 实际服务实例地址。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub address: u64,
}

/// 以双方选定的 codec 绑定 Join 的键、DTO 和摘要。
pub const fn join<C: Codec<Join>>() -> TypedMethod<C, Join, NoReply> {
    TypedMethod::one_way(Method::Join as u32)
}
/// 以双方选定的 codec 绑定 JoinResult 的键、DTO 和摘要。
pub const fn join_result<C: Codec<JoinResult>>() -> TypedMethod<C, JoinResult, NoReply> {
    TypedMethod::one_way(Method::JoinResult as u32)
}
/// 以双方选定的 codec 绑定 Members 的键、DTO 和摘要。
pub const fn members<C: Codec<Members>>() -> TypedMethod<C, Members, NoReply> {
    TypedMethod::one_way(Method::Members as u32)
}
/// 以双方选定的 codec 绑定 Leave 的键、DTO 和摘要。
pub const fn leave<C: Codec<Leave>>() -> TypedMethod<C, Leave, NoReply> {
    TypedMethod::one_way(Method::Leave as u32)
}
/// 以双方选定的 codec 绑定 Heartbeat 的键、DTO 和摘要。
pub const fn heartbeat<C: Codec<Heartbeat>>() -> TypedMethod<C, Heartbeat, NoReply> {
    TypedMethod::one_way(Method::Heartbeat as u32)
}
/// 以双方选定的 codec 绑定 Resolve 的键、DTO 和摘要。
pub const fn resolve<C: Codec<Resolve> + Codec<Resolved>>() -> TypedMethod<C, Resolve, Resolved> {
    TypedMethod::call(Method::Resolve as u32, 1024)
}
/// 该角色的完整方法编译期约束，不要求非默认实现提供 ace trait。
pub trait ClusterCodec:
    Codec<Join>
    + Codec<JoinResult>
    + Codec<Members>
    + Codec<Leave>
    + Codec<Heartbeat>
    + Codec<Resolve>
    + Codec<Resolved>
{
}
impl<
    C: Codec<Join>
        + Codec<JoinResult>
        + Codec<Members>
        + Codec<Leave>
        + Codec<Heartbeat>
        + Codec<Resolve>
        + Codec<Resolved>,
> ClusterCodec for C
{
}
