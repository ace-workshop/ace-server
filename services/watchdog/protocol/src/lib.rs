//! 准入和会话的逻辑角色契约；默认不链接 SDK 或具体编解码器。
#![no_std]

/// 启动配置声明的逻辑角色名。
pub const ROLE: &str = "watchdog";
/// 角色接口修订；与 codec 修订分别比较。
pub const REVISION: u32 = 1;
/// 逻辑方法键，wire 描述与所选 codec 一起绑定；列在这里不代表服务已就绪。
#[repr(u32)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Method {
    /// Open 的稳定逻辑键。
    Open = 513,
    /// Close 的稳定逻辑键。
    Close = 514,
    /// CheckAdmission 的稳定逻辑键。
    CheckAdmission = 515,
}

extern crate alloc;
use ace_example_protocol::Admission;
use ace_role_gateway::Connection;
use alloc::string::String;

use ace_codec::{Codec, NoReply, TypedMethod};
mod schema;

/// 未绑定连接的两种有序输入；均由 Gate 绑定连接身份，不借用玩家 session 或推送令牌。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Open {
    /// Accepted 建立待准入上下文。
    #[cfg_attr(feature = "codec_ace", ace(tag = 1))]
    Accepted {
        /// Gate 所有的带代次连接。
        #[cfg_attr(feature = "codec_ace", ace(order = 0))]
        conn: Connection,
        /// 由 Gate 观察到的对端端点。
        #[cfg_attr(feature = "codec_ace", ace(order = 1))]
        peer: String,
    },
    /// 后续公共握手，仅推进已有的同代上下文，不能单凭本记录创建连接。
    #[cfg_attr(feature = "codec_ace", ace(tag = 2))]
    Handshake {
        /// 取自实际收到 Data 的连接，不取自握手 body。
        #[cfg_attr(feature = "codec_ace", ace(order = 0))]
        conn: Connection,
        /// 仅含公共业务字段的握手。
        #[cfg_attr(feature = "codec_ace", ace(order = 1))]
        handshake: Admission,
    },
}
/// Gate 通知 Watchdog 指定连接代次已经关闭。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    /// Gate 所有的带代次连接。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub conn: Connection,
}
/// Watchdog 在绑定前向配置的策略服务请求判定。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckAdmission {
    /// Gate 所有的带代次连接。
    #[cfg_attr(feature = "codec_ace", ace(order = 0))]
    pub conn: Connection,
    /// 由 Gate 观察到的对端端点。
    #[cfg_attr(feature = "codec_ace", ace(order = 1))]
    pub peer: String,
    /// 客户端演示准入 DTO。
    #[cfg_attr(feature = "codec_ace", ace(order = 2))]
    pub handshake: Admission,
}
/// 准入结论；不可用不应默认为允许。
#[cfg_attr(feature = "codec_ace", derive(ace_serialize_derive::AceSerialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// 允许绑定到给定实例。
    #[cfg_attr(feature = "codec_ace", ace(tag = 1))]
    Admitted {
        /// 目标服务地址的线上整数，绑定前须验证地址和策略权限。
        #[cfg_attr(feature = "codec_ace", ace(order = 0))]
        target: u64,
    },
    /// 策略拒绝。
    #[cfg_attr(feature = "codec_ace", ace(tag = 2))]
    Denied {
        /// 拒绝原因，由策略解释。
        #[cfg_attr(feature = "codec_ace", ace(order = 0))]
        code: u32,
    },
    /// 策略不可用。
    #[cfg_attr(feature = "codec_ace", ace(tag = 3))]
    Unavailable,
}

/// 以双方选定的 codec 绑定 Open 的键、DTO 和摘要。
pub const fn open<C: Codec<Open>>() -> TypedMethod<C, Open, NoReply> {
    TypedMethod::one_way(Method::Open as u32)
}
/// 以双方选定的 codec 绑定 Close 的键、DTO 和摘要。
pub const fn close<C: Codec<Close>>() -> TypedMethod<C, Close, NoReply> {
    TypedMethod::one_way(Method::Close as u32)
}
/// 以双方选定的 codec 绑定 CheckAdmission 的键、DTO 和摘要。
pub const fn check_admission<C: Codec<CheckAdmission> + Codec<AdmissionDecision>>()
-> TypedMethod<C, CheckAdmission, AdmissionDecision> {
    TypedMethod::call(Method::CheckAdmission as u32, 1024)
}
/// 该角色的完整方法编译期约束，不要求非默认实现提供 ace trait。
pub trait WatchdogCodec:
    Codec<Open> + Codec<Close> + Codec<CheckAdmission> + Codec<AdmissionDecision>
{
}
impl<C: Codec<Open> + Codec<Close> + Codec<CheckAdmission> + Codec<AdmissionDecision>> WatchdogCodec
    for C
{
}
