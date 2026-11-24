//! 仅供跨库可行性回归使用；测试驱动只依赖这里，不链接示例 DTO 或编解码器。
use abi_stable::{
    StableAbi,
    library::RootModule,
    sabi_types::VersionStrings,
    std_types::{RBox, ROption, RStr, RVec},
};
pub use ace_server_abi::native::{
    Method as Shape, PeerAccess, ReplyMode, Representation as Path, RouteScope, WireContract,
};
use ace_server_local::LocalOwned;

/// 测试驱动持有的独占信封，字节是已经取得所有权的传输副本。
#[repr(u8)]
#[derive(StableAbi)]
pub enum Packet {
    /// 只转移，不做编解码。
    Local(LocalOwned),
    /// 已编码的完整消息。
    Bytes(RVec<u8>),
}
/// 对单次接纳的故障注入，仅出现在测试动态库。
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, StableAbi)]
pub enum Admission {
    /// 正常接纳。
    Accept,
    /// 取走前拒绝。
    Busy,
    /// 取走后在插件内 panic。
    PanicAfterTake,
}
/// 在创建服务的库内 poll；Rust 的 Future、Waker 和 Context 不越过边界。
#[repr(u8)]
#[derive(StableAbi)]
pub enum Step {
    /// 此轮真实 Future 返回 Pending。
    Pending,
    /// 返回由消费的数据计算的值，以及可选的真实回复。
    Done(u64, ROption<Packet>),
    /// 输入、预算或状态不允许继续。
    Failed,
}
/// 只用于观测每个独立库的实际调用次数。
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, StableAbi)]
pub struct Counts {
    /// encode_into 调用数。
    pub encode: usize,
    /// decode 调用数。
    pub decode: usize,
    /// 本库产生并析构的稳定 DTO 数。
    pub released: usize,
    /// 本库已销毁的独立实例数。
    pub destroyed: usize,
}
// 仅隔离这个上游生成的 vtable，其他代码保持原 lint。
#[allow(
    non_local_definitions,
    reason = "abi_stable 0.11.3 的 trait 宏在具名常量内生成 impl"
)]
mod endpoint {
    use super::*;
    /// 固定夹具动作；对象保持独占，移交另一线程后才能继续调用。
    #[abi_stable::sabi_trait]
    pub trait Endpoint: Send {
        /// 构造有标量、字符串和数组的请求。
        fn request(&mut self, path: Path, seed: u64) -> ROption<Packet>;
        /// slot 为空就是唯一所有权已转移；非零结果不能代替此标志。
        fn admit(
            &mut self,
            slot: &mut ROption<Packet>,
            reply: ROption<Path>,
            fault: Admission,
        ) -> u32;
        /// 每次至多 poll 一个片段。
        #[sabi(last_prefix_field)]
        fn poll_one(&mut self) -> Step;
    }
}
pub use endpoint::{Endpoint, Endpoint_TO};
/// 夹具实例由上游稳定 vtable 将析构路由回创建库，不做跨模块 downcast。
pub type EndpointBox = Endpoint_TO<'static, RBox<()>>;

/// 夹具根不是公共服务 ABI；使用上游布局检查验证整张函数表。
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = FixtureRef)))]
#[sabi(missing_field(panic))]
pub struct Fixture {
    /// 库名用于断言确实装载了两个不同产物。
    pub name: RStr<'static>,
    /// 每次创建新的独占实例，而非库级单例。
    pub create: extern "C" fn() -> EndpointBox,
    /// drift=true 时只替换响应的本地表示，wire 不变。
    pub shape: extern "C" fn(bool) -> Shape,
    /// 查询库内计数。
    #[sabi(last_prefix_field)]
    pub counts: extern "C" fn() -> Counts,
}
impl RootModule for FixtureRef {
    abi_stable::declare_root_module_statics! {FixtureRef}
    const BASE_NAME: &'static str = "ace_fixture";
    const NAME: &'static str = "ace_fixture";
    const VERSION_STRINGS: VersionStrings = abi_stable::package_version_strings!();
}
