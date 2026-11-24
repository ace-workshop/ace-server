//! 受信的 native 服务调用边界；纯帧／客户端无需启用 `native`。
//! 本机表用 abi_stable 的根布局检查，线上从不复制这些结构的内存。
use crate::{ABI_VERSION, FRAME_REV, Status};
use abi_stable::{
    StableAbi,
    library::RootModule,
    sabi_types::VersionStrings,
    std_types::{ROption, RSlice, RStr},
};
use ace_net_proto::ServiceAddress;
use ace_server_local::{LocalOwned, LocalType};
use core::num::{NonZeroU32, NonZeroU64};

/// 本次调用中唯一可移动的本地对象槽。空槽表示未提供或已经取走。
pub type LocalSlot = ROption<LocalOwned>;

/// 独立于内存布局的线上方法身份。
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, StableAbi)]
pub struct WireContract {
    /// 共享协议分配的方法键。
    pub key: u32,
    /// codec 标识、修订和选项。
    pub codec: [u32; 3],
    /// 请求规范描述的 SHA-256。
    pub request: [u8; 32],
    /// 成功回复的规范描述摘要；单向方法使用零。
    pub response: [u8; 32],
}
/// 当前方法的方向、接入权限及可选的本地布局。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Method {
    /// 实际使用的线上契约。
    pub wire: WireContract,
    /// 位 0 表示可接收，位 1 表示可发起。
    pub directions: u8,
    /// 只有 1 表示允许来自玩家入口。
    pub client_callable: u8,
    /// 节点控制帧可用阶段；方法声明本身不授予实例节点管理权。
    pub peer_control: PeerAccess,
    /// 可接纳或产生的请求本地布局。
    pub request_local: ROption<LocalType>,
    /// 可接纳或产生的回复本地布局。
    pub response_local: ROption<LocalType>,
    /// 方法允许的最大成功响应 wire 字节数；单向方法为零。
    pub max_reply_bytes: u64,
    /// 本地成功响应的最大递归拥有字节数，无该布局时为零；不按 wire 长度收费。
    pub max_reply_local_bytes: u64,
}
/// 节点控制方法的准入范围，与玩家方法许可分开。
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub enum PeerAccess {
    /// 不是节点控制方法。
    None,
    /// 引导方法，未准入的候选链路也可使用。
    Bootstrap,
    /// 仅在本代次获准入后使用。
    Admitted,
}
/// 请求是否必须经过线上表示。
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RouteScope {
    /// 两端均在本进程内。
    Local,
    /// 远程节点或网络端。
    Network,
}
/// caller 是否能持有类型化成功响应。
#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, StableAbi)]
pub enum ReplyMode {
    /// 两端分别检查成功回复布局。
    Typed,
    /// raw 网关桥只允许可转发的字节响应。
    RawBytes,
}
impl Method {
    /// 在构造、移动或解码前选定请求与响应各自的表示。
    ///
    /// # Errors
    /// 方法、codec/选项、任一 wire 摘要或方向不匹配时返回 Protocol；绝不试解码。
    pub fn select(
        &self,
        target: &Self,
        scope: RouteScope,
        reply: ReplyMode,
    ) -> Result<[Representation; 2], Status> {
        if self.directions & 2 == 0 || target.directions & 1 == 0 || self.wire != target.wire {
            return Err(Status::PROTOCOL);
        }
        let local = |a: ROption<LocalType>, b: ROption<LocalType>| {
            if scope == RouteScope::Local
                && let (ROption::RSome(a), ROption::RSome(b)) = (a, b)
                && a.matches(b)
            {
                Representation::LocalOwned
            } else {
                Representation::Bytes
            }
        };
        Ok([
            local(self.request_local, target.request_local),
            if reply == ReplyMode::RawBytes {
                Representation::Bytes
            } else {
                local(self.response_local, target.response_local)
            },
        ])
    }
}
/// 版本化逻辑角色，不暴露其它角色的内部状态。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Role {
    /// 配置可引用的逻辑名。
    pub name: RStr<'static>,
    /// 角色契约修订。
    pub revision: u32,
    /// 该角色实际装配的 codec、修订和选项。
    pub codec: [u32; 3],
}
impl Role {
    /// 静态装配一个角色能力或所需依赖。
    pub const fn new(name: &'static str, revision: u32, codec: [u32; 3]) -> Self {
        Self {
            name: RStr::from_str(name),
            revision,
            codec,
        }
    }
}
/// 启动装载时由宿主核验的静态声明。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Descriptor {
    /// 描述表字节数。
    pub abi_size: u32,
    /// 动态库 ABI 版本。
    pub abi_version: u32,
    /// 框架帧修订。
    pub frame_rev: u32,
    /// 实现键，同进程内唯一。
    pub implementation: RStr<'static>,
    /// 仅供诊断，不作为 wire 或本地布局凭据。
    pub build_id: RStr<'static>,
    /// 必须声明状态可在线程间迁移且回调串行。
    pub migratable_serial: u8,
    /// 本实现可承担的角色。
    pub roles: RSlice<'static, Role>,
    /// 运行所需角色。
    pub requires: RSlice<'static, Role>,
    /// 共享方法元数据。
    pub methods: RSlice<'static, Method>,
}
impl Descriptor {
    /// 由 SDK 服务静态声明实现名、版本化角色及方法。
    pub const fn new(
        name: &'static str,
        build: &'static str,
        roles: &'static [Role],
        requires: &'static [Role],
        methods: &'static [Method],
    ) -> Self {
        Self {
            abi_size: size_of::<Self>() as u32,
            abi_version: ABI_VERSION,
            frame_rev: FRAME_REV,
            implementation: RStr::from_str(name),
            build_id: RStr::from_str(build),
            migratable_serial: 1,
            roles: RSlice::from_slice(roles),
            requires: RSlice::from_slice(requires),
            methods: RSlice::from_slice(methods),
        }
    }
}
/// 按资源和代次过滤迟到事件。
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, StableAbi)]
pub struct ResourceHandle {
    /// 资源槽身份。
    pub id: u64,
    /// 同一资源槽当前代次。
    pub generation: u64,
}
/// 表示类型；Bytes 只在当前调用期间借用。
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, StableAbi)]
pub enum Representation {
    /// 已编码字节。
    Bytes,
    /// 已验证本地拥有值。
    LocalOwned,
}
/// 消息表示与调用期字节借用；真正的唯一对象通过独立可变槽交付。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Body<'a> {
    /// 实际选择，不通过字节长度猜测。
    pub representation: Representation,
    /// 仅 Bytes 可有内容；宿主在回调返回前复制。
    pub bytes: RSlice<'a, u8>,
}
impl Body<'_> {
    fn matches_slot(self, slot: &LocalSlot) -> bool {
        match self.representation {
            Representation::Bytes => slot.is_none(),
            Representation::LocalOwned => self.bytes.is_empty() && slot.is_some(),
        }
    }
}
/// 空字节的成功回复仍是 Success；错误不能携带字节或占用本地对象槽。
#[repr(u8)]
#[derive(Clone, Copy, StableAbi)]
pub enum Reply<'a> {
    /// 采用接纳 request 时约定的表示与额度。
    Success(Body<'a>),
    /// 不经过业务 codec，只允许 RPC 终态错误。
    Error(Status),
}
impl Reply<'_> {
    fn matches_slot(self, slot: &LocalSlot) -> bool {
        match self {
            Self::Success(body) => body.matches_slot(slot),
            Self::Error(status) => status.is_reply_error() && slot.is_none(),
        }
    }
}
/// 消息来自普通服务还是获授权的玩家入口。公网帧不提供该字段。
#[repr(u8)]
#[derive(Clone, Copy, StableAbi)]
pub enum Entry {
    /// 普通实例间调用，没有玩家推送目的地。
    Service,
    /// 可选的源 Gate 私有会话令牌；不是公网 session、连接操作权或 reply token。
    /// 目标以宿主核定的 source 找到 Gate；Gate 复核代次与当前绑定后才能推送。
    Player {
        /// 没有主动推送需要时不签发。
        push_session: ROption<NonZeroU64>,
    },
}
/// 一次请求预留的成功响应额度，字节与本地对象分别计量。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct ReplyBudget {
    /// 请求建立前独立选择的响应表示。
    pub representation: Representation,
    /// 该表示下的最大拥有字节数。
    pub ceiling: u64,
}
/// 插件不能在此自报可信源地址；源由宿主绑定。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Outbound<'a> {
    /// 完整目标地址。
    pub destination: u64,
    /// 方法契约。
    pub wire: WireContract,
    /// 发出的消息。
    pub body: Body<'a>,
    /// 相对超时毫秒数。
    pub timeout_ms: u64,
    /// 回复表示及边界。
    pub reply: ReplyBudget,
    /// 请求的入口种类。宿主根据调用上下文校验，只有获授权实例可代玩家转发。
    pub entry: Entry,
}
impl Outbound<'_> {
    /// 校验单向／请求的契约、有限超时及实际槽；不转移所有权，也不代替宿主鉴权。
    ///
    /// # Errors
    /// 来源标记与表示矛盾、目标无效或单向／请求元数据不符返回 Protocol。
    pub fn validate(&self, is_request: bool, slot: &LocalSlot) -> Result<(), Status> {
        let bytes_only = self.body.representation == Representation::Bytes
            && self.reply.representation == Representation::Bytes;
        if ServiceAddress::try_from(self.destination).is_err()
            || self.wire.key == 0
            || self.wire.request == [0; 32]
            || !self.body.matches_slot(slot)
            || (is_request != (self.wire.response != [0; 32]))
            || (is_request != (self.timeout_ms != 0))
            || (!is_request
                && (self.reply.representation != Representation::Bytes || self.reply.ceiling != 0))
            || (matches!(self.entry, Entry::Player { .. }) && !bytes_only)
        {
            return Err(Status::PROTOCOL);
        }
        Ok(())
    }
}
/// 由宿主派发的业务回调元数据。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Message<'a> {
    /// 由 host 验证的来源。
    pub source: u64,
    /// 只可由目标实例用于回复；单向为零。
    pub reply_token: u64,
    /// 借用字节转入接收任务后持有的记账 token；没有转移时为零。
    pub charge_token: u64,
    /// 调用参数，目的地址必须为当前实例。
    pub request: Outbound<'a>,
}
/// HostApi 操作。使用有标签联合体保留每个操作的参数类型；未实现的能力一律 NotReady。
#[repr(u32)]
#[derive(StableAbi)]
pub enum HostOp<'a> {
    /// 选择目标地址、契约与 raw 回复标记对应的请求／回复表示；不转移消息。
    Select(u64, WireContract, ReplyMode),
    /// 投递单向消息；拒绝前不取走 LocalOwned。
    Send(Outbound<'a>),
    /// 建立有限截止时间的请求，并预留完成容量。
    Request(Outbound<'a>),
    /// 使用本实例绑定的回复令牌。即时 Busy 不同于已提交 Reply::Error(Busy)，前者不消耗令牌。
    Reply(u64, Reply<'a>),
    /// 按别名解析已经 Running 的地址。
    Resolve(RStr<'a>),
    /// 仅按获批准备依赖取得保留地址，不授予普通业务可见性。
    ResolvePrepared(RStr<'a>),
    /// 只从启动装载的实现创建，参数依次为实现键、配置、可选别名。
    ServiceCreate(RStr<'a>, RSlice<'a, u8>, ROption<RStr<'a>>),
    /// 成功返回 Future 的同一 poll 认领管理票据；宿主在此发布实例及 alias。
    CreateClaim(u64),
    /// 取消尚未认领的创建；不会销毁已经交付的实例。
    CreateAbandon(u64),
    /// caller 已消费完成槽；不是收到网络响应就立刻归还额度。
    CompletionConsumed(u64),
    /// 接收任务释放借用字节转入的预算；本地拥有值由随身凭证回收。
    ChargeReleased(u64),
    /// 通知专用生命周期槽有进展，不经普通业务 mailbox。
    LifecycleReady(LifecycleAction),
    /// 登记本地停止命令，不阻塞等待自身析构。
    ServiceStop(u64),
    /// 订阅本地终态：Ok + 非零 token 表示登记；Stopped/Faulted 为目标终态快照，
    /// NotFound 表示地址已回收。后两类均无订阅槽。
    ServiceSubscribe(u64),
    /// 取消本实例的订阅。
    ResourceCancel(u64),
    /// 相对延时与周期（毫秒）；周期为零表示单次。
    TimerSet(u64, u64),
    /// 取消本代次定时器，与 Timer 通知使用相同句柄。
    TimerCancel(ResourceHandle),
    /// 输出有限长度的日志，参数为等级和文本。
    Log(u8, RStr<'a>),
    /// 申请 TCP 或 KCP 监听资源。
    NetListen(Transport, RStr<'a>),
    /// 向本代次连接发一条完整业务帧。
    NetSend(Connection, RSlice<'a, u8>),
    /// 关闭本代次连接。
    NetClose(Connection),
    /// 清理记录交付后释放关闭连接额度。
    NetReleaseClosed(Connection),
    /// 申请节点 TCP 监听。
    PeerListen(RStr<'a>),
    /// 连接预期节点，取得有代次的资源。
    PeerConnect(RStr<'a>, u16),
    /// 发送有契约元数据的成员控制消息。
    PeerSendControl(ResourceHandle, WireContract, RSlice<'a, u8>),
    /// 核对节点和启动身份，在本连接代次上允许数据路由。
    PeerAdmit(ResourceHandle, u16, u64),
    /// 关闭节点链路并撤销本代次路由。
    PeerClose(ResourceHandle),
}
impl HostOp<'_> {
    /// 同一校验可在 SDK 提交前、host 接纳前执行，不读写业务对象。
    ///
    /// # Errors
    /// 表示、对象槽、回复状态或单／双向调用矛盾返回 Protocol；能力、代次、额度另由 host 裁决。
    pub fn validate(&self, slot: &LocalSlot) -> Result<(), Status> {
        let valid = match self {
            Self::Send(out) => return out.validate(false, slot),
            Self::Request(out) => return out.validate(true, slot),
            Self::Reply(token, reply) => *token != 0 && reply.matches_slot(slot),
            Self::CreateClaim(t)
            | Self::CreateAbandon(t)
            | Self::ChargeReleased(t)
            | Self::CompletionConsumed(t)
            | Self::ResourceCancel(t) => *t != 0 && slot.is_none(),
            Self::Select(dst, _, _) | Self::ServiceStop(dst) | Self::ServiceSubscribe(dst) => {
                ServiceAddress::try_from(*dst).is_ok() && slot.is_none()
            }
            Self::PeerConnect(_, node) => *node != 0 && slot.is_none(),
            Self::PeerAdmit(_, node, boot) => *node != 0 && *boot != 0 && slot.is_none(),
            _ => slot.is_none(),
        };
        valid.then_some(()).ok_or(Status::PROTOCOL)
    }
}
/// HostApi 的即时结果；异步完成另由相关令牌交付。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct HostResult {
    /// 接纳结果或直接错误。
    pub status: Status,
    /// Request / ServiceCreate 接纳的非零 pending / ticket，CreateClaim / Resolve 的地址，
    /// 或 NetListen / PeerListen / PeerConnect / TimerSet 的资源 ID（与 generation 一起使用）。
    pub value: u64,
    /// 有代次的操作使用此值。
    pub generation: u64,
    /// Select 成功时分别对应请求、回复。
    pub paths: [Representation; 2],
}
impl HostResult {
    /// 无有效输出的状态，不把未实现操作伪装为成功。
    pub const fn status(status: Status) -> Self {
        Self {
            status,
            value: 0,
            generation: 0,
            paths: [Representation::Bytes; 2],
        }
    }
}
/// 本机启动检查的输入；角色须来自调用者的 requires，方法键按调用者视角分向。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct StartupPeer<'a> {
    /// 要求部署在目标地址上的角色名；修订和 codec 来自双方实际的描述表。
    pub role: RStr<'a>,
    /// 调用者必须能向目标发送的普通服务方法。
    pub send_methods: RSlice<'a, u32>,
    /// 目标必须能向调用者发送的普通服务方法。
    pub receive_methods: RSlice<'a, u32>,
    /// 目标必须获准代玩家转发；此检查不授予能力。
    pub require_player_entry: bool,
    /// 目标必须已持有对当前调用实例的有效终态订阅。
    pub require_subscription: bool,
}
/// 启动对端检查的独立版本；不改变普通 HostOp 的编号或语义。
pub const STARTUP_PEER_CHECK_V1: u32 = 1;
/// 可选的单项宿主能力。取得本表前须经根布局校验，调用前须核对版本。
#[repr(C)]
#[derive(StableAbi)]
pub struct StartupPeerCheck {
    /// 本表实现的检查版本。
    pub version: u32,
    /// 参数为已签发的 context、本地目标地址、调用期借用的要求。只返回裁决，不发消息。
    pub check: extern "C" fn(u64, u64, StartupPeer<'_>) -> Status,
}
/// 进程常驻的宿主函数表。invoke 只能登记动作，不能栈上重入服务。
#[repr(C)]
#[derive(StableAbi)]
pub struct HostApi {
    /// 表的字节数。
    pub abi_size: u32,
    /// 宿主 ABI 版本。
    pub abi_version: u32,
    /// 使用宿主分配的实例能力上下文发起操作。
    pub invoke: extern "C" fn(u64, HostOp<'_>, &mut LocalSlot) -> HostResult,
    /// 只通知指定上下文的 token；可并发，不访问服务状态。
    pub wake: extern "C" fn(u64, u64),
    /// 可选的本机启动对端检查；缺席时不能以普通解析或订阅代替。
    pub startup_peer_check: ROption<&'static StartupPeerCheck>,
}
/// 创建入参；配置切片只在 create 返回前有效。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct CreateArgs<'a> {
    /// 常驻函数表。
    pub host: &'static HostApi,
    /// 宿主签发、可撤销的上下文句柄。
    pub context: u64,
    /// 本实例可撤销的普通唤醒令牌；SDK 不必猜测或从地址推导。
    pub wake_token: NonZeroU64,
    /// 分配给 SDK 的入站任务槽及负载上限。
    pub limits: SdkLimits,
    /// 本地完整服务地址。
    pub address: u64,
    /// 本地节点编号。
    pub node: u16,
    /// 本节点启动身份。
    pub boot: u64,
    /// 运维配置的节点通告端点；仅本地运行时可为空。
    pub advertised: RStr<'a>,
    /// 服务自有配置。
    pub config: RSlice<'a, u8>,
}
/// 宿主为此实例配置的 SDK 预算；不把插件自有分配当成可沙箱的内存。
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub struct SdkLimits {
    /// 同时驻留的入站任务槽上限。
    pub task_slots: NonZeroU32,
    /// 受管负载的字节上限；响应另受每次请求的预留额度限制。
    pub payload_bytes: NonZeroU64,
}
/// 单实例的一次性初始化动作，阻止旧 Prepare 的通知推进 Start。
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub enum LifecycleAction {
    /// 准备保留依赖。
    Prepare,
    /// 完成启动。
    Start,
}
/// 网关监听的两类完整应用帧传输，不提供原生 UDP 业务端口。
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub enum Transport {
    /// 有分帧的 TCP 字节流。
    Tcp,
    /// KCP 消息模式。
    Kcp,
}
/// owner 作用域内的连接。关闭后同一标识仅用于幂等归还关闭记录，不授权其它实例操作。
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub struct Connection {
    /// 区分 TCP 流与 KCP 消息会话，不假设二者有相同的关闭语义。
    pub transport: Transport,
    /// 本代次连接身份。
    pub handle: ResourceHandle,
}
/// 网络事件保留监听来源与连接代次，不能仅靠复用的整数槽位路由。
#[repr(u8)]
#[derive(StableAbi)]
pub enum NetEvent<'a> {
    /// 监听句柄、传输和实际绑定端点。
    ListenReady(ResourceHandle, Transport, RStr<'a>),
    /// 此次监听未建立；按原申请句柄关联失败。
    ListenFailed(ResourceHandle, Status),
    /// 监听、连接、远端端点；KCP 只在传输握手后产生。
    Accepted(ResourceHandle, Connection, RStr<'a>),
    /// 一帧有界的业务字节。
    Data(Connection, RSlice<'a, u8>),
    /// 相应代次出口可能可写，不保证下一次发送成功。
    Writable(Connection),
    /// 本代次有 I/O 错误，不等于已经消费终态清理记录。
    Error(Connection, Status),
    /// 关闭原因；该连接标识同时是仍占额度的关闭记录令牌。
    Closed(Connection, Status),
}
/// 一条已完成固定引导握手但未必可用于业务的连接候选。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct PeerCandidate<'a> {
    /// 本机连接 ID 与代次。
    pub link: ResourceHandle,
    /// 从 socket 得到的对端端点，不是对方自报的通告地址。
    pub observed: RStr<'a>,
    /// 对端节点号。
    pub remote_node: u16,
    /// 对端本次启动身份。
    pub remote_boot: u64,
    /// 双方共同认可的拨号方节点号。
    pub dialer: u16,
    /// 拨号方本次启动身份。
    pub dialer_boot: u64,
    /// 拨号方在该启动身份下的单调尝试号，不使用到达顺序裁决重复连接。
    pub attempt: u64,
}
/// 宿主对该连接代次的实际裁决，不取自角色控制 body。
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub enum PeerStage {
    /// 已通过固定引导头，未获准入。
    Candidate,
    /// 已通过本代次的 peer_admit 校验。
    Admitted,
}
/// 节点连接事件：成员控制与普通数据就绪是两个状态。
#[repr(u8)]
#[derive(StableAbi)]
pub enum PeerEvent<'a> {
    /// 本机监听句柄和实际端点。
    ListenReady(ResourceHandle, RStr<'a>),
    /// 对应监听申请的异步失败。
    ListenFailed(ResourceHandle, Status),
    /// 两端共享的连接候选身份。
    Connected(PeerCandidate<'a>),
    /// 固定引导头已校验，角色在自己的 codec 中解码控制 body。
    Control(ResourceHandle, PeerStage, WireContract, RSlice<'a, u8>),
    /// 链路双方已经允许普通数据。
    DataReady(ResourceHandle),
    /// 有界写出口的合并提示。
    Writable(ResourceHandle),
    /// 仅影响该代次路由。
    Closed(ResourceHandle, Status),
}
/// 宿主裁决归属与代次后的通知，不能用业务消息代替。
#[repr(u8)]
#[derive(StableAbi)]
pub enum Event<'a> {
    /// 管理票据就绪（尚未交付），Future 醒来后方可认领。
    CreateReady(u64, Status),
    /// 相关令牌的唯一终结，可能同时带一个可移动的本地回复。
    Completion {
        /// 待消费的本次 pending 令牌。
        pending: u64,
        /// 唯一终结结果；错误无 body。
        reply: Reply<'a>,
        /// 成功字节负载移交后独立还账的凭证；没有移交为零，本地对象自带凭证。
        charge_token: u64,
    },
    /// 定时器与代次。
    Timer(ResourceHandle),
    /// 监听、已接纳连接或有界的 I/O 事件。
    Network(NetEvent<'a>),
    /// 节点传输状态或已校验元数据的控制帧。
    Peer(PeerEvent<'a>),
    /// 订阅令牌和已终结的本地地址。
    ResourceGone(u64, u64),
    /// 已合并的回复出口可写提示。
    ReplyWritable(u64),
}
impl Event<'_> {
    /// 可由宿主登记为初始化动作的依赖；还须匹配该动作、owner 和资源代次。
    pub fn is_lifecycle_dependency(&self) -> bool {
        matches!(
            self,
            Self::Network(NetEvent::ListenReady(..) | NetEvent::ListenFailed(..))
                | Self::Peer(_)
                | Self::Timer(_)
                | Self::ResourceGone(..)
                | Self::Completion { .. }
                | Self::CreateReady(..)
        )
    }
    /// 可在 Stopping 中对账的一条通知；不包括普通 timer、可写或新业务。
    pub fn is_cleanup(&self) -> bool {
        matches!(
            self,
            Self::Network(NetEvent::Closed(..))
                | Self::Peer(PeerEvent::Closed(..))
                | Self::ResourceGone(..)
                | Self::Completion { .. }
                | Self::CreateReady(..)
        )
    }
    fn matches_slot(&self, slot: &LocalSlot) -> bool {
        match self {
            Self::Completion {
                pending,
                reply,
                charge_token,
            } => {
                *pending != 0
                    && reply.matches_slot(slot)
                    && (*charge_token == 0
                        || matches!(
                            reply,
                            Reply::Success(Body {
                                representation: Representation::Bytes,
                                ..
                            })
                        ))
            }
            Self::Network(NetEvent::ListenFailed(_, s) | NetEvent::Error(_, s))
            | Self::Peer(PeerEvent::ListenFailed(_, s)) => s.is_failure() && slot.is_none(),
            Self::Network(NetEvent::Closed(_, s)) | Self::Peer(PeerEvent::Closed(_, s)) => {
                (*s == Status::OK || s.is_failure()) && slot.is_none()
            }
            Self::CreateReady(ticket, s) => {
                *ticket != 0 && (*s == Status::OK || s.is_failure()) && slot.is_none()
            }
            Self::ResourceGone(token, address) => {
                *token != 0 && ServiceAddress::try_from(*address).is_ok() && slot.is_none()
            }
            Self::ReplyWritable(token) => *token != 0 && slot.is_none(),
            _ => slot.is_none(),
        }
    }
}
/// 单实例的同步控制片段；事件用途由宿主登记的动作／资源决定，不来自公网。
#[repr(u32)]
#[derive(StableAbi)]
pub enum Control<'a> {
    /// 准备本地依赖，尚不可接纳普通业务。
    Prepare,
    /// 启动实例；是否对外可见由宿主决定。
    Start,
    /// 非零 poll 次数预算，不抢占单次用户回调。
    Poll(u32),
    /// 仅推进指定初始化动作，不 poll 普通业务任务；第二个参数为非零预算。
    LifecyclePoll(LifecycleAction, u32),
    /// 普通运行期通知。
    Event(Event<'a>),
    /// 初始化动作预留通路的所属事件，不因一次事件回调返回成功就发布实例。
    LifecycleEvent(LifecycleAction, Event<'a>),
    /// 关闭或完成记录，只交专用清理入口，不恢复业务 Future。
    Cleanup(Event<'a>),
    /// 正常停止的取消阶段，SDK 只丢弃任务。
    Stop,
    /// 专用取消，Faulted 亦可调用，不执行业务。
    Cancel,
}
impl Control<'_> {
    /// 核对预算、通知用途和附属对象槽；失败不消费对象。
    ///
    /// # Errors
    /// 零 poll 预算、阶段不支持的通知或错误附带对象返回 Protocol。
    pub fn validate(&self, slot: &LocalSlot) -> Result<(), Status> {
        let valid = match self {
            Self::Poll(0) | Self::LifecyclePoll(_, 0) => false,
            Self::Event(e) => e.matches_slot(slot),
            Self::LifecycleEvent(_, e) => e.is_lifecycle_dependency() && e.matches_slot(slot),
            Self::Cleanup(e) => e.is_cleanup() && e.matches_slot(slot),
            _ => slot.is_none(),
        };
        valid.then_some(()).ok_or(Status::PROTOCOL)
    }
}
/// 有界推进的结果。状态与“还有可 poll 工作”分别表达。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct Progress {
    /// 回调结果，可指示生命周期 Pending。
    pub status: Status,
    /// 非零表示仍有任务就绪。
    pub more_ready: u8,
    /// 非零表示可接纳新的业务任务。
    pub task_capacity: u8,
}
impl Progress {
    /// 没有可运行任务时返回指定状态。
    pub const fn idle(status: Status) -> Self {
        Self {
            status,
            more_ready: 0,
            task_capacity: 0,
        }
    }
}
/// 服务库导出的四个同步回调。状态由创建它的库析构；不跨 ABI 传 Future。
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct ServiceApi {
    /// 表字节数。
    pub abi_size: u32,
    /// 服务 ABI 版本。
    pub abi_version: u32,
    /// 只读描述。
    pub descriptor: &'static Descriptor,
    /// 仅成功时交出一个非空实例指针。
    pub create: extern "C" fn(CreateArgs<'_>, &mut *mut ()) -> Status,
    /// 以独占的活实例接纳业务，消费标志取决于槽本身。
    pub on_message: unsafe extern "C" fn(*mut (), Message<'_>, &mut LocalSlot) -> Status,
    /// 排他派发控制或有限预算 poll。
    pub on_control: unsafe extern "C" fn(*mut (), Control<'_>, &mut LocalSlot) -> Progress,
    /// 仅调用一次；不接受已析构指针。
    pub destroy: unsafe extern "C" fn(*mut ()) -> Status,
}
const _: () = {
    assert!(
        core::mem::offset_of!(HostApi, abi_size) == 0
            && core::mem::offset_of!(HostApi, abi_version) == 4
    );
    assert!(
        core::mem::offset_of!(ServiceApi, abi_size) == 0
            && core::mem::offset_of!(ServiceApi, abi_version) == 4
    );
    assert!(
        core::mem::offset_of!(Descriptor, abi_size) == 0
            && core::mem::offset_of!(Descriptor, abi_version) == 4
    );
};

/// 唯一版本化的服务入口位于常驻根模块；上游先校验全表，再允许取得函数指针。
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = ServiceModuleRef)))]
#[sabi(missing_field(panic))]
pub struct ServiceModule {
    /// 服务 ABI v1，返回同库常驻表。
    #[sabi(last_prefix_field)]
    pub ace_service_v1: extern "C" fn() -> &'static ServiceApi,
}
impl RootModule for ServiceModuleRef {
    abi_stable::declare_root_module_statics! {ServiceModuleRef}
    const BASE_NAME: &'static str = "ace_service";
    const NAME: &'static str = "ace_service";
    const VERSION_STRINGS: VersionStrings = abi_stable::package_version_strings!();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reply_distinguishes_empty_success_from_bodyless_terminal_error() {
        let slot = ROption::RNone;
        let body = Body {
            representation: Representation::Bytes,
            bytes: RSlice::from_slice(&[]),
        };
        assert_eq!(
            HostOp::Reply(1, Reply::Success(body)).validate(&slot),
            Ok(())
        );
        assert_eq!(
            HostOp::Reply(1, Reply::Error(Status::BUSY)).validate(&slot),
            Ok(())
        );
        for s in [Status::OK, Status::PENDING, Status(u32::MAX)] {
            assert_eq!(
                HostOp::Reply(1, Reply::Error(s)).validate(&slot),
                Err(Status::PROTOCOL)
            );
        }
        assert_eq!(
            Control::Event(Event::Completion {
                pending: 1,
                reply: Reply::Error(Status::TIMEOUT),
                charge_token: 2
            })
            .validate(&slot),
            Err(Status::PROTOCOL)
        );
    }
}
