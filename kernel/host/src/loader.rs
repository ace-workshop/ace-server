//! 启动时一次性装载可信实现。这里没有运行中加载或卸载接口。
use crate::config::IngressGrants;
use abi_stable::{
    library::{LibraryError, lib_header_from_path},
    std_types::{ROption::RNone, RSlice},
};
use ace_net_proto::ServiceAddress;
use ace_server_abi::{ABI_VERSION, FRAME_REV, Status, native::*};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    ptr::{self, NonNull},
    sync::atomic::{AtomicBool, Ordering},
};

static STARTED: AtomicBool = AtomicBool::new(false);
const MAX_LIBS: usize = 128;
const MAX_METHODS: usize = 4096;
const MAX_ROLES: usize = 32;
/// 静态装载、布局或声明冲突；实例回调错误独立返回 Status。
#[derive(Debug)]
pub enum LoadError {
    /// 已尝试本进程的启动装载，不再打开更多库。
    AlreadyStarted,
    /// 参数、布局声明或版本不符。
    Contract(&'static str),
    /// 配置文件系统路径不可用。
    Path(PathBuf, std::io::Error),
    /// 上游动态库错误保留路径、布局/版本检查原因。
    Library(PathBuf, Box<LibraryError>),
}
impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyStarted => f.write_str("本进程已经执行启动装载"),
            Self::Contract(s) => write!(f, "拒绝装载：{s}"),
            Self::Path(p, e) => write!(f, "{}：{e}", p.display()),
            Self::Library(p, e) => write!(f, "装载 {}：{e}", p.display()),
        }
    }
}
impl std::error::Error for LoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Path(_, e) => Some(e),
            Self::Library(_, e) => Some(e.as_ref()),
            _ => None,
        }
    }
}
/// 按声明校验，不把静态元数据、路径或版本当作对恶意本机库的沙箱。
fn validate(api: &ServiceApi) -> Result<(), LoadError> {
    let d = api.descriptor;
    if api.abi_size < size_of::<ServiceApi>() as u32
        || d.abi_size < size_of::<Descriptor>() as u32
        || api.abi_version != ABI_VERSION
        || d.abi_version != ABI_VERSION
        || d.frame_rev != FRAME_REV
        || d.migratable_serial != 1
    {
        return Err(LoadError::Contract("版本、表长或可迁移串行声明不匹配"));
    }
    if !valid_name(d.implementation.as_str())
        || d.build_id.len() > 256
        || d.methods.len() > MAX_METHODS
        || d.roles.len() > MAX_ROLES
        || d.requires.len() > MAX_ROLES
    {
        return Err(LoadError::Contract("描述表名称或数量不合法"));
    }
    for roles in [d.roles, d.requires] {
        let mut seen = BTreeSet::new();
        for r in roles.as_slice() {
            if !valid_name(r.name.as_str()) || r.revision == 0 || !seen.insert(r.name.as_str()) {
                return Err(LoadError::Contract("角色声明无效或重复"));
            }
        }
    }
    let mut keys = BTreeSet::new();
    for m in d.methods.as_slice() {
        if !(1..=3).contains(&m.directions)
            || m.client_callable > 1
            || (m.client_callable == 1
                && (m.directions & 1 == 0 || m.peer_control != PeerAccess::None))
            || (m.wire.response == [0; 32]
                && (m.response_local.is_some() || m.max_reply_bytes != 0))
            || (m.response_local.is_none() && m.max_reply_local_bytes != 0)
            || m.wire.key == 0
            || m.wire.request == [0; 32]
            || !keys.insert(m.wire.key)
        {
            return Err(LoadError::Contract("方法声明无效或重复"));
        }
    }
    Ok(())
}
fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_./-".contains(&c))
}
pub(crate) fn find_method(d: &Descriptor, key: u32) -> Result<Method, Status> {
    d.methods
        .as_slice()
        .iter()
        .find(|m| m.wire.key == key)
        .copied()
        .ok_or(Status::UNKNOWN_METHOD)
}
pub(crate) fn check_peer_method(
    d: &Descriptor,
    w: WireContract,
    stage: PeerStage,
    direction: u8,
) -> Result<(), Status> {
    let m = find_method(d, w.key)?;
    if m.directions & direction == 0 || m.wire != w || w.response != [0; 32] {
        return Err(Status::PROTOCOL);
    }
    if !matches!(
        (m.peer_control, stage),
        (PeerAccess::Bootstrap, _) | (PeerAccess::Admitted, PeerStage::Admitted)
    ) {
        return Err(Status::DENIED);
    }
    Ok(())
}
/// 仅暴露已装载条目的实现注册表，实例销毁不卸载库。
pub struct Implementations(BTreeMap<&'static str, &'static ServiceApi>);
impl Implementations {
    /// 一次装载指定的有限个文件。上游用 libloading 打开，并为静态 vtable/layout
    /// 保留库映射至进程退出；每个文件用自己的 LibHeader，不能按同一 root 类型缓存首库。
    ///
    /// # Safety
    /// 路径必须属于受信启动配置且内容不可由不可信方替换。库的静态初始化器、
    /// 导出表、状态指针、独占/Send/析构约束必须符合 SDK 契约；布局检查不是沙箱。
    ///
    /// # Errors
    /// 重复启动、超限、缺少布局、版本不符、重名或方法冲突都会拒绝整次启动。
    pub unsafe fn load_startup(paths: &[PathBuf]) -> Result<Self, LoadError> {
        if paths.is_empty() || paths.len() > MAX_LIBS {
            return Err(LoadError::Contract("启动库数量越界"));
        }
        if STARTED.swap(true, Ordering::AcqRel) {
            return Err(LoadError::AlreadyStarted);
        }
        let mut registry = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let mut methods = BTreeMap::new();
        for p in paths {
            let path = p
                .canonicalize()
                .map_err(|e| LoadError::Path(p.clone(), e))?;
            if !seen.insert(path.clone()) {
                return Err(LoadError::Contract("重复库路径"));
            }
            let api = load(&path)?;
            validate(api)?;
            for m in api.descriptor.methods.as_slice() {
                if methods
                    .insert(m.wire.key, m.wire)
                    .is_some_and(|prev| prev != m.wire)
                {
                    return Err(LoadError::Contract("同方法键 wire 声明冲突"));
                }
            }
            if registry
                .insert(api.descriptor.implementation.as_str(), api)
                .is_some()
            {
                return Err(LoadError::Contract("实现键重复"));
            }
        }
        Ok(Self(registry))
    }
    /// 检查配置显式选出的 (实现键, 可选角色)。无角色的业务实例也要核对所需依赖。
    ///
    /// # Errors
    /// 实现/依赖缺失或同角色版本/codec 不匹配时拒绝整组；角色不是实例身份。
    pub fn check_bindings(&self, bindings: &[(&str, Option<&str>)]) -> Result<(), LoadError> {
        let mut provided = BTreeMap::new();
        let mut active = Vec::with_capacity(bindings.len());
        for &(key, role) in bindings {
            let api = self
                .0
                .get(key)
                .ok_or(LoadError::Contract("选定实例的实现未装载"))?;
            active.push(api.descriptor);
            if let Some(name) = role {
                let r = api
                    .descriptor
                    .roles
                    .as_slice()
                    .iter()
                    .find(|r| r.name.as_str() == name)
                    .ok_or(LoadError::Contract("实现未声明选定角色"))?;
                let contract = (r.revision, r.codec);
                if provided
                    .insert(name, contract)
                    .is_some_and(|old| old != contract)
                {
                    return Err(LoadError::Contract("同角色的修订或 codec 冲突"));
                }
            }
        }
        for d in active {
            for r in d.requires.as_slice() {
                if provided.get(r.name.as_str()) != Some(&(r.revision, r.codec)) {
                    return Err(LoadError::Contract("所需角色、修订或 codec 不匹配"));
                }
            }
        }
        Ok(())
    }
    /// 列出已装载描述；不表示有可用运行实例或监听器。
    pub fn descriptors(&self) -> impl Iterator<Item = &'static Descriptor> + '_ {
        self.0.values().map(|p| p.descriptor)
    }
    /// 唯一实例所有者的构造入口。创建不等于开始接收业务。
    ///
    /// # Errors
    /// 实现不存在、地址或配置不符、插件初始化失败/panic 时不交出实例。
    pub fn create(
        &self,
        key: &str,
        args: CreateArgs<'_>,
        grants: IngressGrants,
    ) -> Result<Instance, Status> {
        let api = *self.0.get(key).ok_or(Status::NOT_FOUND)?;
        let address = ServiceAddress::try_from(args.address).map_err(|_| Status::PROTOCOL)?;
        let mut out = ptr::null_mut();
        let status = (api.create)(args, &mut out);
        if status != Status::OK {
            return Err(status);
        }
        let state = NonNull::new(out).ok_or(Status::PROTOCOL)?;
        Ok(Instance {
            state,
            api,
            address,
            grants,
            phase: Phase::Created,
        })
    }
}
fn load(path: &Path) -> Result<&'static ServiceApi, LoadError> {
    let err = |e| LoadError::Library(path.to_owned(), Box::new(e));
    let header = lib_header_from_path(path).map_err(err)?;
    if header.layout().is_none() {
        return Err(LoadError::Contract("不接受省略根布局的库"));
    }
    let m = header.init_root_module::<ServiceModuleRef>().map_err(err)?;
    Ok((m.ace_service_v1())())
}
#[derive(PartialEq, Eq)]
enum Phase {
    Created,
    Initializing(LifecycleAction),
    Prepared,
    AwaitingClaim,
    Running,
    Stopping,
    Faulted,
    Cancelled,
}
/// 排他句柄；没有 Clone 或 Sync。宿主 driver 可迁移整个实例，不能并发调用。
pub struct Instance {
    state: NonNull<()>,
    api: &'static ServiceApi,
    address: ServiceAddress,
    grants: IngressGrants,
    phase: Phase,
}
// SAFETY: state 只来自已核验可迁移串行声明的受信 create；SDK 要求 S:Send，
// 只有这个非 Clone 句柄可调用或析构。api 指向常驻只读函数表；其余为拥有的整数状态。
// 所有状态调用要求 &mut self，包括 Drop，不为 Instance 实现 Sync。
unsafe impl Send for Instance {}
impl Instance {
    /// 读取签发地址，不通过实现名猜测实例身份。
    pub fn address(&self) -> ServiceAddress {
        self.address
    }
    /// prepare/start 已成功且已认领发布才为真；不是整节点 readiness。
    pub fn is_running(&self) -> bool {
        self.phase == Phase::Running
    }
    /// 管理方认领已经完成 Start 的实例后开放普通回调；此句柄本身不登记 alias 或裁决票据。
    ///
    /// # Errors
    /// 初始化尚未完成、已经发布或开始终结时拒绝重复发布。
    pub fn publish(&mut self) -> Result<(), Status> {
        if self.phase != Phase::AwaitingClaim {
            return Err(self.unavailable());
        }
        self.phase = Phase::Running;
        Ok(())
    }
    /// 立即关闭普通回调，不调用插件。宿主可随后关闭资源，再投递有限 Cleanup。
    pub fn begin_stop(&mut self) {
        if !matches!(self.phase, Phase::Cancelled | Phase::Faulted) {
            self.phase = Phase::Stopping;
        }
    }
    fn unavailable(&self) -> Status {
        match self.phase {
            Phase::Faulted => Status::FAULTED,
            Phase::Stopping | Phase::Cancelled => Status::STOPPED,
            _ => Status::NOT_READY,
        }
    }
    fn receive_method(&self, wire: WireContract) -> Result<&Method, Status> {
        let method = self
            .api
            .descriptor
            .methods
            .as_slice()
            .iter()
            .find(|m| m.wire.key == wire.key)
            .ok_or(Status::UNKNOWN_METHOD)?;
        if method.directions & 1 == 0 || method.wire != wire {
            return Err(Status::PROTOCOL);
        }
        Ok(method)
    }
    fn check_event(&self, event: &Event<'_>) -> Result<(), Status> {
        if let Event::Peer(peer) = event {
            if !self.grants.peer_control {
                return Err(Status::DENIED);
            }
            if let PeerEvent::Control(_, stage, wire, _) = peer {
                check_peer_method(self.api.descriptor, *wire, *stage, 1)?;
            }
        }
        Ok(())
    }
    fn cancel_tasks(&mut self, event: Control<'_>) {
        if self.phase != Phase::Cancelled {
            // SAFETY: 此唯一活句柄独占 state，Stop/Cancel 都映射到本库的专用取消 wrapper。
            let result = unsafe { (self.api.on_control)(self.state.as_ptr(), event, &mut RNone) };
            if result.status != Status::OK {
                std::process::abort();
            }
            self.phase = Phase::Cancelled;
        }
    }
    /// 驱动一个有界控制片段。资源须已经过宿主的 owner/代次核对；生命周期通知须属于该动作。
    /// Stopping 只接收清理，Faulted 只接受 Cancel。取消后可先清理宿主额度，最后 drop 句柄。
    pub fn control(&mut self, event: Control<'_>, slot: &mut LocalSlot) -> Progress {
        if self.phase == Phase::Faulted && !matches!(event, Control::Cancel) {
            return Progress::idle(Status::FAULTED);
        }
        if let Err(status) = event.validate(slot) {
            return Progress::idle(status);
        }
        let action = match &event {
            Control::Prepare if self.phase == Phase::Created => Some(LifecycleAction::Prepare),
            Control::Start if self.phase == Phase::Prepared => Some(LifecycleAction::Start),
            Control::LifecyclePoll(a, _) if self.phase == Phase::Initializing(*a) => Some(*a),
            Control::Poll(_) if self.is_running() => None,
            Control::Event(e) if self.is_running() => {
                if let Err(s) = self.check_event(e) {
                    return Progress::idle(s);
                }
                None
            }
            Control::LifecycleEvent(a, e) if self.phase == Phase::Initializing(*a) => {
                if let Err(s) = self.check_event(e) {
                    return Progress::idle(s);
                }
                None
            }
            Control::Cleanup(e) if self.phase == Phase::Stopping => {
                if let Err(s) = self.check_event(e) {
                    return Progress::idle(s);
                }
                None
            }
            Control::Stop | Control::Cancel
                if matches!(
                    self.phase,
                    Phase::Stopping | Phase::Faulted | Phase::Cancelled
                ) =>
            {
                self.cancel_tasks(event);
                return Progress::idle(Status::OK);
            }
            _ => return Progress::idle(self.unavailable()),
        };
        // SAFETY: state 来自同一 api 的成功创建，&mut self 独占其整个回调。
        let result = unsafe { (self.api.on_control)(self.state.as_ptr(), event, slot) };
        if result.status == Status::FAULTED {
            self.phase = Phase::Faulted;
        } else if let Some(a) = action {
            self.phase = match (result.status, a) {
                (Status::PENDING, _) => Phase::Initializing(a),
                (Status::OK, LifecycleAction::Prepare) => Phase::Prepared,
                (Status::OK, LifecycleAction::Start) => Phase::AwaitingClaim,
                _ => Phase::Stopping,
            };
        }
        result
    }
    /// 单次接纳。未发布、未知方法、权限或形状不符均不取走对象。source/入口来自宿主，不是公网帧。
    pub fn message(&mut self, msg: Message<'_>, slot: &mut LocalSlot) -> Status {
        if !self.is_running() {
            return self.unavailable();
        }
        let req = msg.request;
        if let Err(s) = req.validate(msg.reply_token != 0, slot) {
            return s;
        }
        let Ok(source) = ServiceAddress::try_from(msg.source) else {
            return Status::PROTOCOL;
        };
        if req.destination != self.address.bits()
            || (msg.charge_token != 0 && req.body.representation != Representation::Bytes)
            || (source.node() != self.address.node()
                && (req.body.representation != Representation::Bytes
                    || req.reply.representation != Representation::Bytes))
        {
            return Status::PROTOCOL;
        }
        let method = match self.receive_method(req.wire) {
            Ok(m) => m,
            Err(s) => return s,
        };
        if method.peer_control != PeerAccess::None
            || matches!(req.entry, Entry::Player { .. }) && method.client_callable != 1
        {
            return Status::DENIED;
        }
        let ceiling = match req.reply.representation {
            Representation::Bytes => method.max_reply_bytes,
            Representation::LocalOwned => method.max_reply_local_bytes,
        };
        if req.reply.ceiling > ceiling {
            return Status::TOO_SMALL;
        }
        if (req.reply.representation == Representation::LocalOwned
            && method.response_local.is_none())
            || slot.as_ref().into_option().is_some_and(|p| {
                !method
                    .request_local
                    .into_option()
                    .is_some_and(|t| t.matches(p.local_type()))
            })
        {
            return Status::PROTOCOL;
        }
        // SAFETY: 函数表已核验；唯一活句柄和 slot 在回调中独占，外部字节仅借用。
        let status = unsafe { (self.api.on_message)(self.state.as_ptr(), msg, slot) };
        if status == Status::FAULTED {
            self.phase = Phase::Faulted;
        }
        status
    }
}
impl Drop for Instance {
    fn drop(&mut self) {
        // 遗漏显式取消时兜底；正常路径在宿主释放仍归此实例的额度之前已取消。
        self.cancel_tasks(Control::Cancel);
        // SAFETY: Drop 持有最后且唯一的句柄，原库在此释放成功 create 的那一个 state。
        if unsafe { (self.api.destroy)(self.state.as_ptr()) } != Status::OK {
            std::process::abort();
        }
    }
}
/// 骨架宿主用的空能力表；所有操作显式不可用，不宣称运行时资源已建。
pub static UNAVAILABLE: HostApi = HostApi {
    abi_size: size_of::<HostApi>() as u32,
    abi_version: ABI_VERSION,
    invoke: unavailable,
    wake: inactive_wake,
    startup_peer_check: RNone,
};
extern "C" fn unavailable(_: u64, op: HostOp<'_>, slot: &mut LocalSlot) -> HostResult {
    HostResult::status(op.validate(slot).err().unwrap_or(Status::NOT_READY))
}
extern "C" fn inactive_wake(_: u64, _: u64) {}
/// 组装不提供运行时能力的启动检查入参；用于检验服务装载而非部署。
pub fn inspection_args(address: ServiceAddress, boot: u64, config: &[u8]) -> CreateArgs<'_> {
    CreateArgs {
        host: &UNAVAILABLE,
        context: address.bits(),
        wake_token: NonZeroU64::MIN,
        limits: SdkLimits {
            task_slots: NonZeroU32::MIN,
            payload_bytes: NonZeroU64::MIN,
        },
        address: address.bits(),
        node: address.node().get(),
        boot,
        advertised: abi_stable::std_types::RStr::from_str(""),
        config: RSlice::from_slice(config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admitted_control_is_not_a_bootstrap_or_a_send_capability() {
        const W: WireContract = WireContract {
            key: 1,
            codec: [5, 1, 0],
            request: [3; 32],
            response: [0; 32],
        };
        const D: Descriptor = Descriptor::new(
            "test",
            "",
            &[],
            &[],
            &[Method {
                wire: W,
                directions: 1,
                client_callable: 0,
                peer_control: PeerAccess::Admitted,
                request_local: RNone,
                response_local: RNone,
                max_reply_bytes: 0,
                max_reply_local_bytes: 0,
            }],
        );
        assert_eq!(
            check_peer_method(&D, W, PeerStage::Candidate, 1),
            Err(Status::DENIED)
        );
        assert_eq!(
            check_peer_method(&D, W, PeerStage::Admitted, 2),
            Err(Status::PROTOCOL)
        );
        assert_eq!(check_peer_method(&D, W, PeerStage::Admitted, 1), Ok(()));
    }
}
