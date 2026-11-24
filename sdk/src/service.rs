//! 将 Rust 服务封装为串行、可迁移、不可重入的服务表。业务状态永不以普通 Rust 布局跨库访问。
use abi_stable::{prefix_type::PrefixTypeTrait, std_types::ROption::RNone};
use ace_net_proto::ServiceAddress;
use ace_server_abi::{ABI_VERSION, Status, native::*};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// 调用实例的不可伪造来源由宿主根据 context 解析，不接受客户端指定的来源字段。
/// 这是受信 native SDK 的约束，不是内存沙箱。
#[derive(Clone)]
pub struct Context {
    pub(crate) api: &'static HostApi,
    pub(crate) key: u64,
    pub(crate) managed: bool,
    pub(crate) waiters: std::sync::Arc<std::sync::Mutex<crate::rpc::Waiters>>,
    wake_token: core::num::NonZeroU64,
    limits: SdkLimits,
    address: ServiceAddress,
    boot: u64,
    advertised: std::sync::Arc<str>,
}
impl Context {
    /// 当前实例的完整地址。
    pub fn address(&self) -> ServiceAddress {
        self.address
    }
    /// 本节点启动身份，不是地址的一部分。
    pub fn boot(&self) -> u64 {
        self.boot
    }
    /// 从启动配置复制的通告端点；本地模式可以为空。
    pub fn advertised_endpoint(&self) -> &str {
        &self.advertised
    }
    /// 宿主在 create 时为本实例分配的 SDK 预算。
    pub fn limits(&self) -> SdkLimits {
        self.limits
    }
    /// 发起版本化宿主操作；矛盾的表示或令牌返回 Protocol，不取走对象。
    /// 正式接纳仍由宿主按 context 检查权限和额度；以槽的剩余内容判定所有权。
    pub fn invoke(&self, op: HostOp<'_>, slot: &mut LocalSlot) -> HostResult {
        if let Err(status) = op.validate(slot) {
            return HostResult::status(status);
        }
        if self.managed {
            match op {
                HostOp::TimerSet(d, p) => return self.set_timer(d, p, None),
                HostOp::TimerCancel(h) => return self.cancel_timer(h),
                _ => {}
            }
        }
        (self.api.invoke)(self.key, op, slot)
    }
    /// 无本地对象时使用此入口，Bytes 只借用至返回。
    pub fn command(&self, op: HostOp<'_>) -> HostResult {
        self.invoke(op, &mut RNone)
    }
    /// 仅用创建时签发的普通令牌跨线程通知；不用地址推导，也不在当前栈内 poll。
    pub fn wake(&self) {
        (self.api.wake)(self.key, self.wake_token.get())
    }
}
/// 每份实现可以有多个独占实例；状态可以迁移，不要求 Sync。
/// ```compile_fail,E0277
/// use ace_server_sdk::{Context, Service, Status, native::Descriptor};
/// struct Local(std::rc::Rc<()>);
/// impl Service for Local {
///     const DESCRIPTOR: Descriptor = Descriptor::new("local", "1", &[], &[], &[]);
///     fn create(_: Context, _: &[u8]) -> Result<Self, Status> { Ok(Self(std::rc::Rc::new(()))) }
/// }
/// ```
pub trait Service: Send + 'static + Sized {
    /// 所有实例共用的静态契约。
    const DESCRIPTOR: Descriptor;
    /// 建立私有状态；未成功时宿主不调用 destroy。
    ///
    /// # Errors
    /// 无效配置或资源不足时返回具体状态，不产生部分可用实例。
    fn create(context: Context, config: &[u8]) -> Result<Self, Status>;
    /// 接纳一条业务，不能保存字节借用。尚未实现的服务显式 NotReady。
    fn on_message(&mut self, _message: Message<'_>, _slot: &mut LocalSlot) -> Status {
        Status::NOT_READY
    }
    /// 生命周期和有界 poll；不支持的控制动作不返回成功。
    fn on_control(&mut self, _control: Control<'_>, _slot: &mut LocalSlot) -> Progress {
        Progress::idle(Status::NOT_READY)
    }
    /// 只对账或释放这一条关闭／终结记录，不调度普通 handler。Faulted 不进此入口。
    fn on_cleanup(&mut self, _event: Event<'_>, _slot: &mut LocalSlot) -> Status {
        Status::NOT_READY
    }
    /// 丢弃任务与 Waker，不允许在此 poll 业务；析构再次 panic 时 fail-stop。
    fn cancel(&mut self) {}
}
extern "C" fn create<S: Service>(args: CreateArgs<'_>, out: &mut *mut ()) -> Status {
    if !out.is_null()
        || args.host.abi_version != ABI_VERSION
        || args.host.abi_size < size_of::<HostApi>() as u32
        || args.context == 0
        || !(1..=ServiceAddress::MAX_SEQUENCE).contains(&args.boot)
        || args.advertised.len() > 512
    {
        return Status::PROTOCOL;
    }
    let Ok(address) = ServiceAddress::try_from(args.address) else {
        return Status::PROTOCOL;
    };
    if args.node != address.node().get() {
        return Status::PROTOCOL;
    }
    let ctx = Context {
        api: args.host,
        key: args.context,
        managed: false,
        waiters: Default::default(),
        wake_token: args.wake_token,
        limits: args.limits,
        address,
        boot: args.boot,
        advertised: args.advertised.as_str().into(),
    };
    match catch_unwind(AssertUnwindSafe(|| S::create(ctx, args.config.as_slice()))) {
        Ok(Ok(s)) => {
            *out = Box::into_raw(Box::new(s)).cast();
            Status::OK
        }
        Ok(Err(s)) if s.is_failure() => s,
        Ok(Err(_)) => Status::PROTOCOL,
        Err(_) => Status::FAULTED,
    }
}
unsafe fn exclusive<'a, S: Service>(raw: *mut ()) -> Option<&'a mut S> {
    let ptr = raw.cast::<S>();
    if ptr.is_null() || !ptr.is_aligned() {
        return None;
    }
    // SAFETY: 调用方必须传入本表 create 尚未销毁的指针，整个 'a 期间唯一且不重入。
    Some(unsafe { &mut *ptr })
}
unsafe extern "C" fn message<S: Service>(
    raw: *mut (),
    msg: Message<'_>,
    slot: &mut LocalSlot,
) -> Status {
    // SAFETY: 唯一实例持有者按表契约串行调用，借用不离开本次回调。
    let Some(service) = (unsafe { exclusive::<S>(raw) }) else {
        return Status::PROTOCOL;
    };
    catch_unwind(AssertUnwindSafe(|| service.on_message(msg, slot))).unwrap_or(Status::FAULTED)
}
unsafe extern "C" fn control<S: Service>(
    raw: *mut (),
    event: Control<'_>,
    slot: &mut LocalSlot,
) -> Progress {
    // SAFETY: 与 message 相同；在本次独占回调以外不保留 &mut。
    let Some(service) = (unsafe { exclusive::<S>(raw) }) else {
        return Progress::idle(Status::PROTOCOL);
    };
    if let Err(status) = event.validate(slot) {
        return Progress::idle(status);
    }
    if matches!(event, Control::Stop | Control::Cancel) {
        if catch_unwind(AssertUnwindSafe(|| service.cancel())).is_err() {
            std::process::abort();
        }
        return Progress::idle(Status::OK);
    }
    catch_unwind(AssertUnwindSafe(|| match event {
        Control::Cleanup(event) => Progress::idle(service.on_cleanup(event, slot)),
        event => service.on_control(event, slot),
    }))
    .unwrap_or(Progress::idle(Status::FAULTED))
}
unsafe extern "C" fn destroy<S: Service>(raw: *mut ()) -> Status {
    if raw.is_null() || !raw.cast::<S>().is_aligned() {
        return Status::PROTOCOL;
    }
    if catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: 与 create 的 Box 精确配对；调用者已撤销状态且不可能再次析构。
        drop(unsafe { Box::from_raw(raw.cast::<S>()) });
    }))
    .is_err()
    {
        std::process::abort();
    }
    Status::OK
}
/// 生成常驻只读表；库只销毁自己分配的原生状态。
pub extern "C" fn api<S: Service>() -> &'static ServiceApi {
    &ServiceApi {
        abi_size: size_of::<ServiceApi>() as u32,
        abi_version: ABI_VERSION,
        descriptor: &S::DESCRIPTOR,
        create: create::<S>,
        on_message: message::<S>,
        on_control: control::<S>,
        destroy: destroy::<S>,
    }
}
/// 从薄 cdylib 的 export_root_module 入口返回；上游验证根模块的递归布局。
pub fn export<S: Service>() -> ServiceModuleRef {
    ServiceModule {
        ace_service_v1: api::<S>,
    }
    .leak_into_prefix()
}

impl Context {
    /// 查询已发布的本地实例，不因别名相同而重投先前的调用。
    /// # Errors
    /// 别名不存在或实例未运行时失败。
    pub fn resolve(&self, alias: &str) -> Result<ServiceAddress, Status> {
        address(self.command(HostOp::Resolve(alias.into())))
    }
    /// 在 Prepare 中取得清单授权的依赖地址，不授予普通业务可见性。
    /// # Errors
    /// 依赖未创建、未授权或阶段不符时失败。
    pub fn resolve_prepared(&self, alias: &str) -> Result<ServiceAddress, Status> {
        address(self.command(HostOp::ResolvePrepared(alias.into())))
    }
    /// 在本机清单的 Prepare／Start 中核对实际角色对端，不获取新地址、订阅或普通消息权限。
    /// # Errors
    /// 能力缺席返回 NotReady，版本或角色契约不匹配返回 Protocol；阶段、权限、方法及订阅错误原样返回。
    pub fn check_startup_peer(
        &self,
        target: ServiceAddress,
        required: StartupPeer<'_>,
    ) -> Result<(), Status> {
        let api = self
            .api
            .startup_peer_check
            .into_option()
            .ok_or(Status::NOT_READY)?;
        if api.version != STARTUP_PEER_CHECK_V1 {
            return Err(Status::PROTOCOL);
        }
        let status = (api.check)(self.key, target.bits(), required);
        match status {
            Status::OK => Ok(()),
            e if e.is_failure() => Err(e),
            _ => Err(Status::PROTOCOL),
        }
    }
    /// 登记本地目标的一次终态；同一目标的重复订阅合并。
    /// # Errors
    /// 已终结返回终态，回收后返回 NotFound，预留槽不足返回 Busy。
    pub fn subscribe(&self, target: ServiceAddress) -> Result<u64, Status> {
        let r = self.command(HostOp::ServiceSubscribe(target.bits()));
        unit_result(r)?;
        Ok(r.value)
    }
    /// 建立从 control 接收事件的 timer；周期为零时只触发一次。
    /// # Errors
    /// 时限、阶段或 timer 槽不满足时不产生句柄。
    pub fn event_timer(&self, delay_ms: u64, period_ms: u64) -> Result<ResourceHandle, Status> {
        resource(self.command(HostOp::TimerSet(delay_ms, period_ms)))
    }
    /// 登记本实例的监听；实际绑定结果由 ListenReady/ListenFailed 携同一代次异步交付。
    /// # Errors
    /// 端点不是数值 IP:port、预算不足或阶段不允许时拒绝。
    pub fn net_listen(
        &self,
        transport: Transport,
        endpoint: &str,
    ) -> Result<ResourceHandle, Status> {
        let r = self.command(HostOp::NetListen(transport, endpoint.into()));
        resource(r)
    }
    /// 提交一份完整公共帧，只承诺本地有界出口接纳；Busy 后关注合并的 Writable。
    /// # Errors
    /// 帧、owner、代次或容量不符时不发送。
    pub fn net_send(&self, conn: Connection, frame: &[u8]) -> Result<(), Status> {
        unit_result(self.command(HostOp::NetSend(conn, frame.into())))
    }
    /// 请求关闭本代连接；KCP 对端靠 idle 判定，不模拟 FIN。
    /// # Errors
    /// 非 owner、旧代次或终止的实例不能关闭其它资源。
    pub fn net_close(&self, conn: Connection) -> Result<(), Status> {
        unit_result(self.command(HostOp::NetClose(conn)))
    }
    /// 消费 Closed 清理记录后归还连接额度；不会释放活跃连接。
    /// # Errors
    /// 调用者、代次或状态不符时拒绝。
    pub fn net_release_closed(&self, conn: Connection) -> Result<(), Status> {
        unit_result(self.command(HostOp::NetReleaseClosed(conn)))
    }
}
fn address(r: HostResult) -> Result<ServiceAddress, Status> {
    unit_result(r)?;
    r.value.try_into().map_err(|_| Status::PROTOCOL)
}
fn unit_result(r: HostResult) -> Result<(), Status> {
    if r.status == Status::OK {
        Ok(())
    } else {
        Err(r.status)
    }
}

impl Context {
    /// 申请本节点受管 TCP 监听；实际结果经同一代次的 PeerEvent 返回。
    /// # Errors
    /// 无控制授权、端点不在部署域或额度不足时拒绝。
    pub fn peer_listen(&self, endpoint: &str) -> Result<ResourceHandle, Status> {
        resource(self.command(HostOp::PeerListen(endpoint.into())))
    }
    /// 异步连接部署许可的端点与预期节点，返回的候选尚不授权数据。
    /// # Errors
    /// 阶段、权限、身份域或预算不符时拒绝。
    pub fn peer_connect(&self, endpoint: &str, node: u16) -> Result<ResourceHandle, Status> {
        resource(self.command(HostOp::PeerConnect(endpoint.into(), node)))
    }
    /// 提交角色编码的单向控制体；宿主复核方法、方向、阶段和两份摘要。
    /// # Errors
    /// 旧链接或授权不符时拒绝；Busy 可等待合并的 Writable 后重试。
    pub fn peer_send_control(
        &self,
        link: ResourceHandle,
        wire: WireContract,
        body: &[u8],
    ) -> Result<(), Status> {
        unit_result(self.command(HostOp::PeerSendControl(link, wire, body.into())))
    }
    /// 在 verified 候选上准入指定身份；不代表双方业务已发布。
    /// # Errors
    /// 高水位需落盘时先返回 Busy；Writable 后重试，未返回成功前不能发布成员。
    pub fn peer_admit(&self, link: ResourceHandle, node: u16, boot: u64) -> Result<(), Status> {
        unit_result(self.command(HostOp::PeerAdmit(link, node, boot)))
    }
    /// 立即撤销本代次路由并关闭 TCP，不能操作其它 owner 的资源。
    /// # Errors
    /// 错权限、代次或实例阶段返回错误。
    pub fn peer_close(&self, link: ResourceHandle) -> Result<(), Status> {
        unit_result(self.command(HostOp::PeerClose(link)))
    }
}
fn resource(r: HostResult) -> Result<ResourceHandle, Status> {
    unit_result(r)?;
    Ok(ResourceHandle {
        id: r.value,
        generation: r.generation,
    })
}
