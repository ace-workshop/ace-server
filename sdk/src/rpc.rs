use crate::{
    Context, Direction, LocalDto, LocalOwned, ServiceAddress, Status, TypedMethod, bytes_method,
    native::*, tasks::lock,
};
use abi_stable::std_types::ROption::{RNone, RSome};
use ace_codec::{Codec, CodecError, Limits, NoReply};
use ace_server_local::LocalType;
use futures_channel::oneshot;
use std::{collections::BTreeMap, future::Future, ops::Deref};

/// 将已经核对布局的只读稳定值转为业务 DTO。可复制字段，不调用 wire codec。
pub trait FromLocal: LocalDto {
    /// 源对象仍保有其真实额度；需要复制的普通 DTO 由业务库分配。
    fn from_stable(value: &Self::Stable) -> Self;
}
/// typed RPC 的本机 Rust 错误；具体 codec 原因不作为 RPC body 或 native 状态传送。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypedRpcError {
    /// 宿主接纳、RPC 无体终态或本地表示/响应容量检查的稳定状态。
    Status(Status),
    /// 当前库的编解码器返回的具体原因，包括所选 ace 实现的原始错误。
    Codec(CodecError),
}
impl From<Status> for TypedRpcError {
    fn from(s: Status) -> Self {
        Self::Status(s)
    }
}
impl From<CodecError> for TypedRpcError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}
impl std::fmt::Display for TypedRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(s) => write!(f, "RPC 状态：{s:?}"),
            Self::Codec(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for TypedRpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(e) => Some(e),
            Self::Status(_) => None,
        }
    }
}
impl TypedRpcError {
    fn reply_status(self, ctx: &Context, method: u32) -> Status {
        match self {
            Self::Status(s) => s,
            Self::Codec(e) => {
                // 本节点诊断只含方法键和有限错误枚举，不记录输入或失败的输出缓冲。
                let diagnostic = format!("方法 {method} 编解码失败：{e:?}");
                ctx.command(HostOp::Log(1, diagnostic.as_str().into()));
                Status::PROTOCOL
            }
        }
    }
}
struct Adapter<T> {
    encode: fn(&T, Limits) -> Result<Vec<u8>, CodecError>,
    decode: fn(&[u8], Limits) -> Result<T, TypedRpcError>,
    local: Option<LocalAdapter<T>>,
}
struct LocalAdapter<T> {
    pack: fn(T, usize) -> Result<LocalOwned, Status>,
    read: fn(&LocalOwned) -> Result<T, Status>,
}
impl<T> Copy for LocalAdapter<T> {}
impl<T> Clone for LocalAdapter<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Adapter<T> {}
impl<T> Clone for Adapter<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Adapter<T> {
    const fn bytes<C: Codec<T>>() -> Self {
        Self {
            encode: |v, l| {
                let mut dst = vec![0; l.wire_bytes];
                let mut scratch = vec![0; l.decoded_bytes];
                let n = C::encode_into(v, &mut dst, &mut scratch, l)?;
                if n > dst.len() {
                    return Err(CodecError::Limit);
                }
                dst.truncate(n);
                Ok(dst)
            },
            decode: |b, l| {
                if b.len() > l.wire_bytes {
                    return Err(Status::TOO_SMALL.into());
                }
                C::decode(b, l).map_err(TypedRpcError::Codec)
            },
            local: None,
        }
    }
    const fn local(mut self) -> Self
    where
        T: FromLocal,
    {
        self.local = Some(LocalAdapter {
            pack: |v, n| v.into_local(n).map_err(|_| Status::TOO_SMALL),
            read: |p| {
                p.view::<T::Stable>()
                    .map(T::from_stable)
                    .map_err(|_| Status::PROTOCOL)
            },
        });
        self
    }
    fn pack(
        &self,
        v: T,
        path: Representation,
        l: Limits,
        limit: usize,
    ) -> Result<Owned, TypedRpcError> {
        match path {
            Representation::Bytes => {
                Ok(Owned::Bytes((self.encode)(&v, l)?.into_boxed_slice(), None))
            }
            Representation::LocalOwned => Ok(Owned::Local((self
                .local
                .ok_or(Status::PROTOCOL)?
                .pack)(v, limit)?)),
        }
    }
    fn unpack(&self, body: Owned, l: Limits) -> Result<Managed<T>, TypedRpcError> {
        let value = match &body {
            Owned::Bytes(b, ..) => (self.decode)(b, l)?,
            Owned::Local(p) => (self.local.ok_or(Status::PROTOCOL)?.read)(p)?,
        };
        Ok(Managed { value, _body: body })
    }
}
/// SDK typed 业务方法。输入和成功响应单独装配本地表示；同一声明用于生产 descriptor。
pub struct RpcMethod<Q, R> {
    method: Method,
    input: Adapter<Q>,
    output: Option<Adapter<R>>,
}
impl<Q, R> Copy for RpcMethod<Q, R> {}
impl<Q, R> Clone for RpcMethod<Q, R> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<Q, R> RpcMethod<Q, R> {
    /// 绑定一个已经具备两向 codec 的调用方法；缺本地声明时只走 Bytes。
    pub const fn call<C: Codec<Q> + Codec<R>>(
        m: TypedMethod<C, Q, R>,
        direction: Direction,
    ) -> Self {
        Self {
            method: bytes_method(m, direction),
            input: Adapter::bytes::<C>(),
            output: Some(Adapter::bytes::<C>()),
        }
    }
    /// 为输入增加可验证的拥有型表示，不影响响应选择。
    pub const fn local_input(mut self) -> Self
    where
        Q: FromLocal,
    {
        self.method.request_local = RSome(LocalType::of::<Q::Stable>());
        self.input = self.input.local();
        self
    }
    /// 为成功响应增加可验证的拥有型表示和递归容量上限。
    pub const fn local_output(mut self, ceiling: u64) -> Self
    where
        R: FromLocal,
    {
        self.method.response_local = RSome(LocalType::of::<R::Stable>());
        self.method.max_reply_local_bytes = ceiling;
        if let Some(a) = self.output {
            self.output = Some(a.local())
        }
        self
    }
    /// 供已装载库验证的静态 native 方法声明。
    pub const fn descriptor(&self) -> Method {
        self.method
    }
}
impl<Q> RpcMethod<Q, NoReply> {
    /// 绑定无成功响应的单向方法。
    pub const fn send<C: Codec<Q>>(m: TypedMethod<C, Q, NoReply>, direction: Direction) -> Self {
        Self {
            method: bytes_method(m, direction),
            input: Adapter::bytes::<C>(),
            output: None,
        }
    }
}
/// 业务可跨 await 持有的 typed 值。保留底层额度凭据直到最后一次析构。
pub struct Managed<T> {
    value: T,
    _body: Owned,
}
impl<T> Deref for Managed<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
struct Receipt {
    api: &'static HostApi,
    key: u64,
    id: u64,
    completion: bool,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        (self.api.invoke)(
            self.key,
            if self.completion {
                HostOp::CompletionConsumed(self.id)
            } else {
                HostOp::ChargeReleased(self.id)
            },
            &mut RNone,
        );
    }
}
enum Owned {
    Bytes(Box<[u8]>, Option<Receipt>),
    Local(LocalOwned),
}
impl Owned {
    fn receive(
        ctx: &Context,
        b: Body<'_>,
        slot: &mut LocalSlot,
        token: u64,
    ) -> Result<Self, Status> {
        match b.representation {
            Representation::Bytes => Ok(Self::Bytes(
                b.bytes.as_slice().into(),
                ctx.receipt(token, false),
            )),
            Representation::LocalOwned => slot
                .take()
                .into_option()
                .map(Self::Local)
                .ok_or(Status::PROTOCOL),
        }
    }
    fn invoke(&mut self, ctx: &Context, f: impl FnOnce(Body<'_>) -> HostOp<'_>) -> HostResult {
        match self {
            Self::Bytes(b, _) => ctx.command(f(Body {
                representation: Representation::Bytes,
                bytes: b.as_ref().into(),
            })),
            Self::Local(_) => {
                let Self::Local(p) = std::mem::replace(self, Self::Bytes(Box::default(), None))
                else {
                    unreachable!()
                };
                let mut slot = RSome(p);
                let r = ctx.invoke(
                    f(Body {
                        representation: Representation::LocalOwned,
                        bytes: [].as_slice().into(),
                    }),
                    &mut slot,
                );
                if let RSome(p) = slot {
                    *self = Self::Local(p)
                }
                r
            }
        }
    }
}
/// raw 字节成功响应；释放这个值才释放字节额度，不等同于归还已消费的完成槽。
pub struct ManagedBytes {
    bytes: Box<[u8]>,
    _charge: Option<Receipt>,
}
impl Deref for ManagedBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
struct ResultSlot {
    result: Result<Owned, Status>,
    _slot: Option<Receipt>,
}
#[derive(Default)]
pub(crate) struct Waiters {
    pub(super) timers: BTreeMap<u64, crate::timer::TimerSlot>,
    calls: BTreeMap<u64, oneshot::Sender<ResultSlot>>,
    creates: BTreeMap<u64, oneshot::Sender<Status>>,
    writes: BTreeMap<u64, oneshot::Sender<()>>,
}
fn check(r: HostResult) -> Result<HostResult, Status> {
    if r.status == Status::OK {
        Ok(r)
    } else {
        Err(r.status)
    }
}
impl Context {
    fn receipt(&self, id: u64, completion: bool) -> Option<Receipt> {
        (id != 0).then_some(Receipt {
            api: self.api,
            key: self.key,
            id,
            completion,
        })
    }
    /// 将默认 codec 的字节／工作区预算收紧到本实例已配置的负载上限。
    pub fn limits_codec(&self) -> Limits {
        let cap = self.limits().payload_bytes.get() as usize;
        let mut l = Limits::default();
        l.wire_bytes = l.wire_bytes.min(cap);
        l.decoded_bytes = l.decoded_bytes.min(cap);
        l
    }
    fn register_call(
        &self,
        f: impl FnOnce() -> HostResult,
    ) -> Result<oneshot::Receiver<ResultSlot>, Status> {
        if !self.managed {
            return Err(Status::NOT_READY);
        }
        let mut w = lock(&self.waiters);
        let id = check(f())?.value;
        let (tx, rx) = oneshot::channel();
        w.calls.insert(id, tx);
        Ok(rx)
    }
    fn paths(&self, dst: ServiceAddress, m: Method) -> Result<[Representation; 2], Status> {
        Ok(check(self.command(HostOp::Select(dst.bits(), m.wire, ReplyMode::Typed)))?.paths)
    }
    /// 按契约及逐向选择提交单向值。成功只代表本机接纳。
    /// # Errors
    /// 编码失败返回 `Codec` 且不提交；方法、权限、阶段、布局或额度拒绝返回 `Status`。
    pub fn send<Q>(
        &self,
        method: RpcMethod<Q, NoReply>,
        dst: ServiceAddress,
        q: Q,
    ) -> Result<(), TypedRpcError> {
        let paths = self.paths(dst, method.method)?;
        let l = self.limits_codec();
        let mut b =
            method
                .input
                .pack(q, paths[0], l, self.limits().payload_bytes.get() as usize)?;
        check(b.invoke(self, |body| {
            HostOp::Send(Outbound {
                destination: dst.bits(),
                wire: method.method.wire,
                body,
                timeout_ms: 0,
                reply: ReplyBudget {
                    representation: Representation::Bytes,
                    ceiling: 0,
                },
                entry: Entry::Service,
            })
        }))?;
        Ok(())
    }
    /// 提交 typed 调用，随即返回可丢弃的本地 Future。drop 不取消被调业务。
    /// 按选定响应表示取方法与调用方 `ceiling` 的较小值。
    /// # Errors
    /// 提交前编码失败同步返回 `Codec`；Future 分别返回无体终态的 `Status` 或成功体解码失败的 `Codec`。
    pub fn call<Q, R: Send + 'static>(
        &self,
        method: RpcMethod<Q, R>,
        dst: ServiceAddress,
        q: Q,
        timeout_ms: u64,
        ceiling: u64,
    ) -> Result<
        impl Future<Output = Result<Managed<R>, TypedRpcError>> + Send + use<Q, R>,
        TypedRpcError,
    > {
        let out = method.output.ok_or(Status::PROTOCOL)?;
        let paths = self.paths(dst, method.method)?;
        let l = self.limits_codec();
        let ceiling = ceiling.min(match paths[1] {
            Representation::Bytes => method.method.max_reply_bytes,
            Representation::LocalOwned => method.method.max_reply_local_bytes,
        });
        let mut b =
            method
                .input
                .pack(q, paths[0], l, self.limits().payload_bytes.get() as usize)?;
        let rx = self.register_call(|| {
            b.invoke(self, |body| {
                HostOp::Request(Outbound {
                    destination: dst.bits(),
                    wire: method.method.wire,
                    body,
                    timeout_ms,
                    reply: ReplyBudget {
                        representation: paths[1],
                        ceiling,
                    },
                    entry: Entry::Service,
                })
            })
        })?;
        Ok(async move {
            let slot = rx.await.map_err(|_| Status::STOPPED)?;
            out.unpack(slot.result?, l)
        })
    }
    /// 转发已经编码的单向字节，不建完成槽；来源永远由绑定的 host context 核定。
    /// # Errors
    /// 非字节、入口未授权、契约不符或 mailbox 满时拒绝。
    pub fn raw_send(&self, r: Outbound<'_>) -> Result<(), Status> {
        if r.body.representation != Representation::Bytes {
            return Err(Status::PROTOCOL);
        }
        check(self.command(HostOp::Select(r.destination, r.wire, ReplyMode::RawBytes)))?;
        check(self.command(HostOp::Send(r)))?;
        Ok(())
    }
    /// 转发已经编码的 request，不试解码或计算来源。成功体和完成槽分别还账。
    /// # Errors
    /// 只接纳双字节表示，宿主复核玩家授权、目标契约和额度；Future 返回业务终态。
    pub fn raw_call(
        &self,
        r: Outbound<'_>,
    ) -> Result<impl Future<Output = Result<ManagedBytes, Status>> + Send + use<>, Status> {
        if r.body.representation != Representation::Bytes
            || r.reply.representation != Representation::Bytes
        {
            return Err(Status::PROTOCOL);
        }
        check(self.command(HostOp::Select(r.destination, r.wire, ReplyMode::RawBytes)))?;
        let rx = self.register_call(|| self.command(HostOp::Request(r)))?;
        Ok(async move {
            match rx.await.map_err(|_| Status::STOPPED)?.result? {
                Owned::Bytes(b, p) => Ok(ManagedBytes {
                    bytes: b,
                    _charge: p,
                }),
                _ => Err(Status::PROTOCOL),
            }
        })
    }
    /// 从已经装载的实现创建实例。ticket 在返回地址的最后一次 poll 才认领。
    /// # Errors
    /// 授权、配额、初始化、认领或有限期限失败时不发布半实例。
    pub fn create_service(
        &self,
        implementation: &str,
        config: &[u8],
        alias: Option<&str>,
    ) -> Result<impl Future<Output = Result<ServiceAddress, Status>> + Send + use<>, Status> {
        if !self.managed {
            return Err(Status::NOT_READY);
        }
        let mut w = lock(&self.waiters);
        let id = check(self.command(HostOp::ServiceCreate(
            implementation.into(),
            config.into(),
            alias.map(Into::into).into(),
        )))?
        .value;
        let (tx, rx) = oneshot::channel();
        w.creates.insert(id, tx);
        drop(w);
        let ticket = Claim {
            ctx: self.clone(),
            id: Some(id),
        };
        Ok(async move {
            let result = rx.await.map_err(|_| Status::STOPPED)?;
            if result != Status::OK {
                return Err(result);
            }
            ticket.deliver(id)
        })
    }
    pub(crate) fn route_event(&self, e: &Event<'_>, slot: &mut LocalSlot) -> Option<Status> {
        match e {
            Event::Timer(h) => self.tick_timer(*h),
            Event::Completion {
                pending,
                reply,
                charge_token,
            } => {
                let result = match reply {
                    Reply::Success(b) => Owned::receive(self, *b, slot, *charge_token),
                    Reply::Error(e) => Err(*e),
                };
                let value = ResultSlot {
                    result,
                    _slot: self.receipt(*pending, true),
                };
                let tx = lock(&self.waiters).calls.remove(pending);
                if let Some(tx) = tx {
                    let _ = tx.send(value);
                }
                Some(Status::OK)
            }
            Event::CreateReady(id, s) => {
                let tx = lock(&self.waiters).creates.remove(id);
                if let Some(tx) = tx {
                    let _ = tx.send(*s);
                }
                Some(Status::OK)
            }
            Event::ReplyWritable(id) => {
                let tx = lock(&self.waiters).writes.remove(id);
                if let Some(tx) = tx {
                    let _ = tx.send(());
                }
                Some(Status::OK)
            }
            _ => None,
        }
    }
}
struct Claim {
    ctx: Context,
    id: Option<u64>,
}
impl Claim {
    fn deliver(mut self, id: u64) -> Result<ServiceAddress, Status> {
        let a = check(self.ctx.command(HostOp::CreateClaim(id)))?;
        self.id = None;
        ServiceAddress::try_from(a.value).map_err(|_| Status::PROTOCOL)
    }
}
impl Drop for Claim {
    fn drop(&mut self) {
        if let Some(t) = self.id {
            lock(&self.ctx.waiters).creates.remove(&t);
            self.ctx.command(HostOp::CreateAbandon(t));
        }
    }
}
/// 已被 SDK 接纳并持有的入站调用；使用 typed handler 会自动生成唯一回复。
pub struct Incoming {
    ctx: Context,
    wire: WireContract,
    source: ServiceAddress,
    entry: Entry,
    token: u64,
    reply: ReplyBudget,
    body: Owned,
}
impl Incoming {
    pub(crate) fn take(ctx: Context, m: Message<'_>, slot: &mut LocalSlot) -> Result<Self, Status> {
        let source = ServiceAddress::try_from(m.source).map_err(|_| Status::PROTOCOL)?;
        let body = Owned::receive(&ctx, m.request.body, slot, m.charge_token)?;
        Ok(Self {
            ctx,
            wire: m.request.wire,
            source,
            entry: m.request.entry,
            token: m.reply_token,
            reply: m.request.reply,
            body,
        })
    }
    /// 经宿主复核的实际方法。
    pub fn wire(&self) -> WireContract {
        self.wire
    }
    /// 经宿主核定的来源。
    pub fn source(&self) -> ServiceAddress {
        self.source
    }
    /// 经宿主核定的入口与可选推送会话。
    pub fn entry(&self) -> Entry {
        self.entry
    }
    /// 将输入交给拥有型 Send handler，返回值自动按选定表示提交。出口 Busy 保留同一个结果，不重跑业务。
    pub fn handle<
        Q: Send + 'static,
        R: Send + 'static,
        F: Future<Output = Result<R, TypedRpcError>> + Send + 'static,
    >(
        self,
        m: RpcMethod<Q, R>,
        f: impl FnOnce(Managed<Q>) -> F + Send + 'static,
    ) -> crate::Handler {
        Box::pin(async move {
            let l = self.ctx.limits_codec();
            let result = async {
                if self.wire != m.method.wire {
                    return Err(Status::PROTOCOL.into());
                }
                let adapter = m.output.ok_or(Status::PROTOCOL)?;
                let q = m.input.unpack(self.body, l)?;
                let v = f(q).await?;
                let body =
                    adapter.pack(v, self.reply.representation, l, self.reply.ceiling as usize)?;
                if let Owned::Bytes(b, _) = &body
                    && b.len() as u64 > self.reply.ceiling
                {
                    return Err(Status::TOO_SMALL.into());
                }
                Ok(body)
            }
            .await
            .map_err(|e: TypedRpcError| e.reply_status(&self.ctx, self.wire.key));
            respond(&self.ctx, self.token, result).await;
        })
    }
    /// 单向输入不建 pending，也不伪造回执。
    pub fn receive<Q: Send + 'static, F: Future<Output = ()> + Send + 'static>(
        self,
        m: RpcMethod<Q, NoReply>,
        f: impl FnOnce(Managed<Q>) -> F + Send + 'static,
    ) -> crate::Handler {
        Box::pin(async move {
            if self.wire == m.method.wire && self.token == 0 {
                match m.input.unpack(self.body, self.ctx.limits_codec()) {
                    Ok(q) => f(q).await,
                    Err(e) => {
                        e.reply_status(&self.ctx, self.wire.key);
                    }
                }
            }
        })
    }
    /// 在业务解码前拒绝，未知或不允许的错误码统一转为无体 Protocol。
    pub fn reject(self, e: Status) -> crate::Handler {
        Box::pin(async move { respond(&self.ctx, self.token, Err(e)).await })
    }
}
async fn respond(ctx: &Context, token: u64, mut body: Result<Owned, Status>) {
    if token == 0 {
        return;
    }
    loop {
        let r = match &mut body {
            Ok(p) => p.invoke(ctx, |b| HostOp::Reply(token, Reply::Success(b))),
            Err(e) => ctx.command(HostOp::Reply(
                token,
                Reply::Error(if e.is_reply_error() {
                    *e
                } else {
                    Status::PROTOCOL
                }),
            )),
        };
        if r.status != Status::BUSY {
            return;
        }
        let (tx, rx) = oneshot::channel();
        lock(&ctx.waiters).writes.insert(token, tx);
        if rx.await.is_err() {
            return;
        }
    }
}
