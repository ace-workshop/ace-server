use crate::{
    Context, Handler, Incoming, Service, Status,
    native::*,
    tasks::{Tasks, lock},
};
use std::sync::{Arc, Mutex};
/// Actor 私有状态的短同步访问；闭包不能移出借用或跨 await 持锁。
pub struct State<S>(Arc<Mutex<S>>);
impl<S> Clone for State<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<S> State<S> {
    /// 只在一次非阻塞同步片段内访问状态，不返回 guard。
    pub fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> R {
        f(&mut *lock(&self.0))
    }
}
/// 使用有界 SDK 任务与自动回复的业务服务，仍由原四入口 wrapper 导出。
pub trait ActorService: Send + 'static + Sized {
    /// 由实际 typed 绑定生成的能力声明。
    const DESCRIPTOR: Descriptor;
    /// 创建实例状态；失败不产生可发布实例。
    /// # Errors
    /// 配置或依赖不满足时返回具体错误。
    fn create(ctx: Context, config: &[u8]) -> Result<Self, Status>;
    /// 匹配方法并返回拥有所有权的 handler，不能阻塞或借用实例跨 await。
    fn handle(state: State<Self>, msg: Incoming) -> Handler;
    /// 接纳 owner 的一帧网络数据；可在同步检查／提交后返回一个有界任务等待结果。
    /// 默认沿用同步 control。返回 Busy 前不能取走输入或产生业务副作用。
    /// # Errors
    /// 拒收返回稳定状态，借用字节不能留给返回的 Future。
    fn network_data(
        state: State<Self>,
        conn: Connection,
        bytes: &[u8],
    ) -> Result<Option<Handler>, Status> {
        let status = state.with(|s| {
            s.control(Control::Event(Event::Network(NetEvent::Data(
                conn,
                bytes.into(),
            ))))
            .status
        });
        if status == Status::OK {
            Ok(None)
        } else {
            Err(status)
        }
    }
    /// 生命周期动作及 timer、终态等非 RPC 事件。未支持的动作明确 NotReady。
    fn control(&mut self, op: Control<'_>) -> Progress {
        let _ = op;
        Progress::idle(Status::NOT_READY)
    }
    /// Stopping 只对账，不恢复普通 Future。
    fn cleanup(&mut self, event: Event<'_>) -> Status {
        let _ = event;
        Status::NOT_READY
    }
    /// SDK 已丢弃任务后清理本实例其余状态；不能恢复业务。
    fn cancel(&mut self) {}
}
/// 对 `ActorService` 装配有界任务集和就绪队列；导出 `export::<Actor<MyService>>()`。
pub struct Actor<S> {
    ctx: Context,
    state: State<S>,
    tasks: Tasks,
}
impl<S: ActorService> Service for Actor<S> {
    const DESCRIPTOR: Descriptor = S::DESCRIPTOR;
    fn create(mut ctx: Context, config: &[u8]) -> Result<Self, Status> {
        ctx.managed = true;
        Ok(Self {
            state: State(Arc::new(Mutex::new(S::create(ctx.clone(), config)?))),
            tasks: Tasks::new(ctx.clone()),
            ctx,
        })
    }
    fn on_message(&mut self, m: Message<'_>, slot: &mut LocalSlot) -> Status {
        if !self.tasks.has_capacity() {
            return Status::BUSY;
        }
        let msg = match Incoming::take(self.ctx.clone(), m, slot) {
            Ok(m) => m,
            Err(e) => return e,
        };
        self.tasks.insert(S::handle(self.state.clone(), msg));
        Status::OK
    }
    fn on_control(&mut self, op: Control<'_>, slot: &mut LocalSlot) -> Progress {
        if let Control::Poll(n) = op {
            return self.tasks.poll(n);
        }
        if let Control::Event(Event::Network(NetEvent::Data(conn, bytes))) = op {
            if !self.tasks.has_capacity() {
                return Progress::idle(Status::BUSY);
            }
            return Progress::idle(
                match S::network_data(self.state.clone(), conn, bytes.as_slice()) {
                    Ok(task) => {
                        if let Some(f) = task {
                            self.tasks.insert(f);
                        }
                        Status::OK
                    }
                    Err(e) => e,
                },
            );
        }
        if let Control::Event(e) | Control::LifecycleEvent(_, e) = &op
            && let Some(status) = self.ctx.route_event(e, slot)
        {
            if status == Status::OK
                && let Control::LifecycleEvent(action, _) = op
            {
                // 事件已被 SDK 接纳；与停止竞争而失效的后续唤醒不能撤销这次转移。
                self.ctx.command(HostOp::LifecycleReady(action));
            }
            return Progress::idle(status);
        }
        let timer = match &op {
            Control::Event(Event::Timer(h)) | Control::LifecycleEvent(_, Event::Timer(h)) => {
                Some(*h)
            }
            _ => None,
        };
        let r = self.state.with(|s| s.control(op));
        if let Some(h) = timer
            && r.status == Status::OK
        {
            self.ctx.accepted_raw_tick(h);
        }
        r
    }
    fn on_cleanup(&mut self, e: Event<'_>, slot: &mut LocalSlot) -> Status {
        self.ctx
            .route_event(&e, slot)
            .unwrap_or_else(|| self.state.with(|s| s.cleanup(e)))
    }
    fn cancel(&mut self) {
        self.tasks.cancel();
        let waiters = std::mem::take(&mut *lock(&self.ctx.waiters));
        for (_, timer) in waiters.timers {
            timer.stop();
        }
        self.state.with(|s| s.cancel());
    }
}
