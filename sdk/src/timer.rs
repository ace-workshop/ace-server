use crate::{
    Context, Status,
    native::{HostOp, HostResult, ResourceHandle},
    tasks::lock,
};
use std::{
    future::poll_fn,
    sync::{Arc, Mutex, Weak},
    task::{Poll, Waker},
};
#[derive(Default)]
pub(crate) struct Tick {
    ready: bool,
    ended: bool,
    waker: Option<Waker>,
}
pub(crate) struct TimerSlot {
    generation: u64,
    tick: Option<Weak<Mutex<Tick>>>,
    periodic: bool,
}
/// 可取消的单次或周期 timer；同一 timer 未消费的 tick 合并，不积压周期数。
pub struct Timer {
    ctx: Context,
    handle: ResourceHandle,
    tick: Arc<Mutex<Tick>>,
}
impl Timer {
    /// 等一个 tick。单次已消费或 timer 已停止时返回 Stopped。
    /// # Errors
    /// 已经消费的单次 timer 不能再次等待。
    pub async fn tick(&mut self) -> Result<(), Status> {
        poll_fn(|cx| {
            let mut t = lock(&self.tick);
            if std::mem::take(&mut t.ready) {
                Poll::Ready(Ok(()))
            } else if t.ended {
                Poll::Ready(Err(Status::STOPPED))
            } else {
                t.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
    /// 按创建时的代次取消；等价于 drop，不取消其它 timer。
    pub fn cancel(self) {}
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.ctx.command(HostOp::TimerCancel(self.handle));
    }
}
impl Context {
    /// 启动本实例的相对 timer；周期毫秒为零表示单次，需要使用 Actor wrapper。
    /// # Errors
    /// 阶段、间隔或独立 timer 槽不满足时直接失败。
    pub fn timer(&self, delay_ms: u64, period_ms: u64) -> Result<Timer, Status> {
        if !self.managed {
            return Err(Status::NOT_READY);
        }
        let tick = Arc::default();
        let r = self.set_timer(delay_ms, period_ms, Some(Arc::downgrade(&tick)));
        if r.status != Status::OK {
            return Err(r.status);
        }
        let handle = ResourceHandle {
            id: r.value,
            generation: r.generation,
        };
        Ok(Timer {
            ctx: self.clone(),
            handle,
            tick,
        })
    }
    pub(crate) fn set_timer(
        &self,
        delay: u64,
        period: u64,
        tick: Option<Weak<Mutex<Tick>>>,
    ) -> HostResult {
        let mut book = lock(&self.waiters);
        let r = (self.api.invoke)(
            self.key,
            HostOp::TimerSet(delay, period),
            &mut abi_stable::std_types::ROption::RNone,
        );
        if r.status == Status::OK {
            book.timers.insert(
                r.value,
                TimerSlot {
                    generation: r.generation,
                    tick,
                    periodic: period != 0,
                },
            );
        }
        r
    }
    pub(crate) fn cancel_timer(&self, h: ResourceHandle) -> HostResult {
        let mut book = lock(&self.waiters);
        let r = (self.api.invoke)(
            self.key,
            HostOp::TimerCancel(h),
            &mut abi_stable::std_types::ROption::RNone,
        );
        let removed = if matches!(r.status, Status::OK | Status::NOT_FOUND | Status::STOPPED)
            && book
                .timers
                .get(&h.id)
                .is_some_and(|s| s.generation == h.generation)
        {
            book.timers.remove(&h.id)
        } else {
            None
        };
        drop(book);
        if let Some(s) = removed {
            s.stop();
        }
        r
    }
    pub(crate) fn accepted_raw_tick(&self, h: ResourceHandle) {
        let mut book = lock(&self.waiters);
        if book
            .timers
            .get(&h.id)
            .is_some_and(|t| t.generation == h.generation && !t.periodic && t.tick.is_none())
        {
            book.timers.remove(&h.id);
        }
    }
    pub(crate) fn tick_timer(&self, h: ResourceHandle) -> Option<Status> {
        let mut book = lock(&self.waiters);
        let Some(slot) = book.timers.get(&h.id) else {
            return Some(Status::NOT_FOUND);
        };
        if slot.generation != h.generation {
            return Some(Status::NOT_FOUND);
        }
        // raw timer 的接受点在业务 control 返回 OK，而不在路由到该回调时。
        let tick = slot.tick.clone()?;
        let periodic = slot.periodic;
        if !periodic {
            book.timers.remove(&h.id);
        }
        drop(book);
        // 无后台 Future 的原生 timer 仍走业务 control，但只有本实例登记的代次可以交付。
        let Some(tick) = tick.upgrade() else {
            return Some(Status::NOT_FOUND);
        };
        let mut t = lock(&tick);
        t.ready = true;
        t.ended = !periodic;
        let w = t.waker.take();
        drop(t);
        if let Some(w) = w {
            w.wake()
        }
        Some(Status::OK)
    }
}

impl TimerSlot {
    pub(crate) fn stop(self) {
        if let Some(t) = self.tick.and_then(|w| w.upgrade()) {
            let mut t = lock(&t);
            t.ready = false;
            t.ended = true;
            let w = t.waker.take();
            drop(t);
            if let Some(w) = w {
                w.wake();
            }
        }
    }
}
