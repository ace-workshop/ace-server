use crate::{Context, Status, native::Progress};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context as TaskContext, Wake, Waker},
};

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
/// 插件内部拥有的、可迁移的业务片段；不允许跨回调保存借用或非 Send 状态。
/// ```compile_fail
/// use ace_server_sdk::Handler;
/// let local = std::rc::Rc::new(1);
/// let _: Handler = Box::pin(async move { std::future::pending::<()>().await; drop(local); });
/// ```
pub type Handler = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
#[derive(Default)]
struct Ready {
    live: BTreeSet<u64>,
    queue: VecDeque<u64>,
}
struct Signal {
    id: u64,
    ready: Weak<Mutex<Ready>>,
    ctx: Context,
}
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(r) = self.ready.upgrade() {
            let mut r = lock(&r);
            if r.live.contains(&self.id) && !r.queue.contains(&self.id) {
                r.queue.push_back(self.id);
                drop(r);
                self.ctx.wake();
            }
        }
    }
}
pub(crate) struct Tasks {
    ctx: Context,
    ready: Arc<Mutex<Ready>>,
    tasks: BTreeMap<u64, (Handler, Waker)>,
    next: u64,
}
impl Tasks {
    pub fn new(ctx: Context) -> Self {
        Self {
            ctx,
            ready: Arc::default(),
            tasks: BTreeMap::new(),
            next: 0,
        }
    }
    pub fn has_capacity(&self) -> bool {
        self.tasks.len() < (self.ctx.limits().task_slots.get() as usize) && self.next != u64::MAX
    }
    pub fn insert(&mut self, f: Handler) {
        debug_assert!(self.has_capacity());
        self.next += 1;
        let id = self.next;
        let w = Waker::from(Arc::new(Signal {
            id,
            ready: Arc::downgrade(&self.ready),
            ctx: self.ctx.clone(),
        }));
        {
            let mut r = lock(&self.ready);
            r.live.insert(id);
            r.queue.push_back(id);
        }
        self.tasks.insert(id, (f, w));
        self.ctx.wake();
    }
    pub fn poll(&mut self, n: u32) -> Progress {
        for _ in 0..n {
            let Some(id) = lock(&self.ready).queue.pop_front() else {
                break;
            };
            let (f, w) = self.tasks.get_mut(&id).expect("只有活任务能登记就绪");
            if f.as_mut().poll(&mut TaskContext::from_waker(w)).is_ready() {
                let mut r = lock(&self.ready);
                r.live.remove(&id);
                r.queue.retain(|&k| k != id);
                drop(r);
                self.tasks.remove(&id);
            }
        }
        Progress {
            status: Status::OK,
            more_ready: u8::from(!lock(&self.ready).queue.is_empty()),
            task_capacity: u8::from(self.has_capacity()),
        }
    }
    pub fn cancel(&mut self) {
        *lock(&self.ready) = Ready::default();
        self.tasks.clear();
    }
}
