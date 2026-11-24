//! 一次节点关闭共用一个单调时钟。独立的进程期限不依赖插件、账本锁或 Tokio worker 返回。
use super::*;
use crate::config::ShutdownConfig;
use nix::sys::signal::{SigSet, Signal};
use std::{io::Read, sync::mpsc, thread, time::Instant as Clock};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StopStage {
    Open,
    Closing,
    Fault,
}
pub(super) struct Shutdown {
    pub(super) stage: watch::Sender<StopStage>,
    pub(super) core: OnceLock<Weak<Core>>,
    start: OnceLock<Clock>,
    finished: Mutex<Option<mpsc::Sender<()>>>,
    cancel_after: Duration,
    timeout: Duration,
}
impl Shutdown {
    pub(super) fn new(config: &ShutdownConfig) -> Arc<Self> {
        Arc::new(Self {
            stage: watch::channel(StopStage::Open).0,
            core: OnceLock::new(),
            start: OnceLock::new(),
            finished: Mutex::new(None),
            cancel_after: Duration::from_millis(config.cancel_after_ms),
            timeout: Duration::from_millis(config.timeout_ms),
        })
    }
    fn request(&self) -> Clock {
        let start = *self.start.get_or_init(|| {
            let start = Clock::now();
            let (cancel, hard) = (start + self.cancel_after, start + self.timeout);
            let (tx, rx) = mpsc::channel();
            *lock(&self.finished) = Some(tx);
            self.stage.send_replace(StopStage::Closing);
            let events = self.stage.clone();
            thread::Builder::new()
                .name("shutdown-deadline".into())
                .spawn(move || {
                    let expired = |at: Clock| {
                        rx.recv_timeout(at.saturating_duration_since(Clock::now()))
                            == Err(mpsc::RecvTimeoutError::Timeout)
                    };
                    if expired(cancel) {
                        events.send_replace(StopStage::Fault);
                        if expired(hard) {
                            // 不取宿主/输出锁，不并发析构正在运行的 native 回调。
                            std::process::abort();
                        }
                    }
                })
                .unwrap_or_else(|_| std::process::abort());
            start
        });
        // 直接撤去接纳，不以同步 CLI 恰好走出初始化等待或标准输出返回为前提。
        if let Some(core) = self.core.get().and_then(Weak::upgrade) {
            lock(&core.state).close_entry();
        }
        start
    }
}
impl State {
    fn close_entry(&mut self) {
        if self.closing {
            return;
        }
        self.closing = true;
        self.published = false;
        for p in self.ports.values() {
            p.ready.notify_all();
        }
        for a in self.actors.values_mut() {
            a.ordinary = false;
            a.signal.notify_one();
        }
    }
}
impl Node {
    pub(super) fn finish(&mut self) -> Result<(), Status> {
        let Some(rt) = self.rt.take() else {
            return Ok(());
        };
        let ordered = self.started || *self.shutdown.stage.borrow() != StopStage::Open;
        let start = self.shutdown.request();
        let mut stage = self.shutdown.stage.subscribe();
        let (mut records, pending) = {
            let s = lock(&self.core.state);
            let records = s
                .actors
                .iter()
                .map(|(&key, a)| {
                    (
                        if ordered { a.stop_rank } else { 0 },
                        std::cmp::Reverse(key),
                        a.spec.alias.clone(),
                        a.phase.subscribe(),
                        a.budget.clone(),
                    )
                })
                .collect::<Vec<_>>();
            (records, s.pending.len())
        };
        records.sort_unstable_by_key(|r| (r.0, r.1));
        eprintln!(
            "shutdown entry=closed ready=false actors={} pending={pending}",
            records.len()
        );
        let mut forced = false;
        let mut faulted = false;
        for (_, std::cmp::Reverse(key), alias, rx, _) in &mut records {
            let name = alias.as_deref().unwrap_or("-");
            lock(&self.core.state).stop(*key, Status::STOPPED);
            eprintln!("shutdown stop alias={name:?} key={key}");
            let gone = |p: &Phase| matches!(p, Phase::Gone(_));
            let phase = rt
                .block_on(async {
                    if !forced {
                        tokio::select! {
                            biased;
                            _ = stage.wait_for(|p| *p == StopStage::Fault) => {},
                            r = rx.wait_for(gone) => return r.map(|p| *p),
                        }
                        forced = true;
                        eprintln!("shutdown deadline=fault cancel=all");
                        let mut s = lock(&self.core.state);
                        for k in s.actors.keys().copied().collect::<Vec<_>>() {
                            s.stop(k, Status::FAULTED);
                        }
                    }
                    rx.wait_for(gone).await.map(|p| *p)
                })
                .unwrap_or_else(|_| std::process::abort());
            faulted |= phase == Phase::Gone(Status::FAULTED);
            eprintln!("shutdown gone alias={name:?} key={key} phase={phase:?}");
        }
        // 所有实例的 Gone 都在 Cancel、回收和 destroy 之后。此时只可能剩管理端口的已完成出口。
        {
            let mut s = lock(&self.core.state);
            s.ports.clear();
            if !s.actors.is_empty()
                || !s.pending.is_empty()
                || !s.tickets.is_empty()
                || records.iter().any(|r| r.4.used() != 0)
            {
                std::process::abort();
            }
        }
        drop(rt);
        let forced = forced || start.elapsed() >= self.shutdown.cancel_after;
        eprintln!("shutdown runtime=closed pending=0 actors=0 forced={forced}");
        lock(&self.shutdown.finished).take();
        if forced {
            Err(Status::TIMEOUT)
        } else if faulted {
            Err(Status::FAULTED)
        } else {
            Ok(())
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if self.rt.is_none() {
            return;
        }
        // 管理对象意外在另一 Tokio 上析构时，不能嵌套 block_on 或从异步上下文 drop Runtime。
        let mut finish = || {
            if let Err(e) = self.finish() {
                eprintln!("节点析构：{e:?}");
            }
        };
        if Handle::try_current().is_ok() {
            thread::scope(|s| {
                s.spawn(finish)
                    .join()
                    .unwrap_or_else(|_| std::process::abort())
            });
        } else {
            finish();
        }
    }
}

/// 在专用 CLI 进程中装载、发布并运行清单，处理 SIGTERM；发布后也接受 stdin 字节或 EOF。
///
/// # Safety
/// 库目录与清单必须由部署方控制，所有库满足 [`Implementations::load_startup`] 的受信约束。
/// 必须从尚未创建其它线程的专用进程主线程调用；本命令在进程寿命内独占 SIGTERM、stdin，
/// 插件不得修改信号掩码或处置。硬期限到达而 native 代码仍不返回时会终止整个进程。
/// # Errors
/// 装载、配置、身份、初始化、信号或标准输入失败均报错；超过协作收尾期限不报告正常退出。
pub unsafe fn run_manifest(m: &Manifest, dir: &Path, ids: &Path) -> Result<(), StartError> {
    let clock = Shutdown::new(&m.shutdown);
    let mut signals = SigSet::empty();
    signals.add(Signal::SIGTERM);
    // 在 dlopen、身份 I/O 和 worker 创建之前屏蔽。随后所有线程继承，由一个普通线程 sigwait。
    signals
        .thread_block()
        .map_err(|e| StartError::Runtime(e.into()))?;
    let signal = clock.clone();
    thread::Builder::new()
        .name("sigterm".into())
        .spawn(move || {
            loop {
                match signals.wait() {
                    Ok(Signal::SIGTERM) => break,
                    Err(nix::errno::Errno::EINTR) => continue,
                    _ => std::process::abort(),
                }
            }
            signal.request();
            eprintln!("shutdown trigger=SIGTERM");
        })
        .map_err(StartError::Runtime)?;
    // SAFETY: 调用方保证启动目录和所有 native 库受信，装载后仍由唯一 driver 持有实例。
    let libs = unsafe { Implementations::load_startup(&m.library_paths(dir)) }
        .map_err(|e| StartError::Config(ConfigError::Library(e)))?;
    let mut node = Node::new(m, libs, ids, clock.clone())?;
    let mut stop = clock.stage.subscribe();
    let result = match node.start_manifest(m) {
        Ok(()) => {
            node.started = true;
            if *stop.borrow() != StopStage::Open {
                return node.finish().map_err(StartError::Instance);
            }
            println!(
                "清单实例已发布；SIGTERM 停止，stdin_stop={}",
                m.shutdown.stdin_stop
            );
            if !m.shutdown.stdin_stop {
                node.core
                    .rt
                    .block_on(stop.wait_for(|p| *p != StopStage::Open))
                    .unwrap_or_else(|_| std::process::abort());
                return node.finish().map_err(StartError::Instance);
            }
            // 不把阻塞的终端放进 Tokio blocking 池，否则关 runtime 仍会等它。只供此进程命令使用。
            let (tx, input) = tokio::sync::oneshot::channel();
            thread::Builder::new()
                .name("stdin-stop".into())
                .spawn(move || {
                    let _ = tx.send(std::io::stdin().read(&mut [0])); // SIGTERM 可能已释放接收方。
                })
                .map_err(StartError::Runtime)?;
            node.core.rt.block_on(async {
                tokio::select! {
                    biased;
                    _ = stop.wait_for(|p| *p != StopStage::Open) => Ok(()),
                    r = input => r.map_err(|_| StartError::Instance(Status::FAULTED))?.map(|_| ()).map_err(StartError::Runtime),
                }
            })
        }
        Err(Status::STOPPED) if *stop.borrow() != StopStage::Open => {
            eprintln!("shutdown startup=interrupted");
            Ok(())
        }
        Err(e) => Err(StartError::Instance(e)),
    };
    node.finish().map_err(StartError::Instance)?;
    result
}
