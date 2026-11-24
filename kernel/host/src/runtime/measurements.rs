//! 仅供 tools/measure.py 启动的有限 release 实测；不把共享 runner 的时间当断言。
use super::*;
use nix::{
    sys::time::TimeValLike,
    time::{ClockId, clock_gettime},
};
use std::{env, path::PathBuf, sync::Barrier, thread, time::Instant as Clock};
use tokio::{runtime::RuntimeMetrics, sync::mpsc};

const PRODUCERS: usize = 4;
const BATCH: usize = 8;
const PER_PRODUCER: usize = 8192;
fn cpu_ns() -> i64 {
    clock_gettime(ClockId::CLOCK_PROCESS_CPUTIME_ID)
        .unwrap()
        .num_nanoseconds()
}
struct Window {
    start: Clock,
    cpu: i64,
    busy: Vec<Duration>,
    metrics: RuntimeMetrics,
}
impl Window {
    fn start(m: RuntimeMetrics) -> Self {
        Self {
            busy: (0..m.num_workers())
                .map(|i| m.worker_total_busy_duration(i))
                .collect(),
            metrics: m,
            cpu: cpu_ns(),
            start: Clock::now(),
        }
    }
    fn finish(self, mut latencies: Vec<Duration>) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let cpu = (cpu_ns() - self.cpu) as f64 / 1e9;
        let busy = self
            .busy
            .iter()
            .enumerate()
            .map(|(i, &b)| {
                format!(
                    "{:.1}",
                    (self.metrics.worker_total_busy_duration(i) - b).as_secs_f64() * 100. / elapsed
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        latencies.sort_unstable();
        let n = latencies.len();
        let us = |p: usize| latencies[(n * p).div_ceil(100usize) - 1].as_secs_f64() * 1e6;
        println!(
            "measurement samples={n} elapsed_s={elapsed:.6} delivered_s={:.1} p95_us={:.3} p99_us={:.3} process_cpu_s={cpu:.6} process_cpu_pct={:.1} worker_busy_pct={busy}",
            n as f64 / elapsed,
            us(95usize),
            us(99usize),
            cpu * 100. / elapsed
        );
    }
}
fn submit(p: &ReplyPort, dst: ServiceAddress, wire: WireContract, session: u64, word: u64) {
    p.request(InboundCall {
        source: ServiceAddress::try_from((2u64 << 48) + 1).unwrap(),
        destination: dst,
        session,
        wire,
        bytes: &word.to_le_bytes(),
        max_response_bytes: 8,
        timeout_ms: 2000,
    })
    .unwrap();
}
fn result(p: &ReplyPort, wire: WireContract, dst: ServiceAddress) -> (usize, u64) {
    let r = p.recv(Duration::from_secs(4)).unwrap();
    assert_eq!(
        (r.source, r.destination, r.wire),
        (dst.bits(), (2u64 << 48) + 1, wire)
    );
    (
        r.session as usize,
        u64::from_le_bytes(r.body.unwrap().as_ref().try_into().unwrap()),
    )
}
#[test]
#[ignore = "tools/measure.py 分别装载实际 release caller/echo，独立进程共用一个热身和测量窗口"]
fn actor_rpcs() {
    let dir = PathBuf::from(env::var_os("ACE_MEASURE_DIR").unwrap());
    let m = Manifest::read(&dir.join("node.toml")).unwrap();
    let lib = PathBuf::from(env::var_os("ACE_RUNTIME_LIBS").unwrap());
    // SAFETY: 测量入口只传同一 workspace 独立构建的受信夹具，未引入旁路路由器。
    let libs = unsafe { Implementations::load_startup(&m.library_paths(&lib)) }.unwrap();
    let wire = libs
        .descriptors()
        .find(|d| d.implementation == "test.echo")
        .unwrap()
        .methods
        .as_slice()[0]
        .wire;
    let ids = dir.join("identity");
    crate::identity::provision_new(&ids, m.node).unwrap();
    let node = Node::start(&m, libs, &ids).unwrap();
    let single = m.instances.len() == 1;
    let shape = env::var("ACE_LOAD").unwrap();
    let caller = |i| {
        node.resolve(&if single {
            "echo0".into()
        } else {
            format!("caller{i}")
        })
        .unwrap()
    };
    let echo = |i| {
        node.resolve(&format!("echo{}", if shape == "uniform" { i } else { 0 }))
            .unwrap()
    };
    let counts = |addr| {
        let p = node.reply_port(2, 1, 8).unwrap();
        [49, 50, 51].map(|op| {
            submit(&p, addr, wire, op, op);
            result(&p, wire, addr).1
        })
    };
    let barrier = Barrier::new(PRODUCERS + 1);
    let total = PRODUCERS * PER_PRODUCER;
    let (latencies, window, peaks) = thread::scope(|scope| {
        let mut tasks = Vec::new();
        for lane in 0..PRODUCERS {
            let (src, to) = (caller(lane), echo(lane));
            let p = node.reply_port(2, BATCH, BATCH * 8).unwrap();
            let node = &node;
            let barrier = &barrier;
            tasks.push(scope.spawn(move || {
                let mut lat = Vec::with_capacity(PER_PRODUCER);
                for session in 1..=BATCH {
                    submit(&p, src, wire, session as u64, to.bits());
                }
                for _ in 0..BATCH {
                    assert_eq!(result(&p, wire, src).1, 42);
                }
                barrier.wait();
                barrier.wait();
                let mut max_queue = 0;
                let mut max_pending = 0;
                for base in (0..PER_PRODUCER).step_by(BATCH) {
                    let mut starts = [None; BATCH];
                    for (i, time) in starts.iter_mut().enumerate() {
                        *time = Some(Clock::now());
                        submit(&p, src, wire, (base + i + 1) as u64, to.bits());
                    }
                    if base % (BATCH * 32) == 0 {
                        let s = lock(&node.core.state);
                        max_queue =
                            max_queue.max(s.actors.values().map(|a| a.queued).sum::<usize>());
                        max_pending = max_pending.max(s.pending.len());
                    }
                    for _ in 0..BATCH {
                        let (id, value) = result(&p, wire, src);
                        assert_eq!(value, 42);
                        lat.push(starts[id - 1 - base].take().unwrap().elapsed());
                    }
                }
                (lat, max_queue, max_pending)
            }));
        }
        barrier.wait();
        let w = Window::start(node.core.rt.metrics());
        barrier.wait();
        let mut samples = Vec::with_capacity(total);
        let mut peaks = (0, 0);
        for t in tasks {
            let (mut times, q, p) = t.join().unwrap();
            samples.append(&mut times);
            peaks = (peaks.0.max(q), peaks.1.max(p));
        }
        (samples, w, peaks)
    });
    assert_eq!(latencies.len(), total);
    window.finish(latencies);
    let c = counts(caller(0));
    let e = counts(echo(0));
    let calls = (total + BATCH * PRODUCERS) as u64;
    let expected = if m.instances[0].config == "measure-local" {
        0
    } else if single {
        2 * calls
    } else {
        calls
    };
    assert_eq!(
        (c[0], c[1], e[0], e[1]),
        (expected, expected, expected, expected)
    );
    assert_eq!(node.usage(), (m.instances.len(), 0, 0));
    println!(
        "rpc actors={} workers={} producers={PRODUCERS} batch={BATCH} warmup={} observed_queue_peak={} observed_pending_peak={} caller_counts_and_last_bytes={c:?} echo_counts_and_last_bytes={e:?} single_library={single}",
        m.instances.len(),
        m.budgets.workers,
        BATCH * PRODUCERS,
        peaks.0,
        peaks.1
    );
}

const CAPACITY: usize = 32;
const BURSTS: usize = 50;
const BURST: usize = 256;
struct Packet {
    at: Clock,
    lane: usize,
    seq: usize,
    payload: [u8; 32],
}
struct Ring {
    slots: ArrayQueue<Packet>,
    data: Notify,
    space: Notify,
}
#[derive(Clone)]
enum Sender {
    Ring(Arc<Ring>),
    Channel(mpsc::Sender<Packet>),
}
enum Receiver {
    Ring(Arc<Ring>),
    Channel(mpsc::Receiver<Packet>),
}
impl Sender {
    // 初次 Full 不覆盖：保留同一对象等实际空间，不在 worker 上忙试。
    async fn send(&self, mut packet: Packet) -> (usize, usize) {
        match self {
            Self::Ring(q) => {
                let mut full = 0;
                loop {
                    let space = q.space.notified();
                    tokio::pin!(space);
                    space.as_mut().enable();
                    match q.slots.push(packet) {
                        Ok(()) => {
                            let len = q.slots.len();
                            q.data.notify_one();
                            return (full, len);
                        }
                        Err(back) => {
                            packet = back;
                            full += 1;
                            space.await;
                        }
                    }
                }
            }
            Self::Channel(tx) => {
                let mut full = 0;
                match tx.try_send(packet) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(back)) => {
                        full += 1;
                        tx.reserve().await.unwrap().send(back);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => panic!("消费方提前结束"),
                }
                (full, CAPACITY - tx.capacity())
            }
        }
    }
}
impl Receiver {
    async fn recv(&mut self) -> Packet {
        match self {
            Self::Ring(q) => loop {
                if let Some(p) = q.slots.pop() {
                    q.space.notify_waiters();
                    return p;
                }
                q.data.notified().await;
            },
            Self::Channel(rx) => rx.recv().await.unwrap(),
        }
    }
}
#[test]
#[ignore = "tools/measure.py：仅候选 mailbox 后端，不含 ABI/RPC 或生产额度"]
fn mailbox_backends() {
    let workers = env::var("ACE_WORKERS").unwrap().parse::<usize>().unwrap();
    let actors = if env::var("ACE_LOAD").unwrap() == "hot" {
        1
    } else {
        PRODUCERS
    };
    let ring = env::var("ACE_BACKEND").unwrap() == "array";
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_time()
        .build()
        .unwrap();
    let window = Window::start(rt.metrics());
    let (times, full, peak, last_us) = rt.block_on(async {
        let start = Instant::now();
        let end = start + Duration::from_secs(1);
        let mut consumers = vec![];
        let mut producers = vec![];
        for actor in 0..actors {
            let (tx, mut rx) = if ring {
                let q = Arc::new(Ring {
                    slots: ArrayQueue::new(CAPACITY),
                    data: Notify::new(),
                    space: Notify::new(),
                });
                (Sender::Ring(q.clone()), Receiver::Ring(q))
            } else {
                let (tx, rx) = mpsc::channel(CAPACITY);
                (Sender::Channel(tx), Receiver::Channel(rx))
            };
            consumers.push(tokio::spawn(async move {
                let n = PRODUCERS * BURSTS * BURST / actors;
                let mut lat = Vec::with_capacity(n);
                let mut next = [0; PRODUCERS];
                for _ in 0..n {
                    let p = rx.recv().await;
                    lat.push(p.at.elapsed());
                    assert_eq!(
                        (p.lane % actors, p.seq, p.payload),
                        (actor, next[p.lane], [p.lane as u8; 32])
                    );
                    next[p.lane] += 1;
                }
                (lat, start.elapsed().as_micros())
            }));
            for lane in (actor..PRODUCERS).step_by(actors) {
                let tx = tx.clone();
                producers.push(tokio::spawn(async move {
                    let mut full = 0;
                    let mut peak = 0;
                    for burst in 0..BURSTS {
                        tokio::time::sleep_until(start + Duration::from_millis(20 * burst as u64))
                            .await;
                        for i in 0..BURST {
                            let (f, len) = tx
                                .send(Packet {
                                    at: Clock::now(),
                                    lane,
                                    seq: burst * BURST + i,
                                    payload: [lane as u8; 32],
                                })
                                .await;
                            full += f;
                            peak = peak.max(len);
                        }
                    }
                    (full, peak)
                }));
            }
        }
        let mut times = Vec::with_capacity(PRODUCERS * BURSTS * BURST);
        let mut last = 0;
        for c in consumers {
            let (mut lat, at) = c.await.unwrap();
            times.append(&mut lat);
            last = last.max(at);
        }
        let mut full = 0;
        let mut peak = 0;
        for p in producers {
            let (f, l) = p.await.unwrap();
            full += f;
            peak = peak.max(l);
        }
        // 对照同一 offered 时间窗。如果系统过载则保留实际 drain 超时，不截断最后一批。
        tokio::time::sleep_until(end).await;
        (times, full, peak, last)
    });
    assert_eq!(times.len(), PRODUCERS * BURSTS * BURST);
    println!(
        "backend actors={actors} workers={workers} producers={PRODUCERS} capacity={CAPACITY} payload_bytes=32 envelope_bytes={} window_ms=1000 last_delivery_us={last_us} full_observations={full} sampled_peak={peak}",
        size_of::<Packet>()
    );
    window.finish(times);
}
