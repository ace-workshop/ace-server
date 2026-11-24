//! 本机跨库回归驱动：只认识信封和声明，不链接业务类型或 codec。
use abi_stable::{
    library::lib_header_from_path,
    std_types::{
        ROption,
        ROption::{RNone, RSome},
    },
};
use ace_fixture_interface::{Admission, EndpointBox, FixtureRef, Packet, Path as Route, Step};
use ace_fixture_interface::{ReplyMode, RouteScope};
use std::{
    env,
    error::Error,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
};

fn load(path: &Path) -> Result<FixtureRef, Box<dyn Error>> {
    // 使用每个文件自己的 LibHeader；RootModule::load_from_file 会按类型复用首个根。
    let header = lib_header_from_path(path)?;
    if header.layout().is_none() {
        return Err("缺少根布局信息".into());
    }
    Ok(header.init_root_module::<FixtureRef>()?)
}
struct Lease(Arc<AtomicUsize>, usize);
impl Drop for Lease {
    fn drop(&mut self) {
        assert!(self.0.fetch_sub(self.1, SeqCst) >= self.1);
    }
}
fn charge(packet: &mut ROption<Packet>, ledger: &Arc<AtomicUsize>, limit: usize) -> bool {
    if let RSome(Packet::Local(p)) = packet {
        let n = p.owned_bytes();
        if ledger
            .fetch_update(SeqCst, SeqCst, |x| x.checked_add(n).filter(|&v| v <= limit))
            .is_err()
        {
            return false;
        }
        p.attach_lease(Lease(Arc::clone(ledger), n))
            .expect("一次预留");
        assert!(n >= 128 + 64 * 4, "按容量而不是 len 计费");
    }
    true
}
fn run(
    mut who: EndpointBox,
    slot: &mut ROption<Packet>,
    reply: ROption<Route>,
) -> (EndpointBox, u64, ROption<Packet>) {
    assert_eq!(who.admit(slot, reply, Admission::Accept), 0);
    assert!(slot.is_none());
    assert!(matches!(who.poll_one(), Step::Pending));
    // 串行迁移，不依赖调度器恰好换线程；Future/Waker 只在插件内创建和 poll。
    std::thread::spawn(move || {
        let Step::Done(n, result) = who.poll_one() else {
            panic!("第二次 poll 未完成")
        };
        (who, n, result)
    })
    .join()
    .expect("独占实例可迁移")
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("用法：ace-abi-smoke <caller库> <echo库>".into());
    }
    let a = load(Path::new(&args[0]))?;
    let b = load(Path::new(&args[1]))?;
    assert_ne!(a.name(), b.name());
    let (sa, sb) = ((a.shape())(false), (b.shape())(false));
    let mixed = sa
        .select(&(b.shape())(true), RouteScope::Local, ReplyMode::Typed)
        .expect("布局不同但 wire 匹配");
    assert_eq!(mixed, [Route::LocalOwned, Route::Bytes]);
    assert_eq!(
        sa.select(&sb, RouteScope::Local, ReplyMode::RawBytes),
        Ok(mixed)
    );
    let mut incompatible = sb;
    incompatible.wire.response[31] ^= 1;
    assert!(
        sa.select(&incompatible, RouteScope::Local, ReplyMode::Typed)
            .is_err()
    );
    let q = Arc::new(AtomicUsize::new(0));
    for (paths, counts) in [
        (
            sa.select(&sb, RouteScope::Local, ReplyMode::Typed).unwrap(),
            (0, 0),
        ),
        (mixed, (0, 1)),
        (
            sa.select(&sb, RouteScope::Network, ReplyMode::Typed)
                .unwrap(),
            (1, 1),
        ),
    ] {
        let (prior_a, prior_b) = ((a.counts())(), (b.counts())());
        let mut caller = (a.create())();
        let mut req = caller.request(paths[0], 41);
        assert!(charge(&mut req, &q, 1024));
        let (_, n, mut response) = run((b.create())(), &mut req, RSome(paths[1]));
        assert_eq!(n, 56);
        assert_eq!(q.load(SeqCst), 0);
        if paths[1] == Route::LocalOwned {
            assert!(!charge(&mut response, &q, 16));
            assert_eq!(q.load(SeqCst), 0);
        }
        assert!(charge(&mut response, &q, 1024));
        let (_, n, last) = run(caller, &mut response, RNone);
        assert_eq!(n, 71);
        assert!(last.is_none());
        assert_eq!(q.load(SeqCst), 0);
        let (x, y) = ((a.counts())(), (b.counts())());
        assert_eq!(
            (x.encode - prior_a.encode, y.decode - prior_b.decode),
            (counts.0, counts.0)
        );
        assert_eq!(
            (y.encode - prior_b.encode, x.decode - prior_a.decode),
            (counts.1, counts.1)
        );
        assert_eq!(
            x.released - prior_a.released,
            usize::from(paths[0] == Route::LocalOwned)
        );
        assert_eq!(
            y.released - prior_b.released,
            usize::from(paths[1] == Route::LocalOwned)
        );
        println!(
            "request={:?} reply={:?} actual codec (encode,decode)={:?}/{:?}",
            paths[0],
            paths[1],
            (counts.0, counts.0),
            (counts.1, counts.1)
        );
    }
    let before = (a.counts())();
    let mut sender = (a.create())();
    let mut recv = (b.create())();
    let mut slot = sender.request(Route::LocalOwned, 9);
    assert!(charge(&mut slot, &q, 1024));
    assert_eq!(recv.admit(&mut slot, RNone, Admission::Busy), 1);
    assert!(slot.is_some());
    assert_eq!(recv.admit(&mut slot, RNone, Admission::Accept), 0);
    assert!(slot.is_none());
    assert!(matches!(recv.poll_one(), Step::Pending));
    drop(sender);
    assert_eq!((a.counts())().destroyed, before.destroyed + 1);
    assert_eq!((a.counts())().released, before.released);
    assert!(q.load(SeqCst) > 0, "销毁发送方不能释放接收方任务的持有额度");
    std::thread::spawn(move || assert!(matches!(recv.poll_one(), Step::Done(24, RNone))))
        .join()
        .unwrap();
    assert_eq!((a.counts())().released, before.released + 1);
    assert_eq!(q.load(SeqCst), 0);
    let mut sender = (a.create())();
    let mut recv = (b.create())();
    let mut slot = sender.request(Route::LocalOwned, 0);
    assert!(charge(&mut slot, &q, 1024));
    assert_eq!(recv.admit(&mut slot, RNone, Admission::PanicAfterTake), 3);
    assert!(slot.is_none());
    assert_eq!((a.counts())().released, before.released + 2);
    assert_eq!(q.load(SeqCst), 0);
    println!(
        "两独立库：Busy 前未取走、panic 后已消费、跨 Pending/线程与发送实例销毁、逐对象析构/额度归零通过"
    );
    Ok(())
}
