//! 两个独立 cdylib 分别编译的夹具业务；宿主测试不依赖本包。
use abi_stable::{
    StableAbi,
    prefix_type::PrefixTypeTrait,
    std_types::{
        ROption,
        ROption::{RNone, RSome},
        RStr, RString, RVec,
    },
    type_level::downcasting::TD_Opaque,
};
#[cfg(feature = "codec_ace")]
use ace_codec::AceCodec as Selected;
use ace_codec::{Codec, Format, Limits};
#[cfg(not(feature = "codec_ace"))]
use ace_fixture_codec::FixedCodec as Selected;
use ace_fixture_codec::PeerSample;
use ace_fixture_interface::{
    Admission, Counts, Endpoint, EndpointBox, Fixture, FixtureRef, Packet, Path, Shape, Step,
};
use ace_server_local::{LocalOwned, LocalType, StableMessage};
use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
    task::{Context, Poll, Waker},
};

static ENCODE: AtomicUsize = AtomicUsize::new(0);
static DECODE: AtomicUsize = AtomicUsize::new(0);
static RELEASED: AtomicUsize = AtomicUsize::new(0);
static DESTROYED: AtomicUsize = AtomicUsize::new(0);

#[repr(C)]
#[derive(StableAbi)]
struct Sample {
    id: u64,
    text: RString,
    samples: RVec<u32>,
}
impl Drop for Sample {
    fn drop(&mut self) {
        RELEASED.fetch_add(1, Relaxed);
    }
}
// SAFETY: 所有字段是独立拥有的稳定值，析构只更新常驻模块计数，绝不访问实例。
unsafe impl StableMessage for Sample {
    const KEY: [u8; 32] = *b"ace.fixture.Sample.local/v1.....";
    fn heap_bytes(&self) -> Option<usize> {
        self.samples
            .capacity()
            .checked_mul(size_of::<u32>())?
            .checked_add(self.text.capacity())
    }
}
// 同尺寸、同对齐、同声称的语义键，实际字段布局不同。只作声明，不构造对象。
#[repr(C)]
#[derive(StableAbi)]
struct Changed {
    id: i64,
    text: RString,
    samples: RVec<u32>,
}
// SAFETY: 字段自身皆为稳定的拥有值；本类型仅用于挑战布局检查，绝不构造/发送。
unsafe impl StableMessage for Changed {
    const KEY: [u8; 32] = Sample::KEY;
    fn heap_bytes(&self) -> Option<usize> {
        self.samples
            .capacity()
            .checked_mul(4)?
            .checked_add(self.text.capacity())
    }
}

fn make(path: Path, id: u64) -> Option<Packet> {
    let text = "跨库";
    let samples = [2, 5, 8];
    match path {
        Path::LocalOwned => {
            // 保留远大于 len 的容量，用于识别“只按字节长度”记账的错误。
            let mut s = RString::with_capacity(128);
            s.push_str(text);
            let mut v = RVec::with_capacity(64);
            v.extend_from_slice(&samples);
            LocalOwned::new(
                Sample {
                    id,
                    text: s,
                    samples: v,
                },
                1024,
            )
            .ok()
            .map(Packet::Local)
        }
        Path::Bytes => {
            let mut dst = [0u8; 1024];
            let mut scratch = [0u8; 2048];
            ENCODE.fetch_add(1, Relaxed);
            let n = Selected::encode_into(
                &PeerSample {
                    id,
                    text: text.into(),
                    samples: samples.into(),
                },
                &mut dst,
                &mut scratch,
                limits(),
            )
            .ok()?;
            Some(Packet::Bytes(dst[..n].to_vec().into()))
        }
    }
}
fn limits() -> Limits {
    Limits {
        wire_bytes: 1024,
        decoded_bytes: 2048,
        items: 64,
        ..Limits::default()
    }
}
fn observed(id: u64, text: &str, samples: &[u32]) -> Option<u64> {
    if text != "跨库" || samples != [2, 5, 8] {
        return None;
    }
    samples
        .iter()
        .try_fold(id, |v, n| v.checked_add(u64::from(*n)))
}
fn read(packet: &Packet) -> Option<u64> {
    match packet {
        Packet::Local(p) => {
            let x = p.view::<Sample>().ok()?;
            observed(x.id, &x.text, &x.samples)
        }
        Packet::Bytes(p) => {
            DECODE.fetch_add(1, Relaxed);
            let x: PeerSample = Selected::decode(p, limits()).ok()?;
            observed(x.id, &x.text, &x.samples)
        }
    }
}
type Task = Pin<Box<dyn Future<Output = Step> + Send>>;
struct Service {
    task: Option<Task>,
}
impl Drop for Service {
    fn drop(&mut self) {
        DESTROYED.fetch_add(1, Relaxed);
    }
}
impl Endpoint for Service {
    fn request(&mut self, path: Path, seed: u64) -> ROption<Packet> {
        make(path, seed).into()
    }
    fn admit(&mut self, slot: &mut ROption<Packet>, reply: ROption<Path>, fault: Admission) -> u32 {
        catch_unwind(AssertUnwindSafe(|| {
            if self.task.is_some() || fault == Admission::Busy {
                return 1;
            }
            let RSome(packet) = std::mem::replace(slot, RNone) else {
                return 2;
            };
            if fault == Admission::PanicAfterTake {
                panic!("夹具：已取走输入")
            }
            self.task = Some(Box::pin(async move {
                let mut first = true;
                poll_fn(|cx| {
                    if std::mem::take(&mut first) {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        Poll::Ready(())
                    }
                })
                .await;
                let Some(n) = read(&packet) else {
                    return Step::Failed;
                };
                let result = match reply {
                    RNone => RNone,
                    RSome(path) => {
                        let Some(p) = make(path, n) else {
                            return Step::Failed;
                        };
                        RSome(p)
                    }
                };
                Step::Done(n, result)
            }));
            0
        }))
        .unwrap_or(3)
    }
    fn poll_one(&mut self) -> Step {
        let Some(task) = self.task.as_mut() else {
            return Step::Failed;
        };
        match catch_unwind(AssertUnwindSafe(|| {
            task.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        })) {
            Ok(Poll::Pending) => Step::Pending,
            Ok(Poll::Ready(s)) => {
                self.task = None;
                s
            }
            Err(_) => {
                self.task = None;
                Step::Failed
            }
        }
    }
}
extern "C" fn create() -> EndpointBox {
    EndpointBox::from_value(Service { task: None }, TD_Opaque)
}
extern "C" fn shape(drift: bool) -> Shape {
    // 夹具 wire 两方向都复用同一契约描述。正式方法注册不依赖 Rust 类型名。
    Shape {
        wire: ace_fixture_interface::WireContract {
            key: 71,
            codec: Selected::ID,
            request: WIRE_DIGEST,
            response: WIRE_DIGEST,
        },
        directions: 3,
        client_callable: 0,
        peer_control: ace_fixture_interface::PeerAccess::None,
        max_reply_bytes: 1024,
        max_reply_local_bytes: 1024,
        request_local: RSome(LocalType::of::<Sample>()),
        response_local: RSome(if drift {
            LocalType::of::<Changed>()
        } else {
            LocalType::of::<Sample>()
        }),
    }
}
extern "C" fn counts() -> Counts {
    Counts {
        encode: ENCODE.load(Relaxed),
        decode: DECODE.load(Relaxed),
        released: RELEASED.load(Relaxed),
        destroyed: DESTROYED.load(Relaxed),
    }
}
/// 由各 cdylib 在上游完成版本与全表布局核验后返回自己的入口。
pub fn module(name: &'static str) -> FixtureRef {
    Fixture {
        name: RStr::from_str(name),
        create,
        shape,
        counts,
    }
    .leak_into_prefix()
}

const WIRE_DIGEST: [u8; 32] = [
    0x31, 0x2b, 0xdc, 0x70, 0xde, 0xf9, 0x65, 0xf3, 0xbf, 0x24, 0xd9, 0x95, 0x60, 0x02, 0x8b, 0x62,
    0xb9, 0x3d, 0x90, 0x6d, 0x4c, 0xf7, 0xa3, 0x0d, 0xed, 0xa4, 0x71, 0x8f, 0xdb, 0x7c, 0x1d, 0x7f,
];

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    #[test]
    fn fixture_wire_matches_canonical_text() {
        assert_eq!(
            &Sha256::digest(include_bytes!("../wire.txt"))[..],
            WIRE_DIGEST
        );
        assert_eq!(
            (size_of::<Sample>(), align_of::<Sample>()),
            (size_of::<Changed>(), align_of::<Changed>())
        );
    }
}
pub mod runtime;
