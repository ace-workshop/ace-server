//! 互通夹具：以定宽小端字段和 u32 长度前缀表达选定的 DTO。不依赖 ace trait 或另一个序列化库。
use ace_codec::{Codec, CodecError, Format, Limits};
use ace_example_protocol::{Admission, Echo, EchoReply};
use std::io::Write;

/// 测试部署选用的格式；不会自动回退到默认 codec。
pub struct FixedCodec;
impl Format for FixedCodec {
    const ID: [u32; 3] = [0xf17, 1, 0];
}

type Result<T> = std::result::Result<T, CodecError>;
// 仅本夹具的消息形状，不是通用 Rust 对象序列化接口。
trait Record: Sized {
    const DEPTH: usize;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()>;
    fn read(r: &mut Input<'_>) -> Result<Self>;
}
// 每次受测 host 自有目录，文件名只取夹具的静态 DTO 类型。不同动态库各自以
// O_APPEND 打开本机普通文件；一次入口只追加一个字节，不会覆盖或拆成两段。
fn trace<T: Record>(entry: u8) {
    use std::{fs::OpenOptions, path::PathBuf, sync::OnceLock};
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    if let Some(dir) =
        DIR.get_or_init(|| std::env::var_os("ACE_FIXTURE_CODEC_TRACE_DIR").map(PathBuf::from))
    {
        let path = dir.join(std::any::type_name::<T>());
        if let Err(e) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut file| file.write_all(&[entry]))
        {
            // 不能把采集失败算作业务编解码错误，或留下看似有效的零次调用。
            eprintln!("无法写入固定 codec 夹具记录 {}: {e}", path.display());
            std::process::exit(1);
        }
    }
}
impl<T: Record> Codec<T> for FixedCodec {
    fn encode_into(v: &T, dst: &mut [u8], _: &mut [u8], l: Limits) -> Result<usize> {
        trace::<T>(b'E');
        if l.depth < T::DEPTH {
            return Err(CodecError::Limit);
        }
        let cap = l.wire_bytes.min(dst.len());
        let mut w = &mut dst[..cap];
        v.write(&mut w, l)?;
        Ok(cap - w.len())
    }
    fn decode(src: &[u8], l: Limits) -> Result<T> {
        trace::<T>(b'D');
        if src.len() > l.wire_bytes || l.depth < T::DEPTH {
            return Err(CodecError::Limit);
        }
        let mut r = Input {
            src,
            items: l.items,
            remaining: l.decoded_bytes,
        };
        r.charge(size_of::<T>())?;
        let t = T::read(&mut r)?;
        if !r.src.is_empty() {
            return Err(CodecError::Invalid);
        }
        Ok(t)
    }
}
fn put(w: &mut &mut [u8], bytes: &[u8]) -> Result<()> {
    w.write_all(bytes).map_err(|_| CodecError::Limit)
}
fn count(w: &mut &mut [u8], n: usize, l: Limits) -> Result<()> {
    if n > l.items {
        return Err(CodecError::Limit);
    }
    put(
        w,
        &u32::try_from(n)
            .map_err(|_| CodecError::Limit)?
            .to_le_bytes(),
    )
}
fn blob(w: &mut &mut [u8], b: &[u8], l: Limits) -> Result<()> {
    count(w, b.len(), l)?;
    put(w, b)
}
struct Input<'a> {
    src: &'a [u8],
    items: usize,
    remaining: usize,
}
impl<'a> Input<'a> {
    fn string(&mut self) -> Result<String> {
        Ok(std::str::from_utf8(self.blob()?)
            .map_err(|_| CodecError::Invalid)?
            .into())
    }

    fn charge(&mut self, n: usize) -> Result<()> {
        self.remaining = self.remaining.checked_sub(n).ok_or(CodecError::Limit)?;
        Ok(())
    }
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (b, rest) = self.src.split_first_chunk().ok_or(CodecError::Invalid)?;
        self.src = rest;
        Ok(*b)
    }
    fn count(&mut self) -> Result<usize> {
        let n = u32::from_le_bytes(self.take()?) as usize;
        if n > self.items {
            Err(CodecError::Limit)
        } else {
            Ok(n)
        }
    }
    fn blob(&mut self) -> Result<&'a [u8]> {
        let n = self.count()?;
        let b = self.src.split_off(..n).ok_or(CodecError::Invalid)?;
        self.charge(n)?;
        Ok(b)
    }
}
#[cfg(any(feature = "cluster", feature = "player"))]
macro_rules! scalars {
    ($t:ty, $($f:ident: $s:ty),*) => {
        impl Record for $t {
            const DEPTH: usize = 0;
            fn write(&self, _w: &mut &mut [u8], _: Limits) -> Result<()> {
                $(put(_w, &self.$f.to_le_bytes())?;)*
                Ok(())
            }
            fn read(_r: &mut Input<'_>) -> Result<Self> {
                Ok(Self { $($f: <$s>::from_le_bytes(_r.take()?),)* })
            }
        }
    };
}
impl Record for Admission {
    const DEPTH: usize = 1;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        blob(w, self.token.as_bytes(), l)
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self { token: r.string()? })
    }
}
macro_rules! echo {
    ($dto:ty) => {
        impl Record for $dto {
            const DEPTH: usize = 1;
            fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
                blob(w, &self.data, l)
            }
            fn read(r: &mut Input<'_>) -> Result<Self> {
                Ok(Self {
                    data: r.blob()?.into(),
                })
            }
        }
    };
}
echo!(Echo);
echo!(EchoReply);
#[cfg(feature = "cluster")]
mod cluster;
#[cfg(feature = "player")]
mod player;
mod sample;
pub use sample::PeerSample;

/// 仅供回环样例使用的单向业务通知；复用 Echo 的字节字段，不属于内置角色协议。
pub const fn notice<C: Codec<Echo>>() -> ace_codec::TypedMethod<C, Echo, ace_codec::NoReply> {
    ace_codec::TypedMethod::one_way(0x7f01)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_codec::MethodError;
    use ace_example_protocol::echo;
    use ace_net_proto::{Call, Frame, FrameError, Message, RpcError};
    use core::{
        num::NonZeroU64,
        sync::atomic::{AtomicUsize, Ordering::Relaxed},
    };
    static DECODES: AtomicUsize = AtomicUsize::new(0);
    struct Observed;
    impl Format for Observed {
        const ID: [u32; 3] = FixedCodec::ID;
    }
    impl<T: Record> Codec<T> for Observed {
        fn encode_into(v: &T, d: &mut [u8], s: &mut [u8], l: Limits) -> Result<usize> {
            FixedCodec::encode_into(v, d, s, l)
        }
        fn decode(b: &[u8], l: Limits) -> Result<T> {
            DECODES.fetch_add(1, Relaxed);
            FixedCodec::decode(b, l)
        }
    }
    #[test]
    fn typed_input_and_response_reject_drift_without_invoking_either_decoder() {
        let m = echo::<Observed>();
        let l = Limits::default();
        let call = Call {
            session: NonZeroU64::MIN,
            timeout_ms: NonZeroU64::MIN,
            max_response_bytes: 5,
        };
        let mut q = Frame {
            wire: m.wire(),
            message: Message::Request(call),
            body: &[1, 0, 0, 0, 7],
        };
        let mut a = Frame {
            message: Message::Response {
                session: call.session,
                error: None,
            },
            ..q
        };
        for i in 0..5 {
            let mut w = m.wire();
            match i {
                0..3 => w.codec[i] ^= 1,
                3 => w.request[31] ^= 1,
                _ => w.response[0] ^= 1,
            }
            q.wire = w;
            a.wire = w;
            assert_eq!(
                m.decode_frame_input(&q, l),
                Err(MethodError::Frame(FrameError::Protocol))
            );
            assert_eq!(
                m.decode_response(&a, call, l),
                Err(MethodError::Frame(FrameError::Protocol))
            );
        }
        q.wire = m.wire();
        q.wire.key += 1;
        assert_eq!(
            m.decode_frame_input(&q, l),
            Err(MethodError::Frame(FrameError::UnknownMethod))
        );
        a.message = Message::Response {
            session: call.session,
            error: Some(RpcError::Stopped),
        };
        a.body = &[];
        assert_eq!(
            m.decode_response(&a, call, l),
            Err(MethodError::Reply(RpcError::Stopped))
        );
        assert_eq!(DECODES.load(Relaxed), 0);
        q.wire = m.wire();
        a = Frame {
            message: Message::Response {
                session: call.session,
                error: None,
            },
            ..q
        };
        assert_eq!(m.decode_frame_input(&q, l).unwrap().data, [7]);
        assert_eq!(m.decode_response(&a, call, l).unwrap().data, [7]);
        assert_eq!(DECODES.load(Relaxed), 2);
        assert_eq!(
            m.decode_response(
                &a,
                Call {
                    max_response_bytes: 4,
                    ..call
                },
                l
            ),
            Err(MethodError::Frame(FrameError::Limit))
        );
        assert_eq!(DECODES.load(Relaxed), 2);
    }
}
