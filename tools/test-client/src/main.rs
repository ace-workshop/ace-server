//! 在文件或原生 TCP/KCP 上交换相同的公共请求与响应。
mod network;
#[cfg(feature = "codec_ace")]
use ace_codec::AceCodec as Selected;
use ace_codec::{Codec, Limits, NoReply, TypedMethod};
use ace_example_protocol::{Admission, Echo, admission, echo};
#[cfg(not(feature = "codec_ace"))]
use ace_fixture_codec::FixedCodec as Selected;
use ace_net_proto::{Call, Frame, FrameLimits, Message};
use std::{error::Error, fs, io::Read, num::NonZeroU64, path::Path};

const LIMITS: FrameLimits = FrameLimits {
    body_bytes: 128,
    response_bytes: 128,
    timeout_ms: 750,
};
const CALL: Call = Call {
    session: NonZeroU64::new(0x0102030405060708).unwrap(),
    timeout_ms: NonZeroU64::new(750).unwrap(),
    max_response_bytes: 128,
};
fn golden(label: &str, actual: &[u8]) -> Result<(), Box<dyn Error>> {
    use std::fmt::Write;
    let mut line = format!(
        "{}.{label} ",
        if cfg!(feature = "codec_ace") {
            "ace"
        } else {
            "fixed"
        }
    );
    for x in actual {
        write!(&mut line, "{x:02x}")?;
    }
    if include_str!("../golden.txt").lines().any(|s| s == line) {
        Ok(())
    } else {
        Err(format!("不匹配独立 golden：{line}").into())
    }
}
fn request(data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    if data.len() > 2048 {
        return Err("测试文本超过 2048 字节".into());
    }
    let limits = if data.len() > 64 {
        FrameLimits {
            body_bytes: 4096,
            response_bytes: 4096,
            ..LIMITS
        }
    } else {
        LIMITS
    };
    let method = echo::<Selected>();
    let mut dst = [0; 4096];
    let body = method.encode_input(
        &Echo { data: data.into() },
        &mut dst,
        &mut [0; 8192],
        Limits::default(),
    )?;
    let frame = Frame {
        wire: method.wire(),
        message: Message::Request(Call {
            max_response_bytes: limits.response_bytes,
            ..CALL
        }),
        body,
    };
    let mut buf = [0; 4096 + 116];
    let n = frame.encode_into(&mut buf, limits)?;
    if data == b"ACE" {
        golden("request", &buf[..n])?;
    }
    Ok(buf[..n].to_vec())
}
fn one_way<C: Codec<Q>, Q>(m: TypedMethod<C, Q, NoReply>, q: Q) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut dst = [0; 128];
    let body = m.encode_input(&q, &mut dst, &mut [0; 2048], Limits::default())?;
    let f = Frame {
        wire: m.wire(),
        message: Message::Send,
        body,
    };
    let mut buf = [0; 256];
    let n = f.encode_into(&mut buf, LIMITS)?;
    Ok(buf[..n].into())
}
fn handshake(token: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    one_way(
        admission::<Selected>(),
        Admission {
            token: token.into(),
        },
    )
}
fn check(buf: &[u8]) -> Result<(), Box<dyn Error>> {
    let method = echo::<Selected>();
    let frame = Frame::decode(buf, LIMITS)?;
    if method
        .decode_response(&frame, CALL, Limits::default())?
        .data
        != b"ACE"
    {
        return Err("响应没有回显输入".into());
    }
    golden("response", buf)?;
    println!(
        "公共请求/响应与独立 golden 一致，客户端已反向解码：codec={:?} body={:02x?}",
        method.wire().codec,
        frame.body
    );
    Ok(())
}
fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    match args.as_slice() {
        [help] if help == "--help" => println!(
            "ace-test-client sample-request <文件> [测试文本] | check-response <文件> | tcp <IP:port> | kcp <IP:port> | player-tcp <IP:port> | player-kcp <IP:port>（两种 player 模式可加 --token <演示标记>） | sample-admission <文件> <演示标记> | sample-notice <文件> <短文本>"
        ),
        [mode, file] if mode == "sample-request" => {
            fs::write(Path::new(file), request(b"ACE")?)?;
        }
        [mode, file, text] if mode == "sample-request" => {
            let b = text.to_str().ok_or("测试文本不是 UTF-8")?.as_bytes();
            fs::write(Path::new(file), request(b)?)?;
        }
        [mode, file, text] if mode == "sample-admission" || mode == "sample-notice" => {
            let text = text.to_str().ok_or("不是 UTF-8")?;
            let frame = if mode == "sample-admission" {
                handshake(text)?
            } else {
                one_way(
                    ace_fixture_codec::notice::<Selected>(),
                    Echo {
                        data: text.as_bytes().into(),
                    },
                )?
            };
            fs::write(file, frame)?;
        }
        [mode, file] if mode == "check-response" => {
            let mut buf = Vec::new();
            fs::File::open(file)?.take(257).read_to_end(&mut buf)?;
            check(&buf)?;
        }
        [mode, endpoint, rest @ ..]
            if matches!(
                mode.to_str(),
                Some("tcp" | "kcp" | "player-tcp" | "player-kcp")
            ) =>
        {
            let mode = mode.to_str().ok_or("无效模式")?;
            let player = mode.starts_with("player-");
            let (args, barrier) = match rest {
                [args @ .., opt] if player && opt == "--barrier" => (args, true),
                args => (args, false),
            };
            let token = match args {
                [] => "demo",
                [opt, value] if player && opt == "--token" => {
                    value.to_str().ok_or("标记不是 UTF-8")?
                }
                _ => return Err("无效参数".into()),
            };
            let admission = player.then(|| handshake(token)).transpose()?;
            let addr = endpoint.to_str().ok_or("端点不是 UTF-8")?.parse()?;
            let req = request(b"ACE")?;
            let frame = if mode.ends_with("tcp") {
                network::tcp(addr, &req, admission.as_deref(), barrier)?
            } else {
                network::kcp(addr, &req, admission.as_deref(), barrier)?
            };
            check(&frame)?;
        }
        _ => return Err("参数不符；用法见 --help".into()),
    }
    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ace_example_protocol::{Admission, admission};
    #[test]
    fn demo_handshake_is_one_way_and_has_no_internal_session() {
        let m = admission::<Selected>();
        let l = Limits::default();
        let q = Admission {
            token: "demo".into(),
        };
        let mut dst = [0; 128];
        let body = m.encode_input(&q, &mut dst, &mut [0; 2048], l).unwrap();
        let mut buf = [0; 256];
        let n = Frame {
            wire: m.wire(),
            message: Message::Send,
            body,
        }
        .encode_into(&mut buf, LIMITS)
        .unwrap();
        let f = Frame::from_player(&buf[..n], LIMITS).unwrap();
        assert_eq!(f.wire.response, [0; 32]);
        assert_eq!(m.decode_frame_input(&f, l), Ok(q));
    }
}
