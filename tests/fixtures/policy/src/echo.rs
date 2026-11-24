//! 与纯测试客户端共享方法，不用客户端 codec 推断宿主行为。
use abi_stable::std_types::{
    ROption::{RNone, RSome},
    RSlice,
};
use ace_example_protocol::{EchoReply, echo, local::LocalEcho};
#[cfg(not(feature = "codec_ace"))]
use ace_fixture_codec::FixedCodec as Selected;
#[cfg(feature = "codec_ace")]
use ace_server_sdk::AceCodec as Selected;
use ace_server_sdk::{
    Context, Direction, LocalDto, Status, codec::Limits, local_call, native::*, wire_contract,
};

pub(super) const ECHO: Method = Method {
    client_callable: 1,
    ..local_call(echo::<Selected>(), Direction::Receive, 64 * 1024)
};
pub(super) fn handle(ctx: &Context, msg: Message<'_>, slot: &mut LocalSlot) -> Status {
    let method = echo::<Selected>();
    let req = msg.request;
    let budget = usize::try_from(ctx.limits().payload_bytes.get()).unwrap_or(usize::MAX);
    let mut limits = Limits::default();
    limits.wire_bytes = limits.wire_bytes.min(budget);
    limits.decoded_bytes = limits.decoded_bytes.min(budget);
    let error = |s| {
        ctx.command(HostOp::Reply(msg.reply_token, Reply::Error(s)))
            .status
    };
    // 夹具记录进入业务路径的次数；错契约必须连这里都不进入。
    ctx.command(HostOp::Log(0, "echo-dispatch".into()));
    let data = match req.body.representation {
        Representation::Bytes => {
            match method.decode_input(wire_contract(req.wire), req.body.bytes.as_slice(), limits) {
                Ok(q) => q.data,
                Err(_) => return error(Status::PROTOCOL),
            }
        }
        Representation::LocalOwned => {
            let RSome(p) = slot.as_ref() else {
                return Status::PROTOCOL;
            };
            let Ok(v) = p.view::<LocalEcho>() else {
                return Status::PROTOCOL;
            };
            if p.owned_bytes() > budget {
                return Status::BUSY;
            }
            let data = v.data.as_slice().to_vec();
            let _consumed = std::mem::replace(slot, RNone);
            data
        }
    };
    let value = EchoReply { data };
    let cap = usize::try_from(req.reply.ceiling)
        .unwrap_or(usize::MAX)
        .min(budget);
    match req.reply.representation {
        Representation::LocalOwned => match value.into_local(cap) {
            Ok(local) => {
                ctx.invoke(
                    HostOp::Reply(
                        msg.reply_token,
                        Reply::Success(Body {
                            representation: Representation::LocalOwned,
                            bytes: RSlice::from_slice(&[]),
                        }),
                    ),
                    &mut RSome(local),
                )
                .status
            }
            Err(_) => error(Status::TOO_SMALL),
        },
        Representation::Bytes => {
            // 使用夹具自己的有界编码区；packed 的实际长度不能从 DTO 大小推断。
            let mut dst = vec![0; limits.wire_bytes.min(method.max_response_bytes() as usize)];
            let mut scratch = vec![0; limits.decoded_bytes];
            match method.encode_response(&value, &mut dst, &mut scratch, limits) {
                Ok(b) if b.len() > cap => error(Status::TOO_SMALL),
                Ok(b) => {
                    ctx.command(HostOp::Reply(
                        msg.reply_token,
                        Reply::Success(Body {
                            representation: Representation::Bytes,
                            bytes: b.into(),
                        }),
                    ))
                    .status
                }
                Err(_) => error(Status::PROTOCOL),
            }
        }
    }
}
