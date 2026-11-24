//! 仅供实库字节互通试验：用现有 loader 投递一个文件中的请求，不调度 RPC、不开 socket。
use super::*;
use ace_net_proto::{Frame, FrameLimits, Message as FrameMessage, ServiceAddress};
use std::{fs, io::Read, sync::OnceLock};

static DISPATCHED: AtomicUsize = AtomicUsize::new(0);
static SUCCESS: OnceLock<Vec<u8>> = OnceLock::new();
extern "C" fn capture(_: u64, op: HostOp<'_>, slot: &mut LocalSlot) -> HostResult {
    if let Err(s) = op.validate(slot) {
        return HostResult::status(s);
    }
    let s = match op {
        HostOp::Log(_, tag) if tag == "echo-dispatch" => {
            DISPATCHED.fetch_add(1, SeqCst);
            Status::OK
        }
        HostOp::Log(_, _) => Status::OK,
        HostOp::Reply(7, Reply::Success(b))
            if b.representation == Representation::Bytes && b.bytes.len() <= 128 =>
        {
            if SUCCESS.set(b.bytes.as_slice().to_vec()).is_ok() {
                Status::OK
            } else {
                Status::PROTOCOL
            }
        }
        _ => Status::NOT_READY,
    };
    HostResult::status(s)
}
static EXCHANGE: HostApi = HostApi {
    invoke: capture,
    ..HOST
};
pub(super) fn run(
    input: &Path,
    output: &Path,
    libraries: &[PathBuf],
) -> Result<(), Box<dyn Error>> {
    let mut bytes = Vec::new();
    fs::File::open(input)?.take(257).read_to_end(&mut bytes)?;
    let limits = FrameLimits {
        body_bytes: 128,
        response_bytes: 128,
        timeout_ms: 1000,
    };
    let f = Frame::from_player(&bytes, limits)?;
    let FrameMessage::Request(call) = f.message else {
        return Err("试验输入必须是请求".into());
    };
    // SAFETY: 沿用 loader-check 的受信启动库约束；只装载显式传入的本地策略夹具和 Cluster 薄库。
    let registry = unsafe { Implementations::load_startup(libraries) }?;
    registry.check_bindings(&[("cluster", Some("cluster")), ("test.policy", None)])?;
    let cluster = registry
        .descriptors()
        .find(|d| d.implementation == "cluster")
        .ok_or("缺少 Cluster")?;
    assert_eq!(
        (cluster.roles[0].revision, cluster.roles[0].codec),
        (1, f.wire.codec)
    );
    let mut methods = cluster.methods.as_slice().iter();
    for (key, stage, call) in [
        (769, PeerAccess::Bootstrap, false),
        (770, PeerAccess::Bootstrap, false),
        (771, PeerAccess::Admitted, false),
        (772, PeerAccess::Admitted, false),
        (773, PeerAccess::Admitted, false),
        (774, PeerAccess::None, true),
    ] {
        let m = methods.next().expect("Cluster 必需方法");
        assert_eq!(
            (
                m.wire.key,
                m.wire.codec,
                m.directions,
                m.client_callable,
                m.peer_control,
                m.wire.response != [0; 32]
            ),
            (key, f.wire.codec, 3, 0, stage, call)
        );
    }
    assert!(methods.next().is_none());
    let addr = ServiceAddress::new(NonZeroU16::MIN, 2)?;
    assert!(matches!(
        registry.create(
            "cluster",
            inspection_args(ServiceAddress::new(NonZeroU16::MIN, 3)?, 1, b""),
            IngressGrants::default()
        ),
        Err(Status::NOT_READY)
    )); // 本机检查没有部署身份端点。
    let mut args = inspection_args(addr, 1, b"");
    args.host = &EXCHANGE;
    args.limits.payload_bytes = NonZeroU64::new(8192).unwrap();
    let mut actor = registry
        .create("test.policy", args, IngressGrants::default())
        .map_err(|s| format!("夹具 create：{s:?}"))?;
    assert_eq!(control(&mut actor, Control::Prepare), Status::OK);
    assert_eq!(control(&mut actor, Control::Start), Status::OK);
    actor.publish().expect("测试实例完成 Start");
    let wire = WireContract {
        key: f.wire.key,
        codec: f.wire.codec,
        request: f.wire.request,
        response: f.wire.response,
    };
    let mut send = |wire| {
        actor.message(
            Message {
                source: ServiceAddress::new(NonZeroU16::MIN, 1).unwrap().bits(),
                reply_token: 7,
                charge_token: 0,
                request: Outbound {
                    destination: addr.bits(),
                    wire,
                    body: Body {
                        representation: Representation::Bytes,
                        bytes: f.body.into(),
                    },
                    timeout_ms: call.timeout_ms.get(),
                    reply: ReplyBudget {
                        ceiling: call.max_response_bytes.into(),
                        representation: Representation::Bytes,
                    },
                    entry: Entry::Player {
                        push_session: RNone,
                    },
                },
            },
            &mut RNone,
        )
    };
    for i in 0..5 {
        let mut wrong = wire;
        match i {
            0..3 => wrong.codec[i] ^= 1,
            3 => wrong.request[0] ^= 1,
            _ => wrong.response[31] ^= 1,
        };
        assert_eq!(send(wrong), Status::PROTOCOL);
    }
    assert_eq!(DISPATCHED.load(SeqCst), 0);
    assert!(SUCCESS.get().is_none());
    assert_eq!(send(wire), Status::OK);
    assert_eq!(DISPATCHED.load(SeqCst), 1);
    let response = Frame {
        wire: f.wire,
        message: FrameMessage::Response {
            session: call.session,
            error: None,
        },
        body: SUCCESS.get().ok_or("服务未提交成功 body")?,
    };
    let mut buf = [0; 256];
    let n = response.encode_into(&mut buf, limits)?;
    fs::write(output, &buf[..n])?;
    println!(
        "实库方法 {} codec={:?}：五种错契约业务入口 0 次；正确请求进入 1 次，已接收插件实际编码的响应",
        wire.key, wire.codec
    );
    Ok(())
}
