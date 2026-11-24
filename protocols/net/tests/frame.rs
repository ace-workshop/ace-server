//! 独立核对公共头、内部入口及拒绝发生的位置，不创建 transport。
use ace_net_proto::*;
use core::num::NonZeroU64;
const W: Contract = Contract {
    key: 0x1020,
    codec: [7, 3, 0],
    request: [1; 32],
    response: [2; 32],
};
const CALL: Call = Call {
    session: NonZeroU64::MIN,
    timeout_ms: NonZeroU64::new(250).unwrap(),
    max_response_bytes: 4,
};
const LIMITS: FrameLimits = FrameLimits {
    body_bytes: 8,
    response_bytes: 4,
    timeout_ms: 250,
};
const INPUT: Frame<'static> = Frame {
    wire: W,
    message: Message::Request(CALL),
    body: b"abc",
};

#[test]
fn request_header_rejects_bad_shape_and_limits_before_body() {
    let mut bytes = [0; 120];
    let n = INPUT.encode_into(&mut bytes, LIMITS).unwrap();
    assert_eq!(n, 119);
    let head = Frame::inspect(&bytes[..116], LIMITS).unwrap();
    assert_eq!(
        (head.wire, head.message, head.body_len, head.frame_len),
        (W, INPUT.message, 3, n)
    );
    assert_eq!(Frame::from_player(&bytes[..n], LIMITS), Ok(INPUT));
    assert_eq!(
        Frame::decode(&bytes[..116], LIMITS),
        Err(FrameError::Truncated)
    );
    assert_eq!(Frame::decode(&bytes, LIMITS), Err(FrameError::Protocol));
    for (i, v) in [(0, 2), (4, 1), (6, 8), (8, 1), (96, 0), (108, 0)] {
        let mut b = bytes;
        b[i] = v;
        assert_eq!(
            Frame::inspect(&b[..116], LIMITS),
            Err(FrameError::Protocol),
            "offset {i}"
        )
    }
    for (i, v) in [(92, 9), (104, 5), (108, 251)] {
        let mut b = bytes;
        b[i] = v;
        assert_eq!(
            Frame::inspect(&b[..116], LIMITS),
            Err(FrameError::Limit),
            "offset {i}"
        )
    }
    for range in [28..60, 60..92] {
        let mut b = bytes;
        b[range].fill(0);
        assert_eq!(Frame::inspect(&b[..116], LIMITS), Err(FrameError::Protocol));
    }
    let mut small = [0xff; 118];
    assert_eq!(
        INPUT.encode_into(&mut small, LIMITS),
        Err(FrameError::Limit)
    );
    assert_eq!(small, [0xff; 118]);
}

#[test]
fn response_checks_pending_budget_session_and_closed_error_domain() {
    let ok = Frame {
        wire: W,
        message: Message::Response {
            session: CALL.session,
            error: None,
        },
        body: b"",
    };
    let mut b = [0; 108];
    let n = ok.encode_into(&mut b, LIMITS).unwrap();
    assert_eq!(
        Frame::decode(&b[..n], LIMITS)
            .unwrap()
            .check_response(W, CALL),
        Ok(Ok(&[][..]))
    );
    assert_eq!(
        Frame::from_player(&b[..n], LIMITS),
        Err(FrameError::Protocol)
    );
    // 合法声明的成功 body 即使尚未抵达，也不能突破这次调用的更小预留。
    b[92] = 4;
    assert_eq!(
        Frame::inspect(&b[..n], LIMITS).unwrap().check_response(
            W,
            Call {
                max_response_bytes: 3,
                ..CALL
            }
        ),
        Err(FrameError::Limit)
    );
    b[92] = 0;
    for code in [4, 6, 9, 14, 255] {
        b[8] = code;
        assert_eq!(Frame::decode(&b[..n], LIMITS), Err(FrameError::Protocol));
    }
    b[8] = RpcError::UnknownMethod as u8;
    b[16..92].fill(0);
    let err = Frame::decode(&b[..n], LIMITS).unwrap();
    assert_eq!(
        err.check_response(W, CALL),
        Ok(Err(RpcError::UnknownMethod))
    );
    assert_eq!(
        err.check_response(
            Contract {
                key: W.key + 1,
                ..W
            },
            CALL
        ),
        Err(FrameError::Protocol)
    );
    assert_eq!(
        err.check_response(
            W,
            Call {
                session: NonZeroU64::MAX,
                ..CALL
            }
        ),
        Err(FrameError::Protocol)
    );
    b[92] = 1;
    assert_eq!(Frame::inspect(&b[..n], LIMITS), Err(FrameError::Protocol));
}

#[test]
fn node_entry_and_control_cannot_be_submitted_by_a_player() {
    let r = Route {
        source: 0x1234_0000_0000_00aau64.try_into().unwrap(),
        destination: 0x5678_0000_0000_00bbu64.try_into().unwrap(),
        entry: NodeEntry::Player(NonZeroU64::new(42)),
    };
    let node = NodeFrame::Routed(r, INPUT);
    let mut b = [0; 160];
    let n = node.encode_into(&mut b, LIMITS).unwrap();
    assert_eq!(&b[116..124], &[0xaa, 0, 0, 0, 0, 0, 0x34, 0x12]);
    assert_eq!(NodeFrame::inspect(&b[..140], LIMITS).unwrap().0, Some(r));
    assert_eq!(NodeFrame::decode(&b[..n], LIMITS), Ok(node));
    assert_eq!(
        Frame::from_player(&b[..n], LIMITS),
        Err(FrameError::Protocol)
    );
    b[116..124].fill(0);
    assert_eq!(
        NodeFrame::decode(&b[..n], LIMITS),
        Err(FrameError::Protocol)
    );
    let one = Contract {
        response: [0; 32],
        ..W
    };
    let ctl = NodeFrame::Control(one, b"x");
    let n = ctl.encode_into(&mut b, LIMITS).unwrap();
    assert_eq!(NodeFrame::decode(&b[..n], LIMITS), Ok(ctl));
    assert_eq!(
        Frame::from_player(&b[..n], LIMITS),
        Err(FrameError::Protocol)
    );
    b[7] = 0;
    assert_eq!(
        Frame::from_player(&b[..n], LIMITS).unwrap().message,
        Message::Send
    );
    assert_eq!(
        NodeFrame::decode(&b[..n], LIMITS),
        Err(FrameError::Protocol)
    );
    for i in [60, 96] {
        let mut wrong = b;
        wrong[i] = 1;
        assert_eq!(
            Frame::from_player(&wrong[..n], LIMITS),
            Err(FrameError::Protocol)
        );
    }
}
