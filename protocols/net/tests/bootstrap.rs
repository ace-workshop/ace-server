//! 只检查固定引导记录；不模拟握手状态机或 KCP 核心。
use ace_net_proto::{FrameError, bootstrap::*};
use core::num::{NonZeroU16, NonZeroU32, NonZeroU64};
#[test]
fn fixed_handshakes_are_distinct_and_validate_candidate_identity() {
    let b = [1, 0, 0, 0, 0, 0, 0x10, 0, 1, 0, 0, 0];
    assert_eq!(Hello.encode(), b);
    assert_eq!(Hello::decode(&b), Ok(Hello));
    let ready = PeerReady.encode();
    assert_eq!(PeerReady::decode(&ready), Ok(PeerReady));
    assert_eq!(Hello::decode(&ready), Err(FrameError::Protocol));
    let mut bad = b;
    bad[8] = 2;
    assert_eq!(Hello::decode(&bad), Err(FrameError::Protocol));
    let hello = PeerHello {
        node: NonZeroU16::MIN,
        boot: NonZeroU64::MIN,
        dialer: NonZeroU16::MIN,
        dialer_boot: NonZeroU64::MIN,
        attempt: NonZeroU64::MAX,
    };
    let mut b = hello.encode().unwrap();
    assert_eq!(PeerHello::decode(&b), Ok(hello));
    assert_eq!(Hello::decode(&b), Err(FrameError::Protocol));
    b[20] = 2;
    assert_eq!(PeerHello::decode(&b), Err(FrameError::Protocol));
}
#[test]
fn kcp_outer_checks_whole_datagram_and_nonzero_session_before_payload() {
    let s = KcpSession {
        epoch: NonZeroU64::MIN,
        token: NonZeroU64::new(2).unwrap(),
        conv: NonZeroU32::new(3).unwrap(),
    };
    let nonce = NonZeroU64::MAX;
    let mut b = [0; 48];
    for p in [
        KcpEnvelope::Hello(nonce),
        KcpEnvelope::Offer(nonce, s),
        KcpEnvelope::Confirm(nonce, s),
        KcpEnvelope::Ready(nonce, s),
    ] {
        let n = p.encode_into(&mut b, 48).unwrap();
        assert_eq!(KcpEnvelope::decode(&b[..n], n), Ok(p));
        assert_eq!(
            KcpEnvelope::decode(&b[..n + 1], n + 1),
            Err(FrameError::Protocol)
        );
    }
    let data = KcpEnvelope::Data(s, b"x");
    let n = data.encode_into(&mut b, 48).unwrap();
    assert_eq!(n, KcpEnvelope::DATA_HEADER + 1);
    assert_eq!(KcpEnvelope::decode(&b[..n], n), Ok(data));
    assert_eq!(KcpEnvelope::decode(&b[..n], n - 1), Err(FrameError::Limit));
    b[16] = 0;
    assert_eq!(KcpEnvelope::decode(&b[..n], n), Err(FrameError::Protocol));
}
