use super::*;
use ace_codec::Schema;
impl Schema for Open {
    const CANONICAL: &'static str = "watchdog.Open/1<tag:u16;1:Accepted{0:conn:!gateway.Connection/1{0:transport:!gateway.Transport/1<enum:u8;1:tcp;2:kcp>,1:id:u64=0(resource-id),2:generation:u64=0(generation)},1:peer:!utf8(observed-endpoint)};2:Handshake{0:conn:!gateway.Connection/1{0:transport:!gateway.Transport/1<enum:u8;1:tcp;2:kcp>,1:id:u64=0(resource-id),2:generation:u64=0(generation)},1:handshake:!demo.Admission/1{0:token:!utf8(demo-token)}}>";
    const DIGEST: [u8; 32] = [
        0xa8, 0x60, 0x46, 0x0d, 0x77, 0x0c, 0x2c, 0x55, 0x1f, 0x52, 0xc2, 0x8a, 0x5c, 0xda, 0xa9,
        0x3e, 0xce, 0x87, 0xd1, 0xa3, 0x88, 0x4a, 0xe2, 0x78, 0x83, 0x5d, 0xf2, 0x85, 0xd0, 0xb7,
        0xe6, 0xbb,
    ];
}
impl Schema for Close {
    const CANONICAL: &'static str = "watchdog.Close/1{0:conn:!gateway.Connection/1{0:transport:!gateway.Transport/1<enum:u8;1:tcp;2:kcp>,1:id:u64=0(resource-id),2:generation:u64=0(generation)}}";
    const DIGEST: [u8; 32] = [
        0x8f, 0xca, 0x2e, 0x3f, 0x08, 0x5b, 0x05, 0x81, 0x91, 0xb1, 0xa3, 0x69, 0xf1, 0x3a, 0x0d,
        0xbc, 0x35, 0x1e, 0x4e, 0x5d, 0x24, 0xf5, 0x66, 0xe8, 0x3e, 0x53, 0x29, 0x66, 0x8a, 0x70,
        0xf6, 0xed,
    ];
}
impl Schema for CheckAdmission {
    const CANONICAL: &'static str = "watchdog.CheckAdmission/1{0:conn:!gateway.Connection/1{0:transport:!gateway.Transport/1<enum:u8;1:tcp;2:kcp>,1:id:u64=0(resource-id),2:generation:u64=0(generation)},1:peer:!utf8(observed-endpoint),2:handshake:!demo.Admission/1{0:token:!utf8(demo-token)}}";
    const DIGEST: [u8; 32] = [
        0x08, 0x53, 0x57, 0x4f, 0x44, 0xd9, 0xf1, 0x2b, 0xf9, 0x85, 0x07, 0x64, 0xde, 0xa2, 0x23,
        0x1e, 0x26, 0xb2, 0xed, 0x87, 0x47, 0x65, 0x4a, 0x79, 0xdb, 0x95, 0x51, 0xa8, 0xe6, 0x73,
        0xfa, 0x38,
    ];
}
impl Schema for AdmissionDecision {
    const CANONICAL: &'static str = "watchdog.AdmissionDecision/1<tag:u16;1:Admitted{0:target:u64=0(service-address:16+48)};2:Denied{0:code:u32=0(denial-code)};3:Unavailable>";
    const DIGEST: [u8; 32] = [
        0x87, 0xa4, 0x3a, 0xf4, 0xf3, 0x32, 0xec, 0x0c, 0xdc, 0x3f, 0xf7, 0x8e, 0x47, 0x0b, 0x78,
        0xf3, 0x8e, 0xf2, 0x8c, 0x4f, 0x44, 0x6c, 0x8b, 0x59, 0x12, 0x95, 0xf8, 0xa6, 0xcd, 0x51,
        0x81, 0x15,
    ];
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    #[test]
    fn digests_match_canonical_bytes() {
        fn check<T: Schema>() {
            assert_eq!(
                Sha256::digest(T::CANONICAL.as_bytes())[..],
                T::DIGEST,
                "{}",
                T::CANONICAL
            );
        }
        check::<Open>();
        check::<Close>();
        check::<CheckAdmission>();
        check::<AdmissionDecision>();
    }
}
