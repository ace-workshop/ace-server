//! 未部署的候选类型声明，只用于本地装配反例，不启动实例。
use abi_stable::prefix_type::PrefixTypeTrait;
use ace_role_gateway::{self as gate, Kick, OnlineCount};
use ace_role_watchdog as wd;
use ace_server_sdk::{
    AceCodec, Context,
    Direction::Send,
    Service, Status, api, bytes_method,
    codec::{Format, Schema},
    native::*,
};
struct Candidate<const CHANGE: u8>;
impl<const CHANGE: u8> Candidate<CHANGE> {
    const ROLE: Role = Role::new(
        wd::ROLE,
        if CHANGE == 0 {
            wd::REVISION + 1
        } else {
            wd::REVISION
        },
        AceCodec::ID,
    );
    const BIND: Method = {
        let mut m = bytes_method(gate::bind::<AceCodec>(), Send);
        match CHANGE {
            1 => m.wire.request = Kick::DIGEST,
            2 => m.wire.response = OnlineCount::DIGEST,
            3 => m.wire.codec[2] += 1,
            _ => (),
        }
        m
    };
}
impl<const CHANGE: u8> Service for Candidate<CHANGE> {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        "example.candidate",
        "fixture",
        &[Self::ROLE],
        &[],
        &[Self::BIND],
    );
    fn create(_: Context, _: &[u8]) -> Result<Self, Status> {
        Err(Status::NOT_READY)
    }
}
extern "C" fn table() -> &'static ServiceApi {
    match std::env::var("ACE_FIXTURE_CANDIDATE").as_deref() {
        Ok("request") => api::<Candidate<1>>(),
        Ok("response") => api::<Candidate<2>>(),
        Ok("options") => api::<Candidate<3>>(),
        _ => api::<Candidate<0>>(),
    }
}
#[abi_stable::export_root_module]
fn root() -> ServiceModuleRef {
    ServiceModule {
        ace_service_v1: table,
    }
    .leak_into_prefix()
}
