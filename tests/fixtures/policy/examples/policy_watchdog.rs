//! 替换样例：不提供 demo 准入，必须显式配置策略。本实现只宣告 Bytes 角色方法。
use abi_stable::std_types::{RSlice, RStr};
#[cfg(not(feature = "codec_ace"))]
use ace_fixture_codec::FixedCodec as Selected;
use ace_role_gateway as gate;
use ace_role_watchdog as wd;
#[cfg(feature = "codec_ace")]
use ace_server_sdk::AceCodec as Selected;
use ace_server_sdk::{Context, Direction::*, Service, Status, bytes_method, native::*};
use ace_watchdog::Watchdog;
struct PolicyWatchdog(Watchdog<Selected>);
impl Service for PolicyWatchdog {
    const DESCRIPTOR: Descriptor = Descriptor {
        implementation: RStr::from_str("example.policy-watchdog"),
        methods: RSlice::from_slice(&[
            bytes_method(wd::open::<Selected>(), Receive),
            bytes_method(wd::close::<Selected>(), Receive),
            bytes_method(wd::check_admission::<Selected>(), Send),
            bytes_method(gate::bind::<Selected>(), Send),
            bytes_method(gate::kick::<Selected>(), Send),
        ]),
        ..Watchdog::<Selected>::DESCRIPTOR
    };
    fn create(ctx: Context, cfg: &[u8]) -> Result<Self, Status> {
        let table: toml::Table = std::str::from_utf8(cfg)
            .map_err(|_| Status::PROTOCOL)?
            .parse()
            .map_err(|_| Status::PROTOCOL)?;
        if table
            .get("admission_policy_alias")
            .and_then(|v| v.as_str())
            .is_none_or(str::is_empty)
        {
            ctx.command(HostOp::Log(
                0,
                "policy-watchdog requires admission_policy_alias".into(),
            ));
            return Err(Status::DENIED);
        }
        Watchdog::create(ctx, cfg).map(Self)
    }
    fn on_message(&mut self, m: Message<'_>, slot: &mut LocalSlot) -> Status {
        self.0.on_message(m, slot)
    }
    fn on_control(&mut self, op: Control<'_>, slot: &mut LocalSlot) -> Progress {
        self.0.on_control(op, slot)
    }
    fn on_cleanup(&mut self, e: Event<'_>, slot: &mut LocalSlot) -> Status {
        self.0.on_cleanup(e, slot)
    }
    fn cancel(&mut self) {
        self.0.cancel();
    }
}
#[abi_stable::export_root_module]
fn root() -> ServiceModuleRef {
    ace_server_sdk::export::<PolicyWatchdog>()
}
