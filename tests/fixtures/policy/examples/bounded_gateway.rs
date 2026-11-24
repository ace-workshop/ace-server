//! 替换样例：入口只接纳至多 256 字节的完整玩家帧；其余路由和清理复用正常 Gateway。
use abi_stable::std_types::RStr;
use ace_gateway::Gateway;
use ace_server_sdk::{AceCodec, Context, Service, Status, native::*};
struct BoundedGateway {
    ctx: Context,
    gate: Gateway<AceCodec>,
}
impl Service for BoundedGateway {
    const DESCRIPTOR: Descriptor = Descriptor {
        implementation: RStr::from_str("example.bounded-gate"),
        ..Gateway::<AceCodec>::DESCRIPTOR
    };
    fn create(ctx: Context, cfg: &[u8]) -> Result<Self, Status> {
        Ok(Self {
            gate: Gateway::create(ctx.clone(), cfg)?,
            ctx,
        })
    }
    fn on_message(&mut self, m: Message<'_>, slot: &mut LocalSlot) -> Status {
        let bind = (m.request.wire.key == ace_role_gateway::Method::Bind as u32).then_some((
            m.request.body.representation,
            m.request.reply.representation,
            slot.is_some(),
        ));
        let status = self.gate.on_message(m, slot);
        if status == Status::OK
            && let Some((input, reply, owned)) = bind
        {
            self.ctx.command(HostOp::Log(
                0,
                format!(
                    "bounded-bind input={input:?} reply={reply:?} transferred={}",
                    owned && slot.is_none()
                )
                .as_str()
                .into(),
            ));
        }
        status
    }
    fn on_control(&mut self, op: Control<'_>, slot: &mut LocalSlot) -> Progress {
        if let Control::Event(Event::Network(NetEvent::Data(c, frame))) = &op
            && frame.len() > 256
        {
            self.ctx.command(HostOp::Log(
                0,
                format!(
                    "bounded-frame conn={}:{} bytes={} limit=256",
                    c.handle.id,
                    c.handle.generation,
                    frame.len()
                )
                .as_str()
                .into(),
            ));
            return Progress::idle(self.ctx.net_close(*c).err().unwrap_or(Status::OK));
        }
        self.gate.on_control(op, slot)
    }
    fn on_cleanup(&mut self, e: Event<'_>, slot: &mut LocalSlot) -> Status {
        self.gate.on_cleanup(e, slot)
    }
    fn cancel(&mut self) {
        self.gate.cancel();
    }
}
#[abi_stable::export_root_module]
fn root() -> ServiceModuleRef {
    ace_server_sdk::export::<BoundedGateway>()
}
