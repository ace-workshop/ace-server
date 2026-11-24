//! 替换样例：远端只可解析导出的 echo；本机注册表和普通数据直连不受导出过滤影响。
use abi_stable::std_types::RStr;
use ace_cluster::Cluster;
use ace_role_cluster::{Method as ClusterMethod, Resolve};
use ace_server_sdk::{AceCodec, Context, Service, Status, codec::Codec, native::*};
struct ExportCluster {
    ctx: Context,
    directory: Cluster<AceCodec>,
}
impl Service for ExportCluster {
    const DESCRIPTOR: Descriptor = Descriptor {
        implementation: RStr::from_str("example.export-cluster"),
        ..Cluster::<AceCodec>::DESCRIPTOR
    };
    fn create(ctx: Context, cfg: &[u8]) -> Result<Self, Status> {
        Ok(Self {
            directory: Cluster::create(ctx.clone(), cfg)?,
            ctx,
        })
    }
    fn on_message(&mut self, m: Message<'_>, slot: &mut LocalSlot) -> Status {
        if m.request.wire.key == ClusterMethod::Resolve as u32
            && m.source >> 48 != u64::from(self.ctx.address().node().get())
        {
            let q: Resolve =
                match AceCodec::decode(m.request.body.bytes.as_slice(), self.ctx.limits_codec()) {
                    Ok(q) => q,
                    Err(_) => return Status::PROTOCOL,
                };
            if q.alias != "echo" {
                self.ctx.command(HostOp::Log(
                    0,
                    format!("export-hidden source={} node={}", m.source, q.node)
                        .as_str()
                        .into(),
                ));
                return Status::NOT_FOUND;
            }
        }
        self.directory.on_message(m, slot)
    }
    fn on_control(&mut self, op: Control<'_>, slot: &mut LocalSlot) -> Progress {
        self.directory.on_control(op, slot)
    }
    fn on_cleanup(&mut self, e: Event<'_>, slot: &mut LocalSlot) -> Status {
        self.directory.on_cleanup(e, slot)
    }
    fn cancel(&mut self) {
        self.directory.cancel();
    }
}
#[abi_stable::export_root_module]
fn root() -> ServiceModuleRef {
    ace_server_sdk::export::<ExportCluster>()
}
