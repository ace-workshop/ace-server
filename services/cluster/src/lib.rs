//! 可替换的单种子成员目录。逐条业务包不进入成员 Actor。
mod membership;
use ace_role_cluster as cluster;
use ace_server_sdk::{
    Actor, ActorService, Context, Direction::*, Handler, Incoming, RpcMethod, ServiceAddress,
    State, Status, bytes_method, native::*,
};
use membership::Directory;

/// 使用指定角色 codec 的完整 SDK Actor（包括异步远端别名查询）。
/// 只有格式号而没有角色 DTO 实现的类型不能导出：
/// ```compile_fail
/// use ace_server_sdk::{api,codec::Format};
/// struct NamedOnly;
/// impl Format for NamedOnly { const ID:[u32;3] = [5,1,0]; }
/// let _table = api::<ace_cluster::Cluster<NamedOnly>>();
/// ```
pub type Cluster<C> = Actor<Directory<C>>;
impl<C: cluster::ClusterCodec + 'static> ActorService for Directory<C> {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        "cluster",
        env!("CARGO_PKG_VERSION"),
        &[Role::new(
            ace_role_cluster::ROLE,
            ace_role_cluster::REVISION,
            C::ID,
        )],
        &[],
        &[
            Method {
                peer_control: PeerAccess::Bootstrap,
                ..bytes_method(cluster::join::<C>(), Both)
            },
            Method {
                peer_control: PeerAccess::Bootstrap,
                ..bytes_method(cluster::join_result::<C>(), Both)
            },
            Method {
                peer_control: PeerAccess::Admitted,
                ..bytes_method(cluster::members::<C>(), Both)
            },
            Method {
                peer_control: PeerAccess::Admitted,
                ..bytes_method(cluster::leave::<C>(), Both)
            },
            Method {
                peer_control: PeerAccess::Admitted,
                ..bytes_method(cluster::heartbeat::<C>(), Both)
            },
            bytes_method(cluster::resolve::<C>(), Both),
        ],
    );
    fn create(ctx: Context, config: &[u8]) -> Result<Self, Status> {
        Self::new(ctx, config)
    }
    fn control(&mut self, op: Control<'_>) -> Progress {
        Progress::idle(self.control_event(op))
    }
    fn cleanup(&mut self, event: Event<'_>) -> Status {
        if matches!(
            event,
            Event::Peer(PeerEvent::Closed(..)) | Event::ResourceGone(..)
        ) {
            Status::OK
        } else {
            Status::NOT_READY
        }
    }
    fn handle(state: State<Self>, msg: Incoming) -> Handler {
        let source = msg.source();
        let method = RpcMethod::call(cluster::resolve::<C>(), Both);
        msg.handle(method, move |q| async move {
            let (ctx, node, remote) =
                state.with(|s| (s.ctx.clone(), s.local.node, s.cluster_address(q.node)));
            if q.alias.is_empty() || q.alias.len() > 128 {
                return Err(Status::PROTOCOL.into());
            }
            let address = if q.node == node {
                let r = ctx.command(HostOp::Resolve(q.alias.as_str().into()));
                if r.status != Status::OK {
                    return Err(r.status.into());
                }
                r.value
            } else {
                if source.node().get() != node {
                    return Err(Status::PROTOCOL.into());
                } // 远端只查询接收节点，不代理下一跳。
                let remote = remote.ok_or(Status::UNREACHABLE)?;
                ctx.call(
                    RpcMethod::call(cluster::resolve::<C>(), Both),
                    remote,
                    (*q).clone(),
                    state.with(|s| s.resolve_ms()),
                    1024,
                )?
                .await?
                .address
            };
            if ServiceAddress::try_from(address)
                .map_err(|_| Status::PROTOCOL)?
                .node()
                .get()
                != q.node
            {
                return Err(Status::PROTOCOL.into());
            }
            Ok(cluster::Resolved { address })
        })
    }
}
#[cfg(all(test, feature = "codec_ace"))]
mod tests {
    use super::*;
    use ace_fixture_codec::FixedCodec;
    use ace_server_sdk::{AceCodec, codec::Limits};
    #[test]
    fn same_join_result_schema_encodes_and_checks_a_complete_snapshot_in_both_formats() {
        let result = cluster::JoinResult::Joined {
            snapshot: cluster::Members {
                seed_boot: 1,
                version: 5,
                members: vec![cluster::Member {
                    node: 2,
                    boot: 19,
                    endpoint: "127.0.0.1:4321".into(),
                    cluster: (2 << 48) | 3,
                }],
            },
        };
        fn bytes<C: cluster::ClusterCodec>(v: &cluster::JoinResult) -> Vec<u8> {
            let m = cluster::join_result::<C>();
            let mut dst = [0; 256];
            let b = m
                .encode_input(v, &mut dst, &mut [0; 8192], Limits::default())
                .unwrap();
            assert_eq!(&m.decode_input(m.wire(), b, Limits::default()).unwrap(), v);
            b.to_vec()
        }
        let fixed = cluster::join_result::<FixedCodec>();
        let ace = cluster::join_result::<AceCodec>();
        assert_eq!(fixed.wire().key, ace.wire().key);
        assert_eq!(fixed.wire().request, ace.wire().request);
        assert_eq!(fixed.wire().response, [0; 32]);
        assert_ne!(fixed.wire().codec, ace.wire().codec);
        let mut data = bytes::<FixedCodec>(&result);
        assert_ne!(bytes::<AceCodec>(&result), data);
        // 固定夹具：Joined(u16) + seed_boot/version(u64) 后是成员数。拒绝伪造的超额快照。
        data[18..22].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(
            fixed
                .decode_input(fixed.wire(), &data, Limits::default())
                .is_err()
        );
    }
}
