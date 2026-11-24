//! 玩家连接路由：同一实例同时持有 TCP/KCP，准入由配置的 Watchdog 裁决。
mod routes;
use ace_codec::{Codec, NoReply};
use ace_example_protocol as demo;
use ace_role_gateway as gateway;
use ace_role_watchdog as watchdog;
use ace_server_sdk::{
    Actor, ActorService, Context, Direction::*, Handler, Incoming, RpcMethod, ServiceAddress,
    State, Status, bytes_method, native::*,
};
use routes::Routes;

/// 在薄入口选择角色 codec；已绑定业务始终按原始字节转发。
pub type Gateway<C> = Actor<Routes<C>>;
/// 路由实现实际使用的全部角色信封及公共握手的静态约束。
pub trait RoutingCodec:
    gateway::GatewayCodec
    + Codec<watchdog::Open>
    + Codec<watchdog::Close>
    + Codec<demo::Admission>
    + 'static
{
}
impl<C> RoutingCodec for C where
    C: gateway::GatewayCodec
        + Codec<watchdog::Open>
        + Codec<watchdog::Close>
        + Codec<demo::Admission>
        + 'static
{
}
impl<C: RoutingCodec> Routes<C> {
    const BIND: RpcMethod<gateway::Bind, gateway::Ack> =
        RpcMethod::call(gateway::bind::<C>(), Receive)
            .local_input()
            .local_output(size_of::<gateway::Ack>() as u64);
    const KICK: RpcMethod<gateway::Kick, gateway::Ack> =
        RpcMethod::call(gateway::kick::<C>(), Receive)
            .local_input()
            .local_output(size_of::<gateway::Ack>() as u64);
    const PUSH: RpcMethod<gateway::SendToSession, gateway::Ack> =
        RpcMethod::call(gateway::send_to_session::<C>(), Receive);
    const ONLINE: RpcMethod<gateway::Online, gateway::OnlineCount> =
        RpcMethod::call(gateway::online::<C>(), Receive);
    const OPEN: RpcMethod<watchdog::Open, NoReply> = RpcMethod::send(watchdog::open::<C>(), Send);
    const CLOSE: RpcMethod<watchdog::Close, NoReply> =
        RpcMethod::send(watchdog::close::<C>(), Send);
}
impl<C: RoutingCodec> ActorService for Routes<C> {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        "gateway",
        env!("CARGO_PKG_VERSION"),
        &[Role::new(gateway::ROLE, gateway::REVISION, C::ID)],
        &[Role::new(watchdog::ROLE, watchdog::REVISION, C::ID)],
        &[
            Self::BIND.descriptor(),
            Self::KICK.descriptor(),
            Self::PUSH.descriptor(),
            Self::ONLINE.descriptor(),
            Self::OPEN.descriptor(),
            Self::CLOSE.descriptor(),
            Method {
                client_callable: 1,
                ..bytes_method(demo::admission::<C>(), Receive)
            },
        ],
    );
    fn create(ctx: Context, config: &[u8]) -> Result<Self, Status> {
        Self::new(ctx, config)
    }
    fn control(&mut self, op: Control<'_>) -> Progress {
        Progress::idle(self.event(op))
    }
    fn network_data(
        state: State<Self>,
        conn: Connection,
        bytes: &[u8],
    ) -> Result<Option<Handler>, Status> {
        state.with(|s| s.data(state.clone(), conn, bytes))
    }
    fn cleanup(&mut self, e: Event<'_>) -> Status {
        match e {
            // Gate 已终结：不再发送普通 Close；Watchdog 的一次性订阅负责整批失效。
            Event::Network(NetEvent::Closed(c, _)) => self.cleanup_closed(c),
            Event::ResourceGone(..) => Status::OK,
            _ => Status::NOT_READY,
        }
    }
    fn handle(state: State<Self>, msg: Incoming) -> Handler {
        if !matches!(msg.entry(), Entry::Service) {
            return msg.reject(Status::DENIED);
        }
        let src = msg.source();
        match msg.wire().key {
            k if k == Self::BIND.descriptor().wire.key && state.with(|s| s.is_watchdog(src)) => msg
                .handle(Self::BIND, move |q| async move {
                    state.with(|s| s.bind(src, &q))?;
                    Ok(gateway::Ack {})
                }),
            k if k == Self::KICK.descriptor().wire.key && state.with(|s| s.is_watchdog(src)) => msg
                .handle(Self::KICK, move |q| async move {
                    state.with(|s| s.kick(src, &q))?;
                    Ok(gateway::Ack {})
                }),
            k if k == Self::PUSH.descriptor().wire.key => {
                msg.handle(Self::PUSH, move |q| async move {
                    state.with(|s| s.push(src, &q))?;
                    Ok(gateway::Ack {})
                })
            }
            k if k == Self::ONLINE.descriptor().wire.key
                && state.with(|s| s.managers.contains(&src)) =>
            {
                msg.handle(Self::ONLINE, move |_| async move {
                    Ok(gateway::OnlineCount {
                        count: state.with(|s| s.online()),
                    })
                })
            }
            _ => msg.reject(Status::DENIED),
        }
    }
}
