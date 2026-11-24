//! 真实服务 ABI 的生命周期夹具，不是第四个内置角色。
mod echo;
mod network;
use abi_stable::{
    prefix_type::PrefixTypeTrait,
    std_types::{
        ROption::{RNone, RSome},
        RStr,
    },
};
use ace_server_sdk::{Context, Service, Status, api, native::*};
use std::{
    future::Future,
    pin::Pin,
    task::{Poll, Waker},
};

// 夹具的空字节契约：SHA-256(b"test.policy/v1: bytes; zero fields; empty")。
const EMPTY: [u8; 32] = [
    0xd1, 0x73, 0x9f, 0xeb, 0xaa, 0x61, 0xdc, 0x7c, 0x92, 0x8b, 0xcf, 0x13, 0x73, 0x6e, 0x67, 0x54,
    0x21, 0x2a, 0x06, 0xc9, 0x73, 0x89, 0xd5, 0x8d, 0xa2, 0x8b, 0x2d, 0xbb, 0x2b, 0xa8, 0x8a, 0x06,
];
const PLAYER: Method = Method {
    wire: WireContract {
        key: 81,
        codec: [1, 1, 1],
        request: EMPTY,
        response: EMPTY,
    },
    directions: 1,
    client_callable: 1,
    peer_control: PeerAccess::None,
    request_local: RNone,
    response_local: RNone,
    max_reply_bytes: 0,
    max_reply_local_bytes: 0,
};
const PEER: Method = Method {
    wire: WireContract {
        key: 82,
        response: [0; 32],
        ..PLAYER.wire
    },
    client_callable: 0,
    peer_control: PeerAccess::Bootstrap,
    ..PLAYER
};

enum Listener {
    NotNeeded,
    Needed,
    Waiting(ResourceHandle),
    Finished(Status),
}
// 在原库内直接观测普通 Future 的恢复和析构，而不是从实例的终态倒推。
struct Work {
    context: Context,
    first: bool,
}
impl Future for Work {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
        if std::mem::take(&mut self.first) {
            self.context.wake();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        self.context.command(HostOp::Log(0, "drop-work".into()));
    }
}
struct Policy {
    context: Context,
    work: Option<Work>,
    panic_on_poll: bool,
    listener: Listener,
    network: Option<network::Owner>,
}
impl Policy {
    fn trace(&self, event: &'static str) {
        self.context.command(HostOp::Log(0, RStr::from_str(event)));
    }
    fn listen_result(&mut self, handle: ResourceHandle, status: Status) -> Status {
        if !matches!(self.listener, Listener::Waiting(w) if w == handle) {
            return Status::NOT_FOUND;
        }
        self.listener = Listener::Finished(status);
        self.context
            .command(HostOp::LifecycleReady(LifecycleAction::Prepare))
            .status
    }
}
impl Service for Policy {
    const DESCRIPTOR: Descriptor = Descriptor::new(
        "test.policy",
        env!("CARGO_PKG_VERSION"),
        &[],
        &[],
        &[PLAYER, PEER, echo::ECHO],
    );
    fn create(context: Context, config: &[u8]) -> Result<Self, Status> {
        match config {
            b"reject" => Err(Status::DENIED),
            b"panic-create" => panic!("夹具：create panic"),
            b"" | b"panic-poll" | b"deferred" | b"network" | b"network-probe" => Ok(Self {
                context,
                work: None,
                network: matches!(config, b"network" | b"network-probe")
                    .then(|| network::Owner::new(config == b"network-probe")),
                panic_on_poll: config == b"panic-poll",
                listener: if config == b"deferred" {
                    Listener::Needed
                } else {
                    Listener::NotNeeded
                },
            }),
            _ => Err(Status::PROTOCOL),
        }
    }
    fn on_message(&mut self, msg: Message<'_>, slot: &mut LocalSlot) -> Status {
        if msg.request.wire.key == echo::ECHO.wire.key {
            let status = echo::handle(&self.context, msg, slot);
            if status == Status::OK && msg.charge_token != 0 {
                self.context
                    .command(HostOp::ChargeReleased(msg.charge_token));
            }
            return status;
        }
        if !matches!(msg.request.entry, Entry::Player { push_session: RSome(s) } if s.get() == 29)
            || msg.reply_token != 7
            || !msg.request.body.bytes.is_empty()
        {
            return Status::PROTOCOL;
        }
        self.context
            .command(HostOp::Reply(
                msg.reply_token,
                Reply::Error(Status::NOT_FOUND),
            ))
            .status
    }
    fn on_control(&mut self, event: Control<'_>, _: &mut LocalSlot) -> Progress {
        if let Some(net) = &mut self.network {
            return net.control(&self.context, event);
        }
        let status = match event {
            Control::Prepare if matches!(self.listener, Listener::Needed) => {
                let r = self
                    .context
                    .command(HostOp::NetListen(Transport::Tcp, "127.0.0.1:0".into()));
                if r.status != Status::OK {
                    return Progress::idle(r.status);
                }
                self.listener = Listener::Waiting(ResourceHandle {
                    id: r.value,
                    generation: r.generation,
                });
                Status::PENDING
            }
            Control::LifecycleEvent(
                LifecycleAction::Prepare,
                Event::Network(NetEvent::ListenReady(handle, Transport::Tcp, _)),
            ) => self.listen_result(handle, Status::OK),
            Control::LifecycleEvent(
                LifecycleAction::Prepare,
                Event::Network(NetEvent::ListenFailed(handle, status)),
            ) => self.listen_result(handle, status),
            Control::LifecycleEvent(_, Event::Peer(PeerEvent::Control(_, _, wire, bytes)))
                if wire == PEER.wire && bytes.is_empty() =>
            {
                Status::OK
            }
            Control::LifecyclePoll(LifecycleAction::Prepare, _) => match self.listener {
                Listener::Finished(s) => s,
                Listener::Waiting(_) => Status::PENDING,
                _ => Status::PROTOCOL,
            },
            Control::Prepare => Status::OK,
            Control::Start => {
                self.work = Some(Work {
                    context: self.context.clone(),
                    first: true,
                });
                Status::OK
            }
            Control::Poll(_) => {
                if self.panic_on_poll {
                    panic!("夹具：poll panic")
                }
                if let Some(work) = &mut self.work
                    && Pin::new(work)
                        .poll(&mut std::task::Context::from_waker(Waker::noop()))
                        .is_ready()
                {
                    self.work = None;
                }
                return Progress {
                    status: Status::OK,
                    more_ready: u8::from(self.work.is_some()),
                    task_capacity: 1,
                };
            }
            _ => Status::NOT_READY,
        };
        Progress::idle(status)
    }
    fn on_cleanup(&mut self, event: Event<'_>, _: &mut LocalSlot) -> Status {
        let op = match event {
            Event::Network(NetEvent::Closed(conn, _)) => {
                if let Some(net) = &self.network {
                    net.trace_conn(&self.context, conn, "cleanup");
                    self.trace("net-cleanup");
                }
                HostOp::NetReleaseClosed(conn)
            }
            Event::Completion {
                pending,
                reply: Reply::Error(Status::TIMEOUT),
                ..
            } => HostOp::CompletionConsumed(pending),
            _ => return Status::NOT_READY,
        };
        self.context.command(op).status
    }
    fn cancel(&mut self) {
        self.work = None;
        self.trace("cancel");
    }
}
impl Drop for Policy {
    fn drop(&mut self) {
        self.trace("destroy");
    }
}
extern "C" fn fixture_table() -> &'static ServiceApi {
    if std::env::var_os("ACE_FIXTURE_WRONG_VERSION").is_none() {
        return api::<Policy>();
    }
    static BAD: std::sync::OnceLock<ServiceApi> = std::sync::OnceLock::new();
    BAD.get_or_init(|| ServiceApi {
        abi_version: u32::MAX,
        ..*api::<Policy>()
    })
}
#[abi_stable::export_root_module]
fn root() -> ServiceModuleRef {
    ServiceModule {
        ace_service_v1: fixture_table,
    }
    .leak_into_prefix()
}
