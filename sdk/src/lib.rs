//! 服务开发接口，依赖协议而不依赖宿主实现。
mod service;
pub use ace_server_abi::{self as abi, Status, native};
pub use service::{Context, Service, api, export};
mod method;
#[cfg(feature = "codec_ace")]
pub use ace_codec::AceCodec;
pub use ace_codec::{self as codec, TypedMethod};
pub use ace_net_proto::ServiceAddress;
pub use ace_server_local::{LocalError, LocalOwned, StableMessage};
pub use method::{Direction, LocalDto, bytes_method, local_call, native_wire, wire_contract};
mod actor;
mod rpc;
mod tasks;
pub use actor::{Actor, ActorService, State};
pub use rpc::{FromLocal, Incoming, Managed, ManagedBytes, RpcMethod, TypedRpcError};
pub use tasks::Handler;
mod timer;
pub use timer::Timer;
