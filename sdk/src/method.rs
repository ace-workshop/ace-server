use abi_stable::std_types::ROption::{RNone, RSome};
use ace_codec::TypedMethod;
use ace_net_proto::Contract;
use ace_server_abi::native::{Method, PeerAccess, WireContract};
use ace_server_local::{LocalError, LocalOwned, LocalType, StableMessage};

/// 同一共享方法在本实现上的声明方向。
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Direction {
    /// 可接收。
    Receive = 1,
    /// 可发起。
    Send = 2,
    /// 同时可接收和发起。
    Both = 3,
}
/// 将纯协议身份逐字段装入 native 表，不复制结构内存。
pub const fn native_wire(w: Contract) -> WireContract {
    WireContract {
        key: w.key,
        codec: w.codec,
        request: w.request,
        response: w.response,
    }
}
/// 取出受信宿主交付的 wire 元数据供 typed 输入复核。
pub const fn wire_contract(w: WireContract) -> Contract {
    Contract {
        key: w.key,
        codec: w.codec,
        request: w.request,
        response: w.response,
    }
}
/// 从实际 typed codec 绑定生成纯字节 descriptor。默认非玩家方法、非 peer 控制方法。
pub const fn bytes_method<C, Q, R>(binding: TypedMethod<C, Q, R>, direction: Direction) -> Method {
    Method {
        wire: native_wire(binding.wire()),
        directions: direction as u8,
        client_callable: 0,
        peer_control: PeerAccess::None,
        request_local: RNone,
        response_local: RNone,
        max_reply_local_bytes: 0,
        max_reply_bytes: binding.max_response_bytes() as u64,
    }
}
/// 仅服务侧可选：DTO 的稳定拥有表示。普通 Vec/String 不可直接按 native 指针借用。
pub trait LocalDto: Sized {
    /// 独立于 wire 描述、有自己布局身份和递归容量的类型。
    type Stable: StableMessage;
    /// 移动到可验证的稳定表示，不运行 codec。
    fn into_stable(self) -> Self::Stable;
    /// 按本地预算检查本体和拥有容量；失败不提交对象。
    /// # Errors
    /// 超出预算返回 TooLarge，转换产生的表示在本模块释放。
    fn into_local(self, limit: usize) -> Result<LocalOwned, LocalError> {
        LocalOwned::new(self.into_stable(), limit).map_err(|(e, _)| e)
    }
}
/// 在同一个 typed 调用上另声明两种稳定表示；是否使用由宿主逐方向 select。
pub const fn local_call<C, Q: LocalDto, R: LocalDto>(
    binding: TypedMethod<C, Q, R>,
    direction: Direction,
    max_reply_local_bytes: u64,
) -> Method {
    Method {
        request_local: RSome(LocalType::of::<Q::Stable>()),
        response_local: RSome(LocalType::of::<R::Stable>()),
        max_reply_local_bytes,
        ..bytes_method(binding, direction)
    }
}
