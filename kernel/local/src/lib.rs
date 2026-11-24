//! 本机服务之间的稳定只读消息。普通 Rust DTO、trait 对象和线程绑定状态不使用此接口。
//! 装载方必须在模块映射仍驻留时初始化 abi_stable；网络协议不依赖本包。

use abi_stable::{
    StableAbi,
    abi_stability::check_layout_compatibility,
    std_types::{
        ROption,
        ROption::{RNone, RSome},
    },
    type_layout::TypeLayout,
};
use std::{
    fmt, mem,
    panic::{AssertUnwindSafe, catch_unwind},
};

/// 可以由匹配声明的独立库只读访问的稳定消息。
///
/// # Safety
/// 必须是具有完整可检查布局的拥有型 DTO；只能包含有效性与语义由同一 KEY
/// 定义的稳定字段（如整数、RString、RVec 的同类元素）。不可隐藏不稳定字段、
/// 普通 Rust 容器、裸借用、业务 vtable、地址敏感或线程绑定状态。相同 KEY
/// 不得复用于互斥的不变量。不可通过共享引用调整容量或破坏值的有效性。析构不访问发送 Actor；必须可在另一线程释放。
pub unsafe trait StableMessage: StableAbi + Send + 'static {
    /// 跨构建的本地语义标识，与用于线上解码的摘要独立。
    const KEY: [u8; 32];
    /// 所有直接和间接拥有的堆容量（字节）；加乘溢出时返回 None。
    fn heap_bytes(&self) -> Option<usize>;
}

/// 本地布局声明；两库必须同时匹配语义、尺寸、对齐和递归布局。
#[repr(C)]
#[derive(Copy, Clone, StableAbi)]
pub struct LocalType {
    key: [u8; 32],
    layout: &'static TypeLayout,
}
impl LocalType {
    /// 从已声明的共享类型生成真实布局，不使用 Rust TypeId 或 wire digest 代替。
    pub const fn of<T: StableMessage>() -> Self {
        Self {
            key: T::KEY,
            layout: T::LAYOUT,
        }
    }
    /// 仅在宿主或已由宿主初始化的库内调用。对完整 DTO 要求两个方向均兼容。
    pub fn matches(self, other: Self) -> bool {
        self.key == other.key
            && self.layout.size() == other.layout.size()
            && self.layout.alignment() == other.layout.alignment()
            && check_layout_compatibility(self.layout, other.layout).is_ok()
            && check_layout_compatibility(other.layout, self.layout).is_ok()
    }
}

/// 在本地对象被拒绝、布局不符或预算不足时返回的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalError {
    /// 类型声明或布局不兼容。
    Incompatible,
    /// 对象递归容量溢出或超限。
    TooLarge,
    /// 先前的记账凭证仍随当前对象持有。
    AlreadyCharged,
}
impl fmt::Display for LocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Incompatible => "本地消息类型或布局不符",
            Self::TooLarge => "本地消息的拥有容量过大",
            Self::AlreadyCharged => "本地消息已记账",
        })
    }
}
impl std::error::Error for LocalError {}

// 所有权凭证也只需要“在原模块丢弃一次”。使用两个 C 字段，不把普通 Rust
// Box 或带有非 C 布局标记的 DynTrait 作为按值 FFI 返回值。
#[repr(C)]
#[derive(StableAbi)]
struct Allocation {
    data: *mut (),
    release: unsafe extern "C" fn(*mut ()),
}
impl Allocation {
    fn new<T: Send + 'static>(value: T) -> Self {
        Self {
            data: Box::into_raw(Box::new(value)).cast(),
            release: release_box::<T>,
        }
    }
}
// SAFETY: data 是唯一的 Box<T: Send>，没有 Clone 或可变视图；release 来自同一
// 常驻模块并在原分配器释放，无线程亲和。没有为该句柄实现 Sync。
unsafe impl Send for Allocation {}
impl Drop for Allocation {
    fn drop(&mut self) {
        // SAFETY: 句柄唯一且字段私有，data 与 release 配对构造。
        unsafe { (self.release)(self.data) };
    }
}

/// 单一拥有者。不提供 Clone 或可变类型化视图；析构调用生产模块，不借用源实例。
#[repr(C)]
#[derive(StableAbi)]
pub struct LocalOwned {
    kind: LocalType,
    body: Allocation,
    bytes: usize,
    // 字段按声明顺序析构，body 释放后才归还宿主预留的额度。
    lease: ROption<Allocation>,
}

unsafe extern "C" fn release_box<T: Send + 'static>(data: *mut ()) {
    if catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: 只由 Allocation::drop 以原构造配对的指针调用一次。
        drop(unsafe { Box::from_raw(data.cast::<T>()) });
    }))
    .is_err()
    {
        // 析构阶段的第二重失败无法可靠恢复，不能让 unwind 穿过 C 边界。
        std::process::abort();
    }
}
impl LocalOwned {
    /// 在接纳前校验消息本体及递归拥有容量；不按编码长度或元素个数收费。
    ///
    /// # Errors
    /// 超限或溢出返回 TooLarge 和原值，尚未转移给下游。
    pub fn new<T: StableMessage>(value: T, limit: usize) -> Result<Self, (LocalError, T)> {
        let Some(bytes) = value
            .heap_bytes()
            .and_then(|heap| mem::size_of::<T>().checked_add(heap))
            .filter(|&n| n <= limit)
        else {
            return Err((LocalError::TooLarge, value));
        };
        Ok(Self {
            kind: LocalType::of::<T>(),
            body: Allocation::new(value),
            bytes,
            lease: RNone,
        })
    }
    /// 随实际对象保存的源布局；宿主不得只信路由请求里另填的标签。
    pub fn local_type(&self) -> LocalType {
        self.kind
    }
    /// 供 host 在接纳前预扣的 DTO 拥有字节数；信封/队列槽独立计数。
    pub fn owned_bytes(&self) -> usize {
        self.bytes
    }
    /// 当前值是否已绑定一次性额度凭证；在新的提交取走前可无损拒绝重复记账。
    pub fn is_charged(&self) -> bool {
        self.lease.is_some()
    }
    /// 将宿主预留的唯一凭证附着到对象；即使源 Actor 已停止，最终持有者才归还。
    ///
    /// # Errors
    /// 不覆盖已有凭证。传入但未安装的凭证按普通 RAII 回收。
    pub fn attach_lease<P: Send + 'static>(&mut self, permit: P) -> Result<(), LocalError> {
        if self.lease.is_some() {
            return Err(LocalError::AlreadyCharged);
        }
        self.lease = RSome(Allocation::new(permit));
        Ok(())
    }
    /// 校验后只读访问；引用的生命周期不会超过拥有者。
    ///
    /// # Errors
    /// 不匹配返回 Incompatible，不访问对象，也不尝试解码。
    pub fn view<T: StableMessage>(&self) -> Result<&T, LocalError> {
        if !LocalType::of::<T>().matches(self.kind) {
            return Err(LocalError::Incompatible);
        }
        // SAFETY: 插件可信且常驻；不变量由 StableMessage 的语义标识与真实递归
        // 布局双向检查共同约束。指针来自对应 Box，活到 self 析构，无可变访问。
        Ok(unsafe { &*self.body.data.cast::<T>() })
    }
}
