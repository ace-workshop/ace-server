//! 业务帧借用调用方的完整输入，不分配、不执行 body decoder。
use crate::{
    ServiceAddress,
    bytes::{Reader, Writer},
};
use core::{fmt, num::NonZeroU64};

/// 逐方法 wire 声明；不是稳定本地布局，也不是身份认证。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contract {
    /// 逻辑方法键，零保留。
    pub key: u32,
    /// codec、修订、选项；未知格式可由不解释 body 的中间层转发。
    pub codec: [u32; 3],
    /// 请求规范字节的 SHA-256。
    pub request: [u8; 32],
    /// 成功响应规范字节的 SHA-256；单向为零。
    pub response: [u8; 32],
}
impl Contract {
    /// 对已查到的方法核对三元组及两个摘要，不试解码。
    /// # Errors
    /// 键未命中返回 UnknownMethod，其余身份差异返回 Protocol。
    pub fn check(self, expected: Self) -> Result<(), FrameError> {
        if self.key != expected.key {
            Err(FrameError::UnknownMethod)
        } else if self != expected {
            Err(FrameError::Protocol)
        } else {
            Ok(())
        }
    }
}
/// 帧结构、限额或预期方法不匹配。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// 帧头或声称的消息尚不完整。
    Truncated,
    /// 修订、形状、来源域、关联身份或契约不符。
    Protocol,
    /// 方法键不是所选方法。
    UnknownMethod,
    /// 长度、预留、期限或可写空间越界。
    Limit,
}
impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "帧不完整",
            Self::Protocol => "协议不符",
            Self::UnknownMethod => "未知方法",
            Self::Limit => "帧超过限额",
        })
    }
}
impl core::error::Error for FrameError {}
/// 公网和节点 RPC 可传递的无体终态。不是本机即时操作结果域。
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcError {
    /// 队列或额度不足。
    Busy = 1,
    /// 协议不符。
    Protocol = 2,
    /// 目标已故障。
    Faulted = 3,
    /// 目标已停止。
    Stopped = 5,
    /// 成功结果放不进预留。
    TooSmall = 7,
    /// 目标不存在。
    NotFound = 8,
    /// 处理期限已到。
    Timeout = 10,
    /// 节点不可达。
    Unreachable = 11,
    /// 未注册该方法。
    UnknownMethod = 12,
    /// 本次处理发生 panic。
    Panicked = 13,
}
impl TryFrom<u32> for RpcError {
    type Error = FrameError;
    fn try_from(n: u32) -> Result<Self, FrameError> {
        Ok(match n {
            1 => Self::Busy,
            2 => Self::Protocol,
            3 => Self::Faulted,
            5 => Self::Stopped,
            7 => Self::TooSmall,
            8 => Self::NotFound,
            10 => Self::Timeout,
            11 => Self::Unreachable,
            12 => Self::UnknownMethod,
            13 => Self::Panicked,
            _ => return Err(FrameError::Protocol),
        })
    }
}
/// 接收端配置的上限；响应还须单独服从发起请求的预留。
#[derive(Clone, Copy, Debug)]
pub struct FrameLimits {
    /// 实际 wire body 字节上限（不是解包后的长度）。
    pub body_bytes: u32,
    /// 请求可预留及响应可携带的最大 wire 字节数。
    pub response_bytes: u32,
    /// 从本端单调时钟起算的最大毫秒数。
    pub timeout_ms: u64,
}
/// 调用方的关联号与响应约束；不含跨机绝对时间。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Call {
    /// 只在所属连接代次和调用方向有效。
    pub session: NonZeroU64,
    /// 有限相对毫秒数。
    pub timeout_ms: NonZeroU64,
    /// 成功响应的实际字节容量，允许为零。
    pub max_response_bytes: u32,
}
/// 单向、请求与两类终态响应；空字节成功不是错误。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    /// 不建 session，也不预留响应。
    Send,
    /// 需要回包的请求。
    Request(Call),
    /// 与原请求关联的响应。
    Response {
        /// 回显原请求的 session。
        session: NonZeroU64,
        /// None 为成功；Some 要求 body 为空。
        error: Option<RpcError>,
    },
}
/// 公共帧没有内部地址、外部入口标志或源 Gate 的推送令牌。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    /// 固定宽度方法身份。
    pub wire: Contract,
    /// 决定扩展和状态语义。
    pub message: Message,
    /// 已编码业务体；中间层可原样转发。
    pub body: &'a [u8],
}
/// 仅供节点间路由，必须由接收宿主另行验证 peer/代次和授权。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeEntry {
    /// 普通服务间消息或响应。
    Service,
    /// 获授权 Gate 代玩家发出的请求或单向消息；可附源 Gate 私有非零令牌。
    Player(Option<NonZeroU64>),
}
/// 节点间服务身份，不能当玩家自报的来源。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route {
    /// 由发送宿主根据实例上下文确定。
    pub source: ServiceAddress,
    /// 目标实例，不是角色名。
    pub destination: ServiceAddress,
    /// 入口标志只在内部域使用。
    pub entry: NodeEntry,
}
/// 地址路由帧与绑定到 peer 连接本身的角色控制帧不混用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeFrame<'a> {
    /// 仅在已准入路由上交付。
    Routed(Route, Frame<'a>),
    /// 无 RPC session，不假造服务地址；接收宿主另核验 owner、方法与准入阶段。
    Control(Contract, &'a [u8]),
}

const HEADER: usize = 104;
const NODE: u16 = 0x100;
const CONTROL: u16 = 0x200;
const PLAYER: u16 = 1;
const NODE_SEND: u16 = NODE | 1;
const NODE_REQUEST: u16 = NODE | 2;
const NODE_RESPONSE: u16 = NODE | 3;
const PEER_CONTROL: u16 = CONTROL | 1;
#[derive(Clone, Copy)]
enum Domain {
    Public,
    Routed(Route),
    Control,
}
impl Domain {
    fn prefix(self) -> (u16, u16) {
        match self {
            Self::Public => (0, 0),
            Self::Routed(r) => (
                if matches!(r.entry, NodeEntry::Player(_)) {
                    PLAYER
                } else {
                    0
                },
                NODE,
            ),
            Self::Control => (0, CONTROL),
        }
    }
}
/// 已验证的头部预检。仅凭头和固定扩展即可查白名单或 pending、复核容量，再取得 body 空间。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHead {
    /// 查找本地方法并比较两个方向的 wire 契约。
    pub wire: Contract,
    /// 请求或响应的关联身份。
    pub message: Message,
    /// 实际线上 body 长度。
    pub body_len: u32,
    /// 含域专属扩展的完整帧长度。
    pub frame_len: usize,
}
impl FrameHead {
    /// 对响应头检查关联与该次请求的预留。不需要先接收、分配或解码 body。
    /// # Errors
    /// 不是该调用的合法响应、成功契约不符、超出预留或错误带 body 时失败。
    pub fn check_response(
        &self,
        expected: Contract,
        call: Call,
    ) -> Result<Option<RpcError>, FrameError> {
        check_response(
            self.message,
            self.wire,
            self.body_len as usize,
            expected,
            call,
        )
    }
}
fn check_response(
    message: Message,
    wire: Contract,
    body_len: usize,
    expected: Contract,
    call: Call,
) -> Result<Option<RpcError>, FrameError> {
    let Message::Response { session, error } = message else {
        return Err(FrameError::Protocol);
    };
    if session != call.session || wire.key != expected.key || expected.response == [0; 32] {
        return Err(FrameError::Protocol);
    }
    if let Some(e) = error {
        return if body_len == 0 {
            Ok(Some(e))
        } else {
            Err(FrameError::Protocol)
        };
    }
    wire.check(expected)?;
    if body_len > call.max_response_bytes as usize {
        return Err(FrameError::Limit);
    }
    Ok(None)
}
struct Header {
    wire: Contract,
    message: Message,
    domain: Domain,
    body_len: u32,
}
impl Header {
    fn len(&self, limits: FrameLimits) -> Result<usize, FrameError> {
        if self.wire.key == 0 {
            return Err(FrameError::Protocol);
        }
        if self.body_len > limits.body_bytes
            || (matches!(self.message, Message::Response { .. })
                && self.body_len > limits.response_bytes)
        {
            return Err(FrameError::Limit);
        }
        let extra = match self.message {
            Message::Send if self.wire.response == [0; 32] => 0,
            Message::Request(c) if self.wire.response != [0; 32] => {
                if c.timeout_ms.get() > limits.timeout_ms
                    || c.max_response_bytes > limits.response_bytes
                {
                    return Err(FrameError::Limit);
                }
                12
            }
            Message::Response { error: None, .. } if self.wire.response != [0; 32] => 0,
            Message::Response { error: Some(_), .. } if self.body_len == 0 => 0,
            _ => return Err(FrameError::Protocol),
        };
        if self.wire.request == [0; 32]
            && !matches!(self.message, Message::Response { error: Some(_), .. })
        {
            return Err(FrameError::Protocol);
        }
        let route = match self.domain {
            Domain::Public => 0,
            Domain::Control if self.message == Message::Send => 0,
            Domain::Routed(Route {
                entry: NodeEntry::Service,
                ..
            }) => 16,
            Domain::Routed(_) if !matches!(self.message, Message::Response { .. }) => 24,
            _ => return Err(FrameError::Protocol),
        };
        (HEADER + extra + route)
            .checked_add(self.body_len as usize)
            .ok_or(FrameError::Limit)
    }
    fn inspect(&self, limits: FrameLimits) -> Result<FrameHead, FrameError> {
        Ok(FrameHead {
            wire: self.wire,
            message: self.message,
            body_len: self.body_len,
            frame_len: self.len(limits)?,
        })
    }
    fn read(r: &mut Reader<'_>, node: bool) -> Result<Self, FrameError> {
        let (flags, kind) = r.prefix()?;
        let group = kind & 0xff00;
        if !matches!(
            (node, kind, flags),
            (false, 1..=3, 0)
                | (true, NODE_SEND | NODE_REQUEST, 0 | PLAYER)
                | (true, NODE_RESPONSE | PEER_CONTROL, 0)
        ) {
            return Err(FrameError::Protocol);
        }
        let status = r.u32()?;
        let wire = Contract {
            key: r.u32()?,
            codec: [r.u32()?, r.u32()?, r.u32()?],
            request: r.take()?,
            response: r.take()?,
        };
        let body_len = r.u32()?;
        let session = NonZeroU64::new(r.u64()?);
        let message = match (kind & 0xff, session, status) {
            (1, None, 0) => Message::Send,
            (2, Some(session), 0) => Message::Request(Call {
                session,
                max_response_bytes: r.u32()?,
                timeout_ms: NonZeroU64::new(r.u64()?).ok_or(FrameError::Protocol)?,
            }),
            (3, Some(session), s) => Message::Response {
                session,
                error: if s == 0 { None } else { Some(s.try_into()?) },
            },
            _ => return Err(FrameError::Protocol),
        };
        let domain = match group {
            NODE => Domain::Routed(Route {
                source: r.u64()?.try_into().map_err(|_| FrameError::Protocol)?,
                destination: r.u64()?.try_into().map_err(|_| FrameError::Protocol)?,
                entry: if flags == PLAYER {
                    NodeEntry::Player(NonZeroU64::new(r.u64()?))
                } else {
                    NodeEntry::Service
                },
            }),
            CONTROL => Domain::Control,
            _ => Domain::Public,
        };
        Ok(Self {
            wire,
            message,
            domain,
            body_len,
        })
    }
    fn write(&self, w: &mut Writer<'_>) -> Result<(), FrameError> {
        let (flags, group) = self.domain.prefix();
        let (kind, session, status) = match self.message {
            Message::Send => (1, 0, 0),
            Message::Request(c) => (2, c.session.get(), 0),
            Message::Response { session, error } => {
                (3, session.get(), error.map_or(0, |e| e as u32))
            }
        };
        w.prefix(flags, group | kind)?;
        for n in [status, self.wire.key].into_iter().chain(self.wire.codec) {
            w.put(&n.to_le_bytes())?;
        }
        w.put(&self.wire.request)?;
        w.put(&self.wire.response)?;
        w.put(&self.body_len.to_le_bytes())?;
        w.put(&session.to_le_bytes())?;
        if let Message::Request(c) = self.message {
            w.put(&c.max_response_bytes.to_le_bytes())?;
            w.put(&c.timeout_ms.get().to_le_bytes())?;
        }
        if let Domain::Routed(r) = self.domain {
            w.put(&r.source.bits().to_le_bytes())?;
            w.put(&r.destination.bits().to_le_bytes())?;
            if let NodeEntry::Player(token) = r.entry {
                w.put(&token.map_or(0, NonZeroU64::get).to_le_bytes())?;
            }
        }
        Ok(())
    }
}
fn parse(src: &[u8], limits: FrameLimits, node: bool) -> Result<(Domain, Frame<'_>), FrameError> {
    let mut r = Reader(src);
    let h = Header::read(&mut r, node)?;
    let len = h.len(limits)?;
    match src.len().cmp(&len) {
        core::cmp::Ordering::Less => Err(FrameError::Truncated),
        core::cmp::Ordering::Greater => Err(FrameError::Protocol),
        core::cmp::Ordering::Equal => Ok((
            h.domain,
            Frame {
                wire: h.wire,
                message: h.message,
                body: r.0,
            },
        )),
    }
}
impl<'a> Frame<'a> {
    /// 仅用帧头和对应扩展取得身份与完整长度；先核对限额再为 TCP 消息分配空间。
    /// # Errors
    /// 头未收全、未知类型、字段矛盾或超限时失败。
    pub fn inspect(prefix: &[u8], limits: FrameLimits) -> Result<FrameHead, FrameError> {
        Header::read(&mut Reader(prefix), false)?.inspect(limits)
    }
    /// 从一个完整公共帧借用 body；多余字节不是第二个隐式消息。
    /// # Errors
    /// 结构、长度、状态域或限额不合规时失败。
    pub fn decode(src: &'a [u8], limits: FrameLimits) -> Result<Self, FrameError> {
        Ok(parse(src, limits, false)?.1)
    }
    /// Gate 的玩家入站边界，只接受公共请求或单向发送，不接受响应或节点帧。
    /// # Errors
    /// 非法来源域、消息形状或超限时失败；目标的方法白名单仍由受信宿主核对。
    pub fn from_player(src: &'a [u8], limits: FrameLimits) -> Result<Self, FrameError> {
        let f = Self::decode(src, limits)?;
        if matches!(f.message, Message::Response { .. }) {
            Err(FrameError::Protocol)
        } else {
            Ok(f)
        }
    }
    /// 写一个完整公共帧，返回有效长度。
    /// # Errors
    /// 超限、元数据矛盾或目标空间不足时不交出帧。
    pub fn encode_into(&self, dst: &mut [u8], limits: FrameLimits) -> Result<usize, FrameError> {
        self.write(Domain::Public, dst, limits)
    }
    fn write(
        &self,
        domain: Domain,
        dst: &mut [u8],
        limits: FrameLimits,
    ) -> Result<usize, FrameError> {
        let h = Header {
            wire: self.wire,
            message: self.message,
            domain,
            body_len: self.body.len().try_into().map_err(|_| FrameError::Limit)?,
        };
        let n = h.len(limits)?;
        let mut w = Writer(dst.get_mut(..n).ok_or(FrameError::Limit)?);
        h.write(&mut w)?;
        w.put(self.body)?;
        Ok(n)
    }
    /// 比较成功响应与调用方保存的 session、方法、两摘要及容量；无体失败只关联方法和 session。
    /// # Errors
    /// 非该调用的响应、wire 不兼容或超过预留时失败。外层先核对连接代次及节点地址。
    pub fn check_response(
        &self,
        wire: Contract,
        call: Call,
    ) -> Result<Result<&'a [u8], RpcError>, FrameError> {
        match check_response(self.message, self.wire, self.body.len(), wire, call)? {
            Some(e) => Ok(Err(e)),
            None => Ok(Ok(self.body)),
        }
    }
}
impl<'a> NodeFrame<'a> {
    /// 节点链路预检：有地址的是路由帧，None 是连接控制帧。可在接收 body 前对照 peer/代次。
    /// # Errors
    /// 类型、地址、入口、期限或限额不匹配时失败。
    pub fn inspect(
        prefix: &[u8],
        limits: FrameLimits,
    ) -> Result<(Option<Route>, FrameHead), FrameError> {
        let h = Header::read(&mut Reader(prefix), true)?;
        let route = match h.domain {
            Domain::Routed(r) => Some(r),
            Domain::Control => None,
            Domain::Public => return Err(FrameError::Protocol),
        };
        Ok((route, h.inspect(limits)?))
    }
    /// 借用完整内部帧。调用者仍须对照已授权 peer 及连接代次核验来源。
    /// # Errors
    /// 结构、地址、长度或入口标志非法时失败。
    pub fn decode(src: &'a [u8], limits: FrameLimits) -> Result<Self, FrameError> {
        let (d, f) = parse(src, limits, true)?;
        match d {
            Domain::Routed(r) => Ok(Self::Routed(r, f)),
            Domain::Control => Ok(Self::Control(f.wire, f.body)),
            Domain::Public => Err(FrameError::Protocol),
        }
    }
    /// 写一个内部帧，不转换业务 body。
    /// # Errors
    /// 额度、形状或空间不符时不交出帧。
    pub fn encode_into(&self, dst: &mut [u8], limits: FrameLimits) -> Result<usize, FrameError> {
        match *self {
            Self::Routed(r, f) => f.write(Domain::Routed(r), dst, limits),
            Self::Control(wire, body) => Frame {
                wire,
                message: Message::Send,
                body,
            }
            .write(Domain::Control, dst, limits),
        }
    }
}
