//! 清单装配与运行时预算。读取清单不创建实例、身份目录或监听器。业务配置对宿主始终是不透明文本。
use crate::loader::{Implementations, LoadError};
use ace_server_abi::{
    Status,
    native::{Entry as MessageEntry, HostOp, SdkLimits},
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    env::consts::{DLL_PREFIX, DLL_SUFFIX},
    fmt,
    fs::File,
    io::{self, Read},
    net::{IpAddr, SocketAddr},
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

const MAX_MANIFEST: u64 = 1024 * 1024;
/// 静态装配失败；不通过此错误掩盖运行时尚不可用。
#[derive(Debug)]
pub enum ConfigError {
    /// 读取源文件失败。
    Io(io::Error),
    /// TOML 结构不合法。
    Toml(Box<toml::de::Error>),
    /// 配置内部的约束或动作顺序不满足。
    Invalid(&'static str),
    /// 加载的实际实现不满足清单。
    Library(LoadError),
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "读取清单：{e}"),
            Self::Toml(e) => write!(f, "解析清单：{e}"),
            Self::Invalid(s) => write!(f, "清单无效：{s}"),
            Self::Library(e) => write!(f, "校验角色：{e}"),
        }
    }
}
impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Toml(e) => Some(e.as_ref()),
            Self::Library(e) => Some(e),
            Self::Invalid(_) => None,
        }
    }
}
/// 调度与入站资源的有限设置，分开描述条数、字节、任务与 poll 次数。
#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Budgets {
    /// 每实例业务队列条数。
    pub mailbox_entries: NonZeroUsize,
    /// 每实例负载预算字节。
    pub payload_bytes: NonZeroU64,
    /// 同时驻留的业务任务上限。
    pub task_slots: NonZeroU32,
    /// 整个宿主唯一的多线程 runtime 的工作线程数。
    #[serde(default = "default_workers")]
    pub workers: NonZeroUsize,
    /// 包含初始化、待认领及停止中实例的全局上限。
    #[serde(default = "default_instances")]
    pub instances: NonZeroUsize,
    /// 每类独立预留的槽数（每实例 pending、timer、订阅、管理结果）。
    #[serde(default = "default_slots")]
    pub control_slots: NonZeroUsize,
    /// 最大 RPC/timer 相对期限；初始化和未认领 ticket 也受此限约束。
    #[serde(default = "default_timeout")]
    pub timeout_ms: NonZeroU64,
    /// 全节点监听槽数，包括未交付的异步监听结果。
    #[serde(default = "default_listeners")]
    pub net_listeners: NonZeroUsize,
    /// 全节点连接槽数；Closed 至 owner 显式归还期间仍占槽。
    #[serde(default = "default_slots")]
    pub net_connections: NonZeroUsize,
    /// 独立于 RPC 的整节点网络负载预算（读、事件、出口及 KCP 核心）。
    #[serde(default = "default_net_bytes")]
    pub net_buffer_bytes: NonZeroUsize,
    /// 含应用头的单帧上限；KCP 还受 127 片限制。
    #[serde(default = "default_frame")]
    pub net_frame_bytes: NonZeroUsize,
    /// 无合法 KCP 入站的超时毫秒；公共 Hello 和待确认候选另有更短期限。
    #[serde(default = "default_net_idle")]
    pub net_idle_ms: NonZeroU32,
    /// 一个执行轮次最多的用户 Future poll 数。
    pub polls_per_turn: NonZeroU32,
}
fn default_listeners() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}
fn default_net_bytes() -> NonZeroUsize {
    NonZeroUsize::new(64 << 20).unwrap()
}
fn default_frame() -> NonZeroUsize {
    NonZeroUsize::new(ace_transport::MAX_FRAME).unwrap()
}
fn default_net_idle() -> NonZeroU32 {
    NonZeroU32::new(30_000).unwrap()
}
fn default_workers() -> NonZeroUsize {
    NonZeroUsize::new(2).unwrap()
}
fn default_instances() -> NonZeroUsize {
    NonZeroUsize::new(128).unwrap()
}
fn default_slots() -> NonZeroUsize {
    NonZeroUsize::new(64).unwrap()
}
fn default_timeout() -> NonZeroU64 {
    NonZeroU64::new(30_000).unwrap()
}
impl Budgets {
    /// 交给插件的那部分预算，不把宿主队列槽或 poll 预算解释为 SDK 任务数。
    pub fn sdk_limits(&self) -> SdkLimits {
        SdkLimits {
            task_slots: self.task_slots,
            payload_bytes: self.payload_bytes,
        }
    }
}
/// 清单对具体实例授予的特殊入口能力，默认均禁用。角色声明本身不是授权。
#[derive(Default, Deserialize, Debug, Clone, Copy)]
#[serde(default, deny_unknown_fields)]
pub struct IngressGrants {
    /// 可代外部玩家转发原始字节。
    pub player_entry: bool,
    /// 可操作所属节点控制资源。
    pub peer_control: bool,
}
impl IngressGrants {
    /// 从已绑定调用实例的授权核对原语。不授予对任意资源、方法、管理或路由的权限。
    ///
    /// # Errors
    /// 普通实例尝试发送玩家入口标志或行使节点控制能力时返回 Denied。
    pub fn check(&self, op: &HostOp<'_>) -> Result<(), Status> {
        match op {
            HostOp::Send(out) | HostOp::Request(out)
                if matches!(out.entry, MessageEntry::Player { .. }) && !self.player_entry =>
            {
                Err(Status::DENIED)
            }
            HostOp::PeerListen(_)
            | HostOp::PeerConnect(..)
            | HostOp::PeerSendControl(..)
            | HostOp::PeerAdmit(..)
            | HostOp::PeerClose(_)
                if !self.peer_control =>
            {
                Err(Status::DENIED)
            }
            _ => Ok(()),
        }
    }
}
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub(crate) implementation: String,
    pub(crate) alias: String,
    pub(crate) role: Option<String>,
    #[serde(default)]
    pub(crate) grants: IngressGrants,
    #[serde(default)]
    pub(crate) manage: Vec<String>,
    #[serde(default)]
    pub(crate) prepare_dependencies: Vec<String>,
    pub(crate) config: String,
}
#[derive(Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Kind {
    Create,
    Prepare,
    Start,
}
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct Action {
    pub(crate) kind: Kind,
    pub(crate) alias: String,
}

/// 受管节点传输的部署边界。规则是授权域，不是活跃成员目录。
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerDeployment {
    pub advertised: SocketAddr,
    pub allow: Vec<PeerRule>,
}
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerRule {
    ip: IpAddr,
    nodes: [u16; 2],
    ports: [u16; 2],
}
impl PeerDeployment {
    pub(crate) fn permits(&self, node: u16, addr: SocketAddr, outbound: bool) -> bool {
        self.allow.iter().any(|r| {
            r.ip == addr.ip()
                && (r.nodes[0]..=r.nodes[1]).contains(&node)
                && (!outbound || (r.ports[0]..=r.ports[1]).contains(&addr.port()))
        })
    }
}
// 不解析角色：显式 first 和 last 之间收回其余清单实例及运行中创建的实例。
#[derive(Deserialize, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ShutdownConfig {
    first: Vec<String>,
    last: Vec<String>,
    pub(crate) stdin_stop: bool,
    pub(crate) cancel_after_ms: u64,
    pub(crate) timeout_ms: u64,
}
impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            first: vec![],
            last: vec![],
            stdin_stop: true,
            cancel_after_ms: 4_000,
            timeout_ms: 5_000,
        }
    }
}
impl ShutdownConfig {
    pub(crate) fn rank(&self, alias: &str) -> isize {
        if let Some(i) = self.first.iter().position(|a| a == alias) {
            i as isize - self.first.len() as isize
        } else {
            self.last
                .iter()
                .position(|a| a == alias)
                .map_or(0, |i| i as isize + 1)
        }
    }
}
/// 实现文件、实例配置与显式动作的只读快照，不以库名推导业务角色。
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    revision: u32,
    /// 部署指定的非零节点号。
    pub node: NonZeroU16,
    /// 每次预留的流水号数量。
    pub sequence_block: NonZeroU64,
    /// 实际运行时使用的有限预算；只读检查不消耗槽位。
    pub budgets: Budgets,
    pub(crate) peers: Option<PeerDeployment>,
    #[serde(default)]
    pub(crate) shutdown: ShutdownConfig,
    libraries: Vec<String>,
    pub(crate) instances: Vec<Entry>,
    pub(crate) actions: Vec<Action>,
}
impl Manifest {
    /// 读取并校验有限清单，不加载库或写盘。
    ///
    /// # Errors
    /// 文件过大、非法字段、重复引用、缺阶段或无效顺序均拒绝。
    pub fn read(path: &Path) -> Result<Self, ConfigError> {
        let mut raw = Vec::new();
        File::open(path)
            .map_err(ConfigError::Io)?
            .take(MAX_MANIFEST + 1)
            .read_to_end(&mut raw)
            .map_err(ConfigError::Io)?;
        if raw.len() as u64 > MAX_MANIFEST {
            return Err(ConfigError::Invalid("配置超过读取上限"));
        }
        let s = std::str::from_utf8(&raw).map_err(|_| ConfigError::Invalid("不是 UTF-8"))?;
        let m: Self = toml::from_str(s).map_err(|e| ConfigError::Toml(Box::new(e)))?;
        m.validate()?;
        Ok(m)
    }
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        let b = &self.budgets;
        if let Some(p) = &self.peers
            && (p.allow.len() > 256
                || !p.permits(self.node.get(), p.advertised, true)
                || p.allow.iter().any(|r| {
                    r.ip.is_unspecified()
                        || r.ip.is_multicast()
                        || r.nodes[0] == 0
                        || r.nodes[0] > r.nodes[1]
                        || r.ports[0] == 0
                        || r.ports[0] > r.ports[1]
                })
                || b.net_frame_bytes.get() < 140)
        {
            return Err(ConfigError::Invalid("节点授权域、通告端点或节点帧上限无效"));
        }
        if b.net_listeners.get() > 256
            || b.net_connections.get() > 4096
            || b.net_buffer_bytes.get() > 1 << 30
            || !(116..=4 << 20).contains(&b.net_frame_bytes.get())
            || b.net_idle_ms.get() > ace_transport::MAX_AGE_MS
            || b.workers.get() > 256
            || b.instances.get() > 4096
            || self.instances.len() > b.instances.get()
            || b.mailbox_entries.get() > 65536
            || b.task_slots.get() > 65536
            || b.control_slots.get() > 65536
            || b.payload_bytes.get() > 1 << 30
            || b.polls_per_turn.get() > 4096
            || b.timeout_ms.get() > 86_400_000
            || b.instances.get() * b.mailbox_entries.get() > 1 << 20
            || b.instances.get() * b.control_slots.get() > 1 << 20
            || b.instances.get() as u64 * b.payload_bytes.get() > 1 << 31
        {
            return Err(ConfigError::Invalid("运行时预算超过有限支持域"));
        }
        if self.revision != 1
            || self.libraries.is_empty()
            || self.libraries.len() > 128
            || self.instances.is_empty()
            || self.actions.len() != self.instances.len() * 3
        {
            return Err(ConfigError::Invalid("版本、库或动作数量不符"));
        }
        let mut libs = BTreeSet::new();
        for f in &self.libraries {
            if f.is_empty()
                || f.len() > 128
                || !f
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || !libs.insert(f)
            {
                return Err(ConfigError::Invalid("库名重复或不是文件基名"));
            }
        }
        if self
            .instances
            .iter()
            .filter(|e| e.grants.peer_control)
            .count()
            > 1
        {
            return Err(ConfigError::Invalid("同一节点不能授权两个节点控制实例"));
        }
        let mut stages = BTreeMap::new();
        for e in &self.instances {
            if e.alias.is_empty()
                || e.alias.len() > 128
                || e.implementation.is_empty()
                || e.config.len() > 64 * 1024
                || stages.insert(e.alias.as_str(), None).is_some()
            {
                return Err(ConfigError::Invalid("实例别名、实现或配置无效"));
            }
        }
        let stop = &self.shutdown;
        let mut aliases = BTreeSet::new();
        if stop.cancel_after_ms == 0
            || stop.cancel_after_ms >= stop.timeout_ms
            || stop.timeout_ms > 60_000
            || stop
                .first
                .iter()
                .chain(&stop.last)
                .any(|name| !stages.contains_key(name.as_str()) || !aliases.insert(name))
        {
            return Err(ConfigError::Invalid("关闭别名重复或不存在，或全局期限无效"));
        }
        for e in &self.instances {
            if e.prepare_dependencies
                .iter()
                .any(|a| !stages.contains_key(a.as_str()) || *a == e.alias)
            {
                return Err(ConfigError::Invalid("准备依赖未知或引用自身"));
            }
        }
        // 只校验显式依赖，不为操作者重排动作或自动建立服务图。
        let mut ready = BTreeSet::new();
        while ready.len() < self.instances.len() {
            let before = ready.len();
            for e in &self.instances {
                if e.prepare_dependencies
                    .iter()
                    .all(|n| ready.contains(n.as_str()))
                {
                    ready.insert(e.alias.as_str());
                }
            }
            if ready.len() == before {
                return Err(ConfigError::Invalid("准备依赖成环"));
            }
        }
        for a in &self.actions {
            let s = stages
                .get_mut(a.alias.as_str())
                .ok_or(ConfigError::Invalid("动作引用未知实例"))?;
            if !matches!(
                (*s, a.kind),
                (None, Kind::Create)
                    | (Some(Kind::Create), Kind::Prepare)
                    | (Some(Kind::Prepare), Kind::Start)
            ) {
                return Err(ConfigError::Invalid(
                    "动作必须按 create、prepare、start 的实例内顺序",
                ));
            }
            *s = Some(a.kind);
        }
        Ok(())
    }
    /// 按配置实例（非角色或库名）取得特殊入口授权，未知 alias 不得推导授权。
    pub fn ingress_grants(&self, alias: &str) -> Option<IngressGrants> {
        self.instances
            .iter()
            .find(|e| e.alias == alias)
            .map(|e| e.grants)
    }
    /// 将基名映射为所选目录中当前平台的真实动态库文件名。
    pub fn library_paths(&self, dir: &Path) -> Vec<PathBuf> {
        self.libraries
            .iter()
            .map(|n| dir.join(format!("{DLL_PREFIX}{n}{DLL_SUFFIX}")))
            .collect()
    }
    /// 将配置的实例及角色与实际库核对，不把已装载误报为 Running。
    ///
    /// # Errors
    /// 实现、角色或所需 codec/修订不一致时失败。
    pub fn check(&self, registry: &Implementations) -> Result<(), ConfigError> {
        for name in self.instances.iter().flat_map(|e| &e.manage) {
            if !registry
                .descriptors()
                .any(|d| d.implementation.as_str() == name)
            {
                return Err(ConfigError::Invalid("管理范围含未装载实现"));
            }
        }
        let selected = self
            .instances
            .iter()
            .map(|e| (e.implementation.as_str(), e.role.as_deref()))
            .collect::<Vec<_>>();
        registry
            .check_bindings(&selected)
            .map_err(ConfigError::Library)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_rejects_start_before_prepare() {
        let mut m: Manifest = toml::from_str(include_str!("../../../config/server.toml")).unwrap();
        m.validate().unwrap();
        m.actions.swap(3, 4);
        assert!(matches!(m.validate(), Err(ConfigError::Invalid(_))));
    }
    #[test]
    fn manifest_rejects_prepare_cycle_and_aggregate_allocation() {
        let mut m: Manifest =
            toml::from_str(include_str!("../../../tests/fixtures/runtime.toml")).unwrap();
        m.instances[0].prepare_dependencies.push("caller".into());
        assert!(m.validate().is_err());
        m.instances[0].prepare_dependencies.clear();
        m.budgets.instances = NonZeroUsize::new(4096).unwrap();
        m.budgets.mailbox_entries = NonZeroUsize::new(65536).unwrap();
        assert!(m.validate().is_err());
    }
    #[test]
    fn shutdown_requires_distinct_known_aliases_and_one_bounded_deadline() {
        let mut m: Manifest = toml::from_str(include_str!("../../../config/server.toml")).unwrap();
        m.shutdown.first.push("absent".into());
        assert!(m.validate().is_err());
        m.shutdown.first.pop();
        m.shutdown.last.push(m.shutdown.first[0].clone());
        assert!(m.validate().is_err());
        m.shutdown.last.pop();
        m.shutdown.cancel_after_ms = m.shutdown.timeout_ms;
        assert!(m.validate().is_err());
    }
    #[test]
    fn peer_rule_checks_observed_ip_node_range_and_advertised_port() {
        let cfg: PeerDeployment = toml::from_str(
            r#"advertised="127.0.0.1:4001"
allow=[{ip="127.0.0.1",nodes=[1,4],ports=[4001,4004]}]"#,
        )
        .unwrap();
        let observed = "127.0.0.1:50123".parse().unwrap();
        assert!(cfg.permits(2, observed, false));
        assert!(!cfg.permits(2, observed, true));
        assert!(!cfg.permits(2, "127.0.0.2:4002".parse().unwrap(), false));
        assert!(!cfg.permits(5, observed, false));
        assert!(cfg.permits(2, "127.0.0.1:4002".parse().unwrap(), true));
    }
}
