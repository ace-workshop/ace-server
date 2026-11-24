use super::*;
use ace_server_sdk::{
    TypedMethod,
    codec::{Codec, Limits, NoReply},
    native_wire, wire_contract,
};
use cluster::{Heartbeat, Join, JoinResult, Leave, Member, Members};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    net::SocketAddr,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    seed: u16,
    seed_endpoint: SocketAddr,
    ports: [u16; 2],
    #[serde(default = "members")]
    max_members: usize,
    #[serde(default = "heartbeat")]
    heartbeat_ms: u64,
    #[serde(default = "lease")]
    lease_ms: u64,
    #[serde(default = "backoff")]
    retry_max_ms: u64,
    #[serde(default = "snapshot")]
    snapshot_bytes: usize,
    #[serde(default = "fanout")]
    notify_fanout: usize,
    leave_with: Option<String>,
}
fn members() -> usize {
    32
}
fn heartbeat() -> u64 {
    250
}
fn lease() -> u64 {
    3000
}
fn backoff() -> u64 {
    2000
}
fn snapshot() -> usize {
    16384
}
fn fanout() -> usize {
    8
}
impl Settings {
    fn endpoint(&self, addr: SocketAddr) -> bool {
        !addr.ip().is_unspecified()
            && !addr.ip().is_multicast()
            && (self.ports[0]..=self.ports[1]).contains(&addr.port())
    }
    fn validate(&self, local: &Member, cap: u64) -> Result<(), Status> {
        if self.seed == 0
            || self.ports[0] == 0
            || !self.endpoint(self.seed_endpoint)
            || !local.endpoint.parse().is_ok_and(|a| self.endpoint(a))
            || (self.seed == local.node && self.seed_endpoint.to_string() != local.endpoint)
            || !(1..=256).contains(&self.max_members)
            || !(25..=10_000).contains(&self.heartbeat_ms)
            || !(self.heartbeat_ms * 3..=120_000).contains(&self.lease_ms)
            || !(self.heartbeat_ms..=60_000).contains(&self.retry_max_ms)
            || !(256..=65536).contains(&self.snapshot_bytes)
            || self.snapshot_bytes as u64 > cap
            || !(1..=256).contains(&self.notify_fanout)
            || self
                .leave_with
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 128)
        {
            Err(Status::PROTOCOL)
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Copy)]
struct Candidate {
    h: ResourceHandle,
    node: u16,
    boot: u64,
    observed: SocketAddr,
    dialed: bool,
    rank: (bool, u64, u64),
}
impl Candidate {
    fn can_replace(self, old: Self) -> bool {
        self.h == old.h || self.boot == old.boot && self.rank > old.rank
    }
}
struct Retry {
    next: Instant,
    delay: u64,
    opening: Option<ResourceHandle>,
}
/// 默认成员策略的 Actor 私有状态。通过外层 `Cluster<C>` 导出，不向宿主暴露目录布局。
pub struct Directory<C> {
    pub(super) ctx: Context,
    pub(super) local: Member,
    cfg: Settings,
    listen: Option<ResourceHandle>,
    bound: bool,
    startup_error: Option<Status>,
    joined: bool,
    leaving: bool,
    leave_token: Option<u64>,
    verified_seed: Option<u64>,
    // 候选有时需等持久化或反向固定握手；保留声明，避免另一启动身份抢占同一待接纳节点。
    claims: BTreeMap<u64, Member>,
    checks: BTreeMap<u64, ResourceHandle>,
    directory: Members,
    links: BTreeMap<u64, Candidate>,
    selected: BTreeMap<u16, ResourceHandle>,
    ready: BTreeSet<u64>,
    seen: BTreeMap<u16, Instant>,
    retries: BTreeMap<u16, Retry>,
    tick: Option<ResourceHandle>,
    output: Vec<u8>,
    scratch: Vec<u8>,
    marker: PhantomData<fn() -> C>,
}
impl<C: cluster::ClusterCodec> Directory<C> {
    pub(super) fn new(ctx: Context, config: &[u8]) -> Result<Self, Status> {
        if ctx.advertised_endpoint().is_empty() {
            return Err(Status::NOT_READY);
        }
        let cfg: Settings =
            toml::from_str(std::str::from_utf8(config).map_err(|_| Status::PROTOCOL)?)
                .map_err(|_| Status::PROTOCOL)?;
        let local = Member {
            node: ctx.address().node().get(),
            boot: ctx.boot(),
            endpoint: ctx.advertised_endpoint().into(),
            cluster: ctx.address().bits(),
        };
        cfg.validate(&local, ctx.limits().payload_bytes.get())?;
        let cap = cfg.snapshot_bytes;
        Ok(Self {
            ctx,
            local,
            cfg,
            listen: None,
            bound: false,
            startup_error: None,
            joined: false,
            leaving: false,
            leave_token: None,
            verified_seed: None,
            claims: BTreeMap::new(),
            checks: BTreeMap::new(),
            directory: Members {
                seed_boot: 0,
                version: 0,
                members: vec![],
            },
            links: BTreeMap::new(),
            selected: BTreeMap::new(),
            ready: BTreeSet::new(),
            seen: BTreeMap::new(),
            retries: BTreeMap::new(),
            tick: None,
            output: vec![0; cap],
            scratch: vec![0; cap * 4],
            marker: PhantomData,
        })
    }
    fn seed(&self) -> bool {
        self.local.node == self.cfg.seed
    }
    fn member(&self, node: u16) -> Option<&Member> {
        self.directory.members.iter().find(|m| m.node == node)
    }
    pub(super) fn cluster_address(&self, node: u16) -> Option<ServiceAddress> {
        if self.leaving || !self.joined {
            return None;
        }
        self.member(node)?.cluster.try_into().ok()
    }
    pub(super) fn resolve_ms(&self) -> u64 {
        self.cfg.lease_ms.min(1000)
    }
    fn log(&self, msg: &str) {
        self.ctx.command(HostOp::Log(0, msg.into()));
    }
    fn codec_limits(&self) -> Limits {
        Limits {
            wire_bytes: self.cfg.snapshot_bytes,
            decoded_bytes: self.cfg.snapshot_bytes * 4,
            items: self.cfg.max_members.max(128),
            ..Limits::default()
        }
    }
    fn encode<T>(&mut self, m: &TypedMethod<C, T, NoReply>, v: &T) -> Result<usize, Status>
    where
        C: Codec<T>,
    {
        let limits = self.codec_limits();
        Ok(
            m.encode_input(v, &mut self.output, &mut self.scratch, limits)
                .map_err(|_| Status::TOO_SMALL)?
                .len(),
        )
    }
    fn send<T>(
        &mut self,
        h: ResourceHandle,
        m: TypedMethod<C, T, NoReply>,
        v: &T,
    ) -> Result<(), Status>
    where
        C: Codec<T>,
    {
        let n = self.encode(&m, v)?;
        self.ctx
            .peer_send_control(h, native_wire(m.wire()), &self.output[..n])
    }
    fn begin(&mut self) -> Result<(), Status> {
        if let Some(name) = &self.cfg.leave_with {
            let r = self
                .ctx
                .command(HostOp::ResolvePrepared(name.as_str().into()));
            if r.status != Status::OK {
                return Err(r.status);
            }
            let r = self.ctx.command(HostOp::ServiceSubscribe(r.value));
            if r.status != Status::OK {
                return Err(r.status);
            }
            self.leave_token = Some(r.value);
        }
        self.listen = Some(self.ctx.peer_listen(&self.local.endpoint)?);
        let r = self.ctx.command(HostOp::TimerSet(
            self.cfg.heartbeat_ms,
            self.cfg.heartbeat_ms,
        ));
        if r.status != Status::OK {
            return Err(r.status);
        }
        self.tick = Some(ResourceHandle {
            id: r.value,
            generation: r.generation,
        });
        Ok(())
    }
    fn hello(&mut self, v: PeerCandidate<'_>) -> Result<(), Status> {
        if self.leaving
            || self.links.len() >= 2 * self.cfg.max_members + 4
                && !self.links.contains_key(&v.link.id)
        {
            return Err(Status::DENIED);
        }
        let p = Candidate {
            h: v.link,
            node: v.remote_node,
            boot: v.remote_boot,
            observed: v.observed.as_str().parse().map_err(|_| Status::PROTOCOL)?,
            dialed: v.dialer == self.local.node,
            rank: (
                v.dialer == self.local.node.min(v.remote_node),
                v.dialer_boot,
                v.attempt,
            ),
        };
        if p.node == self.cfg.seed {
            if p.dialed && p.observed == self.cfg.seed_endpoint {
                self.verified_seed = Some(p.boot);
            }
            if p.observed.ip() != self.cfg.seed_endpoint.ip() || self.verified_seed != Some(p.boot)
            {
                return Err(Status::DENIED);
            }
        }
        if !self.seed()
            && p.node != self.cfg.seed
            && (!self.joined || self.member(p.node).is_none_or(|m| m.boot != p.boot))
        {
            return Err(Status::DENIED);
        }
        self.links.insert(p.h.id, p);
        if !self.seed() {
            self.send(
                p.h,
                cluster::join::<C>(),
                &Join {
                    member: self.local.clone(),
                },
            )?;
        }
        Ok(())
    }
    fn validate_member(&self, m: &Member) -> Result<SocketAddr, Status> {
        let addr = m
            .endpoint
            .parse::<SocketAddr>()
            .map_err(|_| Status::PROTOCOL)?;
        let service = ServiceAddress::try_from(m.cluster).map_err(|_| Status::PROTOCOL)?;
        if service.node().get() != m.node
            || m.boot == 0
            || service.sequence() < m.boot
            || !self.cfg.endpoint(addr)
            || addr.to_string() != m.endpoint
            || (m.node == self.cfg.seed && addr != self.cfg.seed_endpoint)
        {
            return Err(Status::DENIED);
        }
        Ok(addr)
    }
    fn elect(&self, p: Candidate) -> Result<(), Status> {
        if let Some(old) = self
            .selected
            .get(&p.node)
            .and_then(|h| self.links.get(&h.id))
            && !p.can_replace(*old)
        {
            return Err(Status::DENIED);
        }
        Ok(())
    }
    fn select(&mut self, p: Candidate) {
        let old = self.selected.insert(p.node, p.h);
        self.seen.insert(p.node, Instant::now());
        if old != Some(p.h) {
            self.log(&format!(
                "peer-selected node={} boot={} dialer-low={} attempt={} link={} replaced={}",
                p.node,
                p.boot,
                p.rank.0,
                p.rank.2,
                p.h.generation,
                old.map_or(0, |h| h.generation)
            ));
        }
        self.retries.remove(&p.node);
    }
    fn join(&mut self, p: Candidate, q: Join) -> Result<(), Status> {
        let m = q.member;
        let endpoint = self.validate_member(&m)?;
        if (m.node, m.boot) != (p.node, p.boot)
            || endpoint.ip() != p.observed.ip()
            || p.dialed && endpoint != p.observed
            || (!self.seed()
                && (!self.joined || m.node == self.cfg.seed || self.member(m.node) != Some(&m)))
        {
            return Err(Status::DENIED);
        }
        self.elect(p)?;
        if !self.seed() {
            self.ctx.peer_admit(p.h, p.node, p.boot)?;
            self.select(p);
            return Ok(());
        }
        if self
            .directory
            .members
            .iter()
            .chain(self.claims.values())
            .any(|other| {
                other.node == m.node && other != &m
                    || other.node != m.node && other.endpoint.parse() == Ok(endpoint)
            })
        {
            return Err(Status::DENIED);
        }
        if self.member(m.node).is_none() {
            let count = self
                .directory
                .members
                .iter()
                .chain(self.claims.values())
                .map(|m| m.node)
                .chain([m.node])
                .collect::<BTreeSet<_>>()
                .len();
            if count > self.cfg.max_members {
                return Err(Status::TOO_SMALL);
            }
            self.claims.insert(p.h.id, m.clone());
            // 入站源端口是临时端口。首次发布前由实际 peer_connect 对声明端点执行部署 ACL，
            // 再由该反向 TCP 的固定握手和 Join 确认完整声明；未证实的入站声明不先登记。
            if !p.dialed {
                if !self.checks.contains_key(&p.h.id) {
                    let h = self.ctx.peer_connect(&m.endpoint, m.node)?;
                    self.checks.insert(p.h.id, h);
                    self.log(&format!(
                        "peer-confirm node={} boot={} endpoint={} link={}",
                        m.node, m.boot, endpoint, h.generation
                    ));
                }
                return Err(Status::BUSY);
            }
        }
        let mut next = self.directory.clone();
        let new = if let Some(old) = self.member(m.node) {
            if *old != m {
                return Err(Status::DENIED);
            }
            false
        } else {
            next.members.push(m);
            next.members.sort_unstable_by_key(|m| m.node);
            next.version = next.version.checked_add(1).ok_or(Status::NOT_READY)?;
            true
        };
        let result = JoinResult::Joined { snapshot: next };
        let method = cluster::join_result::<C>();
        let n = self.encode(&method, &result)?;
        self.ctx.peer_admit(p.h, p.node, p.boot)?; // 持久化失败／待确认都不能提交此版本。
        self.ctx
            .peer_send_control(p.h, native_wire(method.wire()), &self.output[..n])?;
        self.select(p);
        self.claims.remove(&p.h.id);
        self.checks.remove(&p.h.id);
        if new && let JoinResult::Joined { snapshot } = result {
            self.directory = snapshot;
            self.announce(Some(p.node));
        }
        Ok(())
    }
    fn snapshot(&mut self, p: Candidate, view: Members, joining: bool) -> Result<(), Status> {
        if self.seed()
            || p.node != self.cfg.seed
            || p.boot != view.seed_boot
            || (!joining && self.selected.get(&p.node) != Some(&p.h))
            || view.version == 0
            || view.members.len() > self.cfg.max_members
        {
            return Err(Status::DENIED);
        }
        let mut nodes = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for m in &view.members {
            if !nodes.insert(m.node) || !endpoints.insert(self.validate_member(m)?) {
                return Err(Status::PROTOCOL);
            }
        }
        if !view
            .members
            .iter()
            .any(|m| m.node == p.node && m.boot == p.boot)
        {
            return Err(Status::PROTOCOL);
        }
        let old = &self.directory;
        if view.seed_boot < old.seed_boot
            || (view.seed_boot == old.seed_boot && view.version < old.version)
        {
            return if joining { Err(Status::DENIED) } else { Ok(()) };
        }
        if view.seed_boot == old.seed_boot && view.version == old.version && view != *old {
            return Err(Status::PROTOCOL);
        }
        if !view.members.contains(&self.local) {
            if !joining {
                self.joined = false;
                self.close_except(None);
            }
            return Err(Status::DENIED);
        }
        if joining {
            self.elect(p)?;
            self.ctx.peer_admit(p.h, p.node, p.boot)?;
            self.select(p);
            self.joined = true;
        }
        if self.directory != view {
            self.log(&format!(
                "members epoch={} version={} count={} via={}",
                view.seed_boot,
                view.version,
                view.members.len(),
                p.node
            ));
        }
        self.seen.insert(p.node, Instant::now());
        self.directory = view;
        let stale: Vec<_> = self
            .selected
            .iter()
            .filter(|(n, h)| {
                self.links
                    .get(&h.id)
                    .is_none_or(|l| self.member(**n).is_none_or(|m| m.boot != l.boot))
            })
            .map(|(_, h)| *h)
            .collect();
        for h in stale {
            self.close(h);
        }
        self.retries.retain(|n, _| nodes.contains(n));
        self.seen.retain(|n, _| nodes.contains(n));
        Ok(())
    }
    fn announce(&mut self, except: Option<u16>) {
        self.log(&format!(
            "directory epoch={} version={} count={}",
            self.directory.seed_boot,
            self.directory.version,
            self.directory.members.len()
        ));
        // 尽力通知有限扇出；没收到（或出口满）的成员按心跳版本补齐，不排历史快照。
        let targets: Vec<_> = self
            .selected
            .iter()
            .filter(|(n, h)| except != Some(**n) && self.ready.contains(&h.id))
            .take(self.cfg.notify_fanout)
            .map(|(_, h)| *h)
            .collect();
        let snapshot = self.directory.clone();
        for h in targets {
            let _ = self.send(h, cluster::members::<C>(), &snapshot);
        }
    }
    fn remove(&mut self, n: u16, reason: &str) {
        if self.seed() && self.member(n).is_some() && n != self.local.node {
            let Some(v) = self.directory.version.checked_add(1) else {
                self.leaving = true;
                self.close_except(None);
                return;
            };
            self.directory.version = v;
            self.directory.members.retain(|m| m.node != n);
            self.seen.remove(&n);
            self.retries.remove(&n);
            self.selected.remove(&n);
            for l in self.links.values().filter(|l| l.node == n) {
                self.close(l.h);
            }
            self.log(&format!("member-removed node={n} reason={reason}"));
            self.announce(None);
        }
    }
    fn close(&self, h: ResourceHandle) {
        let _ = self.ctx.peer_close(h);
    }
    fn close_except(&self, keep: Option<ResourceHandle>) {
        for p in self.links.values() {
            if Some(p.h) != keep {
                self.close(p.h)
            }
        }
    }
    fn peer_control(
        &mut self,
        p: Candidate,
        stage: PeerStage,
        w: WireContract,
        b: &[u8],
    ) -> Result<(), Status> {
        let limits = self.codec_limits();
        let w = wire_contract(w);
        macro_rules! decode {
            ($method:ident) => {
                cluster::$method::<C>()
                    .decode_input(w, b, limits)
                    .map_err(|_| Status::PROTOCOL)?
            };
        }
        if self.leaving {
            return Err(Status::STOPPED);
        }
        match w.key {
            k if k == cluster::Method::Join as u32 => {
                let result = self.join(p, decode!(join));
                if self.seed()
                    && stage == PeerStage::Candidate
                    && let Err(status @ (Status::DENIED | Status::TOO_SMALL)) = result
                {
                    let code = status.0;
                    self.log(&format!(
                        "join-rejected node={} boot={} code={code}",
                        p.node, p.boot
                    ));
                    self.send(
                        p.h,
                        cluster::join_result::<C>(),
                        &JoinResult::Rejected { code },
                    )
                } else {
                    result
                }
            }
            k if k == cluster::Method::JoinResult as u32 => {
                let JoinResult::Joined { snapshot } = decode!(join_result) else {
                    return Err(Status::DENIED);
                };
                self.snapshot(p, snapshot, true)
            }
            _ if stage != PeerStage::Admitted || self.selected.get(&p.node) != Some(&p.h) => {
                Err(Status::DENIED)
            }
            k if k == cluster::Method::Members as u32 => self.snapshot(p, decode!(members), false),
            k if k == cluster::Method::Leave as u32 => {
                let leave = decode!(leave);
                if !self.seed() || (leave.node, leave.boot) != (p.node, p.boot) {
                    return Err(Status::DENIED);
                }
                self.remove(p.node, "leave");
                Ok(())
            }
            k if k == cluster::Method::Heartbeat as u32 => {
                let hb = decode!(heartbeat);
                if hb.seed_boot != self.directory.seed_boot
                    || !self.seed() && p.node != self.cfg.seed
                    || self.seed() && hb.version > self.directory.version
                {
                    return Err(Status::DENIED);
                }
                self.seen.insert(p.node, Instant::now());
                if self.seed() {
                    if hb.version != self.directory.version {
                        let snapshot = self.directory.clone();
                        self.send(p.h, cluster::members::<C>(), &snapshot)?;
                        self.log(&format!(
                            "heartbeat-repair node={} from={} to={}",
                            p.node, hb.version, snapshot.version
                        ));
                    } else {
                        self.send(p.h, cluster::heartbeat::<C>(), &hb)?;
                    }
                }
                Ok(())
            }
            _ => Err(Status::UNKNOWN_METHOD),
        }
    }
    fn dial(&mut self, node: u16, endpoint: &str, now: Instant) {
        let retry = self.retries.entry(node).or_insert(Retry {
            next: now,
            delay: self.cfg.heartbeat_ms,
            opening: None,
        });
        if retry.opening.is_some() || now < retry.next {
            return;
        }
        retry.next = now + Duration::from_millis(retry.delay);
        retry.delay = (retry.delay * 2).min(self.cfg.retry_max_ms);
        if let Ok(h) = self.ctx.peer_connect(endpoint, node) {
            retry.opening = Some(h);
        }
    }
    fn depart(&mut self) {
        self.leaving = true;
        let seed = self.selected.get(&self.cfg.seed).copied();
        self.close_except(seed);
        if let Some(h) = seed {
            let _ = self.send(
                h,
                cluster::leave::<C>(),
                &Leave {
                    node: self.local.node,
                    boot: self.local.boot,
                },
            );
        }
    }
    fn tick(&mut self) {
        if !self.bound {
            return;
        }
        if self.leaving {
            self.depart();
            return;
        }
        let now = Instant::now();
        let lease = Duration::from_millis(self.cfg.lease_ms);
        if self.seed() {
            let expired: Vec<_> = self
                .seen
                .iter()
                .filter(|(_, t)| now.duration_since(**t) >= lease)
                .map(|(&n, _)| n)
                .collect();
            for n in expired {
                self.remove(n, "heartbeat-expired");
            }
        } else if let Some(&h) = self.selected.get(&self.cfg.seed) {
            if self
                .seen
                .get(&self.cfg.seed)
                .is_some_and(|t| now.duration_since(*t) >= lease)
            {
                self.close(h)
            } else {
                let _ = self.send(
                    h,
                    cluster::heartbeat::<C>(),
                    &Heartbeat {
                        seed_boot: self.directory.seed_boot,
                        version: self.directory.version,
                    },
                );
            }
        } else {
            self.dial(self.cfg.seed, &self.cfg.seed_endpoint.to_string(), now);
        }
        if self.joined {
            let targets: Vec<_> = self
                .directory
                .members
                .iter()
                .filter(|m| {
                    m.node > self.local.node
                        && self.selected.get(&m.node).is_none_or(|h| {
                            self.ready.contains(&h.id)
                                && self.links.get(&h.id).is_some_and(|p| !p.rank.0)
                        })
                })
                .map(|m| (m.node, m.endpoint.clone()))
                .collect();
            for (n, endpoint) in targets {
                self.dial(n, &endpoint, now);
            }
        }
    }
    fn peer_event(&mut self, e: PeerEvent<'_>) -> Status {
        let (h, r) = match e {
            PeerEvent::ListenReady(h, addr)
                if self.listen == Some(h) && addr.as_str() == self.local.endpoint =>
            {
                self.bound = true;
                if self.seed() {
                    self.joined = true;
                    self.directory = Members {
                        seed_boot: self.local.boot,
                        version: 1,
                        members: vec![self.local.clone()],
                    };
                }
                self.tick();
                return Status::OK;
            }
            PeerEvent::ListenFailed(h, e) if self.listen == Some(h) => {
                self.startup_error = Some(e);
                return Status::OK;
            }
            PeerEvent::Connected(v) => (v.link, self.hello(v)),
            PeerEvent::Control(h, s, w, b) => {
                let Some(p) = self.links.get(&h.id).copied().filter(|p| p.h == h) else {
                    return Status::OK;
                };
                (h, self.peer_control(p, s, w, b.as_slice()))
            }
            PeerEvent::DataReady(h) => {
                if let Some(p) = self.links.get(&h.id)
                    && self.selected.get(&p.node) == Some(&h)
                {
                    self.ready.insert(h.id);
                    self.log(&format!(
                        "peer-ready node={} link={} boot={}",
                        p.node, h.generation, p.boot
                    ));
                }
                return Status::OK;
            }
            PeerEvent::Writable(_) => return Status::OK, // 宿主已保留 Busy 的原始控制输入，接纳本提示后再递交。
            PeerEvent::Closed(h, _) => {
                self.claims.remove(&h.id);
                self.checks.remove(&h.id);
                if let Some(p) = self.links.remove(&h.id)
                    && self.selected.get(&p.node) == Some(&h)
                {
                    self.selected.remove(&p.node);
                    self.log(&format!(
                        "peer-closed node={} link={}",
                        p.node, h.generation
                    ));
                }
                self.ready.remove(&h.id);
                for r in self.retries.values_mut() {
                    if r.opening == Some(h) {
                        r.opening = None;
                    }
                }
                return Status::OK;
            }
            _ => return Status::PROTOCOL,
        };
        match r {
            Ok(()) => Status::OK,
            Err(Status::BUSY) => Status::BUSY,
            Err(Status::FAULTED) => Status::FAULTED,
            Err(e) => {
                self.log(&format!(
                    "peer-refused link={} status={}",
                    h.generation, e.0
                ));
                self.close(h);
                Status::OK
            }
        }
    }
    pub(super) fn control_event(&mut self, op: Control<'_>) -> Status {
        let action = match &op {
            Control::LifecycleEvent(a, _) => Some(*a),
            _ => None,
        };
        match op {
            Control::Prepare => self.begin().err().unwrap_or(Status::PENDING),
            Control::Start if self.joined => Status::OK,
            Control::LifecyclePoll(LifecycleAction::Prepare, _) => {
                self.startup_error.unwrap_or(if self.joined && self.bound {
                    Status::OK
                } else {
                    Status::PENDING
                })
            }
            Control::Event(e) | Control::LifecycleEvent(_, e) => {
                let s = match e {
                    Event::Peer(p) => self.peer_event(p),
                    Event::Timer(h) if self.tick == Some(h) => {
                        self.tick();
                        Status::OK
                    }
                    Event::ResourceGone(token, _) if Some(token) == self.leave_token => {
                        self.depart();
                        Status::OK
                    }
                    _ => Status::NOT_READY,
                };
                if (self.joined || self.startup_error.is_some())
                    && self.listen.is_some()
                    && let Some(a) = action
                {
                    self.ctx.command(HostOp::LifecycleReady(a));
                }
                s
            }
            _ => Status::NOT_READY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_dials_use_common_identity_not_arrival_order() {
        let a = Candidate {
            h: ResourceHandle {
                id: 17,
                generation: 18,
            },
            node: 2,
            boot: 5,
            observed: "127.0.0.1:8921".parse().unwrap(),
            dialed: false,
            rank: (false, 5, 100),
        };
        let preferred = Candidate {
            h: ResourceHandle {
                id: 8,
                generation: 9,
            },
            rank: (true, 1, 1),
            ..a
        };
        let newer = Candidate {
            h: ResourceHandle {
                id: 3,
                generation: 4,
            },
            rank: (true, 1, 2),
            ..a
        };
        assert!(preferred.can_replace(a));
        assert!(!a.can_replace(preferred));
        assert!(newer.can_replace(preferred));
        assert!(!preferred.can_replace(newer));
        assert!(!Candidate { boot: 6, ..newer }.can_replace(preferred));
    }
}
