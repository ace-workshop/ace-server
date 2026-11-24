//! 节点身份目录一次只允许一个宿主持有；冷启动持久预留后才签发地址。
use ace_net_proto::ServiceAddress;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File},
    io::{self, Read, Write},
    num::{NonZeroU16, NonZeroU64},
    path::{Path, PathBuf},
};

const MAGIC: [u8; 8] = *b"ACEID001";
const LEN: usize = 8 + 2 + 8 + 32;
const RECORD: &str = "watermark";

/// 拒绝签发地址的原因。
#[derive(Debug)]
pub enum IdentityError {
    /// 身份文件和目录操作失败，保留 OS 错误。
    Io(io::Error),
    /// 另一个进程正在使用此目录。
    Locked,
    /// 文件长度、版本、校验和或记录值错误。
    Corrupt,
    /// 配置的节点不是这个目录的节点。
    WrongNode,
    /// 48 位流水号已耗尽。
    Exhausted,
    /// 先前的持久化结果不确定，此实例不再发号。
    Poisoned,
}
impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "身份状态 I/O：{e}"),
            Self::Locked => f.write_str("身份目录已被占用"),
            Self::Corrupt => f.write_str("身份记录损坏"),
            Self::WrongNode => f.write_str("节点身份不符"),
            Self::Exhausted => f.write_str("节点流水号耗尽"),
            Self::Poisoned => f.write_str("持久化失败，已停止签发"),
        }
    }
}
impl std::error::Error for IdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Self::Io(e) = self {
            Some(e)
        } else {
            None
        }
    }
}
impl From<io::Error> for IdentityError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn persist(path: &Path, dir: &File, node: NonZeroU16, high: u64) -> io::Result<()> {
    let mut record = [0u8; LEN];
    record[..8].copy_from_slice(&MAGIC);
    record[8..10].copy_from_slice(&node.get().to_le_bytes());
    record[10..18].copy_from_slice(&high.to_le_bytes());
    let sum = Sha256::digest(&record[..18]);
    record[18..].copy_from_slice(&sum);
    replace_synced(path, dir, RECORD, &record)
}
fn replace_synced(path: &Path, dir: &File, name: &str, bytes: &[u8]) -> io::Result<()> {
    // 同一文件系统内原子替换；锁属于目录而不是被 rename 的 inode。
    let mut temp = tempfile::NamedTempFile::new_in(path)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path.join(name)).map_err(|e| e.error)?;
    dir.sync_all()
}
fn watermark(path: &Path, node: NonZeroU16) -> Result<u64, IdentityError> {
    let mut f = File::open(path.join(RECORD))?;
    let mut buf = [0u8; LEN];
    if f.metadata()?.len() != LEN as u64 {
        return Err(IdentityError::Corrupt);
    }
    f.read_exact(&mut buf)?;
    if buf[..8] != MAGIC || Sha256::digest(&buf[..18])[..] != buf[18..] {
        return Err(IdentityError::Corrupt);
    }
    if u16::from_le_bytes([buf[8], buf[9]]) != node.get() {
        return Err(IdentityError::WrongNode);
    }
    let high = u64::from_le_bytes(buf[10..18].try_into().expect("固定 8 字节字段"));
    if high > ServiceAddress::MAX_SEQUENCE {
        return Err(IdentityError::Corrupt);
    }
    Ok(high)
}
fn lock(dir: &File) -> Result<(), IdentityError> {
    dir.try_lock().map_err(|e| match e {
        fs::TryLockError::WouldBlock => IdentityError::Locked,
        fs::TryLockError::Error(e) => IdentityError::Io(e),
    })
}
/// 管理员首次分配此前从未使用的节点号时显式调用；正常启动不走此路径。
///
/// # Errors
/// 不覆盖已有目录或缺失/回滚的旧状态。调用方必须保证节点号属于全新身份域。
pub fn provision_new(path: &Path, node: NonZeroU16) -> Result<(), IdentityError> {
    fs::create_dir(path)?;
    let dir = File::open(path)?;
    lock(&dir)?;
    persist(path, &dir, node, 0)?;
    save_peers(path, &dir, node, &BTreeMap::new())?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

// 与发号同一目录锁、原子替换和同步规则。与 boot 分开：重启只恢复高水位，不恢复资格或链接。
const PEERS: &str = "peer-watermarks";
fn save_peers(
    path: &Path,
    dir: &File,
    node: NonZeroU16,
    peers: &BTreeMap<u16, u64>,
) -> io::Result<()> {
    let mut b = Vec::with_capacity(42 + peers.len() * 10);
    b.extend_from_slice(b"ACEPR001");
    b.extend_from_slice(&node.get().to_le_bytes());
    for (n, boot) in peers {
        b.extend_from_slice(&n.to_le_bytes());
        b.extend_from_slice(&boot.to_le_bytes());
    }
    b.extend_from_slice(&Sha256::digest(&b));
    replace_synced(path, dir, PEERS, &b)
}
/// 独占持久身份；保留旧号段中未使用的空洞而不复用地址。
pub struct Identity {
    path: PathBuf,
    dir: File,
    node: NonZeroU16,
    block: NonZeroU64,
    next: u64,
    high: u64,
    boot: u64,
    poisoned: bool,
}
impl Identity {
    /// 只打开已配好的目录，先同步下一个号段，再使本次启动身份可见。
    ///
    /// # Errors
    /// 文件丢失、校验/节点不符、目录锁冲突、耗尽或持久化失败均拒绝启动。
    pub fn open(path: &Path, node: NonZeroU16, block: NonZeroU64) -> Result<Self, IdentityError> {
        let path = path.canonicalize()?;
        let dir = File::open(&path)?;
        lock(&dir)?;
        let high = watermark(&path, node)?;
        let mut state = Self {
            path,
            dir,
            node,
            block,
            next: 0,
            high,
            boot: 0,
            poisoned: false,
        };
        state.reserve()?;
        state.boot = state.next;
        Ok(state)
    }
    pub(crate) fn peer_boots(&self) -> Result<BTreeMap<u16, u64>, IdentityError> {
        const MAX: usize = 42 + 10 * u16::MAX as usize;
        let mut b = Vec::new();
        File::open(self.path.join(PEERS))?
            .take(MAX as u64 + 1)
            .read_to_end(&mut b)?;
        if !(42..=MAX).contains(&b.len())
            || &b[..8] != b"ACEPR001"
            || b[8..10] != self.node.get().to_le_bytes()
            || !(b.len() - 42).is_multiple_of(10)
            || Sha256::digest(&b[..b.len() - 32])[..] != b[b.len() - 32..]
        {
            return Err(IdentityError::Corrupt);
        }
        let mut peers = BTreeMap::new();
        for record in b[10..b.len() - 32].as_chunks::<10>().0 {
            let n = u16::from_le_bytes([record[0], record[1]]);
            let boot =
                u64::from_le_bytes(record[2..].try_into().map_err(|_| IdentityError::Corrupt)?);
            if n == 0
                || n == self.node.get()
                || !(1..=ServiceAddress::MAX_SEQUENCE).contains(&boot)
                || peers.insert(n, boot).is_some()
            {
                return Err(IdentityError::Corrupt);
            }
        }
        Ok(peers)
    }
    pub(crate) fn save_peer_boots(&self, peers: &BTreeMap<u16, u64>) -> Result<(), IdentityError> {
        save_peers(&self.path, &self.dir, self.node, peers).map_err(Into::into)
    }
    fn reserve(&mut self) -> Result<(), IdentityError> {
        if self.poisoned {
            return Err(IdentityError::Poisoned);
        }
        let n = self
            .block
            .get()
            .min(ServiceAddress::MAX_SEQUENCE - self.high);
        if n == 0 {
            return Err(IdentityError::Exhausted);
        }
        let high = self.high + n;
        if let Err(e) = persist(&self.path, &self.dir, self.node, high) {
            self.poisoned = true;
            return Err(e.into());
        }
        self.next = self.high + 1;
        self.high = high;
        Ok(())
    }
    /// 本次启动的首个预留流水号，不随之后扩充号段而改变。
    pub fn boot(&self) -> u64 {
        self.boot
    }
    /// 签发一个地址；实例之后创建失败也不归还号码。
    ///
    /// # Errors
    /// 已耗尽或不能可靠预留下一个号段时不签发。
    pub fn issue(&mut self) -> Result<ServiceAddress, IdentityError> {
        if self.poisoned {
            return Err(IdentityError::Poisoned);
        }
        if self.next > self.high {
            self.reserve()?;
        }
        let address =
            ServiceAddress::new(self.node, self.next).map_err(|_| IdentityError::Corrupt)?;
        self.next += 1;
        Ok(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ids() -> (NonZeroU16, NonZeroU64) {
        (NonZeroU16::new(7).unwrap(), NonZeroU64::new(4).unwrap())
    }
    #[test]
    fn restart_skips_full_reserved_range_and_lock_is_exclusive() {
        let temp = tempfile::tempdir().unwrap();
        let p = temp.path().join("identity");
        let (n, b) = ids();
        assert!(Identity::open(&p, n, b).is_err());
        provision_new(&p, n).unwrap();
        assert!(provision_new(&p, n).is_err());
        let mut a = Identity::open(&p, n, b).unwrap();
        assert_eq!(a.boot(), 1);
        assert_eq!(a.issue().unwrap().sequence(), 1);
        assert!(matches!(
            Identity::open(&p, n, b),
            Err(IdentityError::Locked)
        ));
        drop(a);
        let mut a = Identity::open(&p, n, b).unwrap();
        assert_eq!(a.boot(), 5);
        for seq in 5..=9 {
            assert_eq!(a.issue().unwrap().sequence(), seq);
        }
        assert_eq!(a.boot(), 5);
        drop(a);
        assert!(matches!(
            Identity::open(&p, NonZeroU16::MAX, b),
            Err(IdentityError::WrongNode)
        ));
        let reserved = Identity::open(&p, n, b).unwrap();
        let boot = reserved.boot();
        drop(reserved); // 尚未 issue 就结束本次启动。
        let mut later = Identity::open(&p, n, b).unwrap();
        let addr = later.issue().unwrap();
        assert_eq!(addr.bits(), (u64::from(n.get()) << 48) | (boot + b.get()));
        assert_eq!(std::mem::size_of_val(&addr), 8);
        assert_eq!(
            ServiceAddress::try_from(u64::from_le_bytes(addr.bits().to_le_bytes())).unwrap(),
            addr
        );
        drop(later);
        let f = File::open(&p).unwrap();
        persist(&p, &f, n, ServiceAddress::MAX_SEQUENCE - 1).unwrap();
        let mut last = Identity::open(&p, n, b).unwrap();
        assert_eq!(
            last.issue().unwrap().sequence(),
            ServiceAddress::MAX_SEQUENCE
        );
        assert!(matches!(last.issue(), Err(IdentityError::Exhausted)));
        drop(last);
        let mut rec = fs::read(p.join(RECORD)).unwrap();
        rec[17] ^= 1;
        fs::write(p.join(RECORD), rec).unwrap();
        assert!(matches!(
            Identity::open(&p, n, b),
            Err(IdentityError::Corrupt)
        ));
    }
    #[test]
    fn failed_reservation_never_issues_an_address() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("identity");
        let (n, _) = ids();
        provision_new(&p, n).unwrap();
        let mut a = Identity::open(&p, n, NonZeroU64::MIN).unwrap();
        assert_eq!(a.issue().unwrap().sequence(), 1);
        // 只移动测试专属临时目录；模拟无法打开下一次暂存文件，不依赖用户权限。
        fs::rename(&p, t.path().join("moved")).unwrap();
        assert!(matches!(a.issue(), Err(IdentityError::Io(_))));
        assert!(matches!(a.issue(), Err(IdentityError::Poisoned)));
    }
    #[test]
    fn peer_watermarks_detect_missing_or_corrupt_history() {
        let temp = tempfile::tempdir().unwrap();
        let p = temp.path().join("identity");
        let (n, b) = ids();
        provision_new(&p, n).unwrap();
        let id = Identity::open(&p, n, b).unwrap();
        id.save_peer_boots(&BTreeMap::from([(2, 17)])).unwrap();
        drop(id);
        let id = Identity::open(&p, n, b).unwrap();
        assert_eq!(id.peer_boots().unwrap(), BTreeMap::from([(2, 17)]));
        fs::rename(p.join(PEERS), p.join("retained")).unwrap();
        assert!(matches!(id.peer_boots(), Err(IdentityError::Io(_))));
        let mut bytes = fs::read(p.join("retained")).unwrap();
        bytes[15] ^= 1;
        fs::write(p.join(PEERS), bytes).unwrap();
        assert!(matches!(id.peer_boots(), Err(IdentityError::Corrupt)));
    }
}
