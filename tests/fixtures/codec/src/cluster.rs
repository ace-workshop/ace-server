//! 同一测试 codec 的服务端装配：字段次序固定，缺失/多余字段均拒绝，不借用 ace 实现。
use super::*;
use ace_role_cluster::{Heartbeat, Join, JoinResult, Leave, Member, Members, Resolve, Resolved};
impl Input<'_> {
    fn member(&mut self) -> Result<Member> {
        Ok(Member {
            node: u16::from_le_bytes(self.take()?),
            boot: u64::from_le_bytes(self.take()?),
            endpoint: self.string()?,
            cluster: u64::from_le_bytes(self.take()?),
        })
    }
}
fn member(v: &Member, w: &mut &mut [u8], l: Limits) -> Result<()> {
    put(w, &v.node.to_le_bytes())?;
    put(w, &v.boot.to_le_bytes())?;
    blob(w, v.endpoint.as_bytes(), l)?;
    put(w, &v.cluster.to_le_bytes())
}
impl Record for Join {
    const DEPTH: usize = 2;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        member(&self.member, w, l)
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self {
            member: r.member()?,
        })
    }
}
impl Record for Members {
    const DEPTH: usize = 3;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        put(w, &self.seed_boot.to_le_bytes())?;
        put(w, &self.version.to_le_bytes())?;
        count(w, self.members.len(), l)?;
        for v in &self.members {
            member(v, w, l)?;
        }
        Ok(())
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        let seed_boot = u64::from_le_bytes(r.take()?);
        let version = u64::from_le_bytes(r.take()?);
        let n = r.count()?;
        // 即使所有 endpoint 都为空，每个成员也要 2+8+4+8 字节；先拒绝伪造长度再分配。
        if n > r.src.len() / 22 {
            return Err(CodecError::Invalid);
        }
        r.charge(
            n.checked_mul(size_of::<Member>())
                .ok_or(CodecError::Limit)?,
        )?;
        let mut members = Vec::with_capacity(n);
        for _ in 0..n {
            members.push(r.member()?);
        }
        Ok(Self {
            seed_boot,
            version,
            members,
        })
    }
}
impl Record for JoinResult {
    const DEPTH: usize = 4;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        match self {
            Self::Joined { snapshot } => {
                put(w, &1u16.to_le_bytes())?;
                snapshot.write(w, l)
            }
            Self::Rejected { code } => {
                put(w, &2u16.to_le_bytes())?;
                put(w, &code.to_le_bytes())
            }
        }
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        match u16::from_le_bytes(r.take()?) {
            1 => Ok(Self::Joined {
                snapshot: Members::read(r)?,
            }),
            2 => Ok(Self::Rejected {
                code: u32::from_le_bytes(r.take()?),
            }),
            _ => Err(CodecError::Invalid),
        }
    }
}
impl Record for Resolve {
    const DEPTH: usize = 1;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        put(w, &self.node.to_le_bytes())?;
        blob(w, self.alias.as_bytes(), l)
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self {
            node: u16::from_le_bytes(r.take()?),
            alias: r.string()?,
        })
    }
}
scalars!(Leave, node: u16, boot: u64);
scalars!(Heartbeat, seed_boot: u64, version: u64);
scalars!(Resolved, address: u64);
