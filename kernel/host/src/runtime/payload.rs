use super::*;
use ace_server_local::LocalOwned;
use std::sync::atomic::{AtomicU64, Ordering};

/// 转移时不依赖 Actor 记录继续存活；Drop 只对独立计数器做减法，不会重入宿主锁。
#[derive(Default)]
pub(super) struct Account(AtomicU64);
pub(super) struct Permit {
    account: Arc<Account>,
    bytes: u64,
    pub sender: Option<Box<Permit>>,
}
impl Account {
    pub(super) fn reserve(self: &Arc<Self>, bytes: u64, limit: u64) -> Result<Permit, Status> {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(bytes).filter(|&x| x <= limit)
            })
            .map_err(|_| Status::BUSY)?;
        Ok(Permit {
            account: self.clone(),
            bytes,
            sender: None,
        })
    }
    pub(super) fn used(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
impl Permit {
    pub(super) fn split(&mut self, n: u64) -> Result<Self, Status> {
        self.bytes = self.bytes.checked_sub(n).ok_or(Status::TOO_SMALL)?;
        Ok(Self {
            account: self.account.clone(),
            bytes: n,
            sender: None,
        })
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.account.0.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

pub(super) struct Payload {
    pub bytes: Box<[u8]>,
    pub local: LocalSlot,
    pub permit: Option<Permit>,
    pub charge: u64,
}
impl Payload {
    pub(super) fn size(body: Body<'_>, slot: &LocalSlot) -> u64 {
        slot.as_ref()
            .into_option()
            .map_or(body.bytes.len(), LocalOwned::owned_bytes) as u64
    }
    pub(super) fn take(
        body: Body<'_>,
        slot: &mut LocalSlot,
        permit: Permit,
        charge: u64,
    ) -> Result<Self, Status> {
        if let RSome(p) = slot {
            p.attach_lease(permit).map_err(|_| Status::PROTOCOL)?;
            Ok(Self {
                local: slot.take(),
                bytes: Box::default(),
                permit: None,
                charge: 0,
            })
        } else {
            Ok(Self {
                bytes: body.bytes.as_slice().into(),
                local: RNone,
                permit: Some(permit),
                charge: if body.bytes.is_empty() { 0 } else { charge },
            })
        }
    }
    pub(super) fn queued(
        body: Body<'_>,
        slot: &mut LocalSlot,
        permit: Permit,
        charge: u64,
    ) -> Self {
        Self {
            local: slot.take(),
            bytes: body.bytes.as_slice().into(),
            permit: Some(permit),
            charge,
        }
    }
    pub(super) fn attach(&mut self) -> Result<(), Status> {
        if let RSome(local) = &mut self.local
            && let Some(p) = self.permit.take()
        {
            local.attach_lease(p).map_err(|_| Status::PROTOCOL)?;
        }
        Ok(())
    }
    // 非空 Bytes 的 ID 在接纳时签发；每次尝试只转移同一份 permit，不再发号或扣费。
    pub(super) fn lend(&mut self, charges: &mut HashMap<u64, Permit>) {
        if self.charge != 0 {
            let prior = charges.insert(self.charge, self.permit.take().expect("未借出的字节凭据"));
            debug_assert!(prior.is_none());
        }
    }
    pub(super) fn reclaim(
        &mut self,
        original: Representation,
        charges: &mut HashMap<u64, Permit>,
    ) -> bool {
        if original == Representation::LocalOwned {
            return self.local.is_some();
        }
        if self.charge == 0 {
            return true;
        }
        self.permit = charges.remove(&self.charge);
        self.permit.is_some()
    }
    pub(super) fn body(&self) -> Body<'_> {
        Body {
            representation: if self.local.is_some() {
                Representation::LocalOwned
            } else {
                Representation::Bytes
            },
            bytes: self.bytes.as_ref().into(),
        }
    }
}
pub(super) struct Envelope {
    pub source: u64,
    pub token: u64,
    pub wire: WireContract,
    pub entry: Entry,
    pub reply: ReplyBudget,
    pub timeout: u64,
    pub payload: Payload,
}
