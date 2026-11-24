//! 本机清单启动期的只读实例关系检查；不经过投递、Select 或订阅登记路径。
use super::*;
use crate::loader::find_method;

pub(super) static API: StartupPeerCheck = StartupPeerCheck {
    version: STARTUP_PEER_CHECK_V1,
    check,
};

extern "C" fn check(key: u64, target: u64, required: StartupPeer<'_>) -> Status {
    guard(|| {
        ACTIVE
            .get()
            .and_then(Weak::upgrade)
            .ok_or(Status::STOPPED)
            .and_then(|c| c.check_startup_peer(key, target, required))
            .err()
            .unwrap_or(Status::OK)
    })
}

impl Core {
    fn check_startup_peer(
        &self,
        key: u64,
        target: u64,
        required: StartupPeer<'_>,
    ) -> Result<(), Status> {
        let s = lock(&self.state);
        let phase = s.phase(key)?;
        if s.published || s.closing || !matches!(phase, Phase::Preparing | Phase::Starting) {
            return Err(Status::NOT_READY);
        }
        let caller = s.actor(key)?;
        // addresses 只含本节点已发号的精确实例，不按 role 遍历其它候选，也不走远端路由。
        // Prepare 对未授权的地址一律拒绝，不借错误差异透露依赖范围外的地址是否存在。
        let dst = s.key(target).map_err(|e| {
            if phase == Phase::Preparing {
                Status::DENIED
            } else {
                e
            }
        })?;
        let peer = s.actor(dst)?;
        if key == dst {
            return Err(Status::DENIED);
        }
        if phase == Phase::Preparing {
            if !peer
                .spec
                .alias
                .as_ref()
                .is_some_and(|a| caller.spec.dependencies.contains(a))
            {
                return Err(Status::DENIED);
            }
            match s.phase(dst)? {
                Phase::Reserved | Phase::Creating => return Err(Status::NOT_READY),
                Phase::Ending(e) | Phase::Gone(e) => return Err(e),
                _ => {}
            }
        } else {
            s.running(dst)?;
        }
        let from = caller.spec.implementation;
        let to = peer.spec.implementation;
        let role = from
            .requires
            .as_slice()
            .iter()
            .find(|r| r.name == required.role)
            .ok_or(Status::DENIED)?;
        if !peer.spec.provides(role) {
            return Err(Status::PROTOCOL);
        }
        if required.require_player_entry && !peer.spec.grants.player_entry {
            return Err(Status::DENIED);
        }
        let source = caller.address.ok_or(Status::NOT_READY)?.bits();
        if required.require_subscription
            && !peer
                .subs
                .values()
                .any(|sub| !sub.gone && sub.target == source)
        {
            return Err(Status::NOT_READY);
        }
        drop(s); // 此后只比较两份常驻描述表，不在账本锁内扫描方法。
        check_methods(from, to, required.send_methods.as_slice(), role.codec)?;
        check_methods(to, from, required.receive_methods.as_slice(), role.codec)
    }
}

fn check_methods(
    from: &Descriptor,
    to: &Descriptor,
    keys: &[u32],
    codec: [u32; 3],
) -> Result<(), Status> {
    if keys.len() > from.methods.len().min(to.methods.len()) {
        return Err(Status::UNKNOWN_METHOD);
    }
    for &key in keys {
        let send = find_method(from, key)?;
        let recv = find_method(to, key)?;
        if send.directions & 2 == 0
            || recv.directions & 1 == 0
            || send.wire != recv.wire
            || send.wire.codec != codec
        {
            return Err(Status::PROTOCOL);
        }
        if send.peer_control != PeerAccess::None || recv.peer_control != PeerAccess::None {
            return Err(Status::DENIED);
        }
    }
    Ok(())
}
