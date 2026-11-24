use super::*;
use ace_net_proto::{Call, Frame, FrameError, FrameHead, Message as Kind, NodeEntry, Route};
use rpc::Stage;

#[derive(Clone, Copy)]
pub(in crate::runtime) struct Binding {
    pub(super) link: ResourceHandle,
    source: ServiceAddress,
    target: ServiceAddress,
    call: Call,
}
impl Binding {
    fn response(
        self,
        link: ResourceHandle,
        r: Route,
        head: FrameHead,
        wire: WireContract,
    ) -> Result<(), FrameError> {
        if (self.link, self.source, self.target) != (link, r.destination, r.source)
            || r.entry != NodeEntry::Service
        {
            return Err(FrameError::Protocol);
        }
        head.check_response(contract(wire), self.call).map(|_| ())
    }
}
fn entry(n: NodeEntry) -> Entry {
    match n {
        NodeEntry::Service => Entry::Service,
        NodeEntry::Player(t) => Entry::Player {
            push_session: t.into(),
        },
    }
}
impl Core {
    fn route<'s>(&self, s: &'s State, dst: u64) -> Result<&'s Link, Status> {
        let dst = ServiceAddress::try_from(dst).map_err(|_| Status::PROTOCOL)?;
        let h = *s
            .peers
            .admitted
            .get(&dst.node().get())
            .ok_or(Status::UNREACHABLE)?;
        let l = s.peers.link(h)?;
        if !l.active() || s.closing {
            return Err(Status::UNREACHABLE);
        }
        Ok(l)
    }
    pub(in crate::runtime) fn peer_method(
        &self,
        s: &State,
        key: u64,
        dst: u64,
        w: WireContract,
        mode: ReplyMode,
    ) -> Result<Method, Status> {
        self.route(s, dst)?;
        let a = s.actor(key)?;
        match crate::loader::find_method(a.spec.implementation, w.key) {
            Ok(m) if m.directions & 2 != 0 => {
                if m.wire != w || m.peer_control != PeerAccess::None {
                    Err(Status::PROTOCOL)
                } else {
                    Ok(m)
                }
            }
            _ if a.spec.grants.player_entry && mode == ReplyMode::RawBytes => Ok(Method {
                wire: w,
                directions: 2,
                client_callable: 1,
                peer_control: PeerAccess::None,
                request_local: RNone,
                response_local: RNone,
                max_reply_bytes: self.limits().response_bytes.into(),
                max_reply_local_bytes: 0,
            }),
            _ => Err(Status::DENIED),
        }
    }
    pub(in crate::runtime) fn peer_submit(&self, key: u64, r: Outbound<'_>) -> Result<u64, Status> {
        if r.body.representation != Representation::Bytes
            || r.reply.representation != Representation::Bytes
        {
            return Err(Status::PROTOCOL);
        }
        let mut s = lock(&self.state);
        let phase = s.phase(key)?;
        if phase != Phase::Running && phase.action().is_none() {
            return Err(phase.ending().unwrap_or(Status::NOT_READY));
        }
        let mode = if matches!(r.entry, Entry::Player { .. }) {
            ReplyMode::RawBytes
        } else {
            ReplyMode::Typed
        };
        let m = self.peer_method(&s, key, r.destination, r.wire, mode)?;
        let a = s.actor(key)?;
        let source = a.address.ok_or(Status::NOT_READY)?;
        let account = a.budget.clone();
        let entry = match r.entry {
            Entry::Player { push_session } if a.spec.grants.player_entry => {
                NodeEntry::Player(push_session.into_option())
            }
            Entry::Player { .. } => return Err(Status::DENIED),
            Entry::Service => NodeEntry::Service,
        };
        let target = ServiceAddress::try_from(r.destination).map_err(|_| Status::PROTOCOL)?;
        let link = self.route(&s, r.destination)?.h;
        let limit = self.b.payload_bytes.get();
        let mut ids = s.ids;
        if r.reply.ceiling > m.max_reply_bytes.min(self.limits().response_bytes.into()) {
            return Err(Status::TOO_SMALL);
        }
        let mut pending = self.reserve_call(&s, key, (link.id, m), r, &mut ids)?;
        let kind = if let Some((token, _, p)) = &mut pending {
            let call = Call {
                session: NonZeroU64::new(*token).ok_or(Status::PROTOCOL)?,
                timeout_ms: NonZeroU64::new(r.timeout_ms).ok_or(Status::PROTOCOL)?,
                max_response_bytes: r.reply.ceiling as u32,
            };
            p.remote = Some(Binding {
                link,
                source,
                target,
                call,
            });
            Kind::Request(call)
        } else {
            Kind::Send
        };
        let charge = account.reserve(r.body.bytes.len() as u64, limit)?;
        s.peers
            .links
            .get_mut(&link.id)
            .ok_or(Status::UNREACHABLE)?
            .send(
                NodeFrame::Routed(
                    Route {
                        source,
                        destination: target,
                        entry,
                    },
                    Frame {
                        wire: contract(r.wire),
                        message: kind,
                        body: r.body.bytes.as_slice(),
                    },
                ),
                self.limits(),
                self.b.net_frame_bytes.get(),
                Some(charge),
            )?;
        s.ids = ids;
        if let Some((token, id, p)) = pending {
            s.pending.insert(token, p);
            s.actor(key)?.signal.notify_one();
            Ok(id)
        } else {
            Ok(0)
        }
    }
    fn target(&self, s: &State, r: Route, head: FrameHead) -> Result<(u64, Method), Status> {
        let ceiling = if let Kind::Request(call) = head.message {
            if call.max_response_bytes > self.limits().response_bytes {
                return Err(Status::TOO_SMALL);
            }
            self.duration(call.timeout_ms.get())?;
            u64::from(call.max_response_bytes)
        } else {
            0
        };
        s.input_method(r.destination, native(head.wire), entry(r.entry), ceiling)
    }
    // 先看固定头；拒绝的请求仍在有界出口回无体终态。过期响应只排空，不取得 body 空间。
    pub(super) fn preflight(
        &self,
        s: &State,
        h: ResourceHandle,
        route: Option<Route>,
        head: FrameHead,
    ) -> Result<Result<(), Status>, Status> {
        let l = s.peers.link(h)?;
        let Some(r) = route else {
            check_peer_method(
                s.actor(l.owner)?.spec.implementation,
                native(head.wire),
                l.stage(),
                1,
            )?;
            return Ok(Ok(()));
        };
        if !l.active()
            || l.hello.is_none_or(|v| v.node != r.source.node())
            || r.destination.node().get() != self.node
        {
            return Err(Status::DENIED);
        }
        if let Kind::Response { session, .. } = head.message {
            let Some(p) = s
                .pending
                .get(&session.get())
                .filter(|p| matches!(p.stage, Stage::Open))
            else {
                return Ok(Err(Status::NOT_FOUND));
            };
            let b = p.remote.ok_or(Status::PROTOCOL)?;
            match b.response(h, r, head, p.method.wire) {
                Ok(()) => Ok(Ok(())),
                Err(FrameError::Limit) => Ok(Err(Status::TOO_SMALL)),
                Err(_) => Err(Status::PROTOCOL),
            }
        } else {
            // session 由 caller host 签发，不要求不同 caller 在同一 TCP 上排序。
            // 活跃会话重复不能另产生一次 handler 或一个抢先完成旧调用的错误回包。
            if let Kind::Request(call) = head.message
                && s.has_inbound_session(h.id, r.source.bits(), call.session.get())
            {
                return Err(Status::PROTOCOL);
            }
            if s.closing {
                return Ok(Err(Status::STOPPED));
            }
            Ok(self.target(s, r, head).map(|_| ()))
        }
    }
    pub(super) fn route_input(
        &self,
        s: &mut State,
        h: ResourceHandle,
        r: Route,
        head: FrameHead,
        b: &[u8],
        checked: Result<(), Status>,
    ) -> Result<(), Status> {
        s.peers.link(h)?;
        // 关停时拒新的业务，但不因这一帧撤掉仍需传 Leave/终结通知的已授权控制连接。
        let checked = if s.closing && !matches!(head.message, Kind::Response { .. }) {
            Err(Status::STOPPED)
        } else {
            checked
        };
        let entry = entry(r.entry);
        match head.message {
            Kind::Response { session, error } => {
                // 帧头没有命中时即永久作废：排空期间签发的新 session 不能认领这次响应。
                if checked == Err(Status::NOT_FOUND) {
                    return Ok(());
                }
                let t = session.get();
                let Some(p) = s.pending.get_mut(&t).filter(|p| {
                    p.remote.is_some_and(|v| v.link == h) && matches!(p.stage, Stage::Open)
                }) else {
                    return Ok(());
                };
                if p.deadline <= Instant::now() {
                    s.finish_error(t, Status::TIMEOUT);
                    return Ok(());
                }
                if let Err(e) = checked {
                    s.finish_error(t, e);
                    return Ok(());
                }
                if let Some(e) = error {
                    s.finish_error(t, Status(e as u32));
                } else {
                    let key = p.owner().ok_or(Status::PROTOCOL)?;
                    p.complete(
                        Ok(Body {
                            representation: Representation::Bytes,
                            bytes: b.into(),
                        }),
                        &mut RNone,
                    )?;
                    s.actor(key)?.signal.notify_one();
                }
                Ok(())
            }
            Kind::Request(call) => {
                let result = checked.and_then(|()| {
                    self.inbound(
                        s,
                        h.id,
                        InboundCall {
                            source: r.source,
                            destination: r.destination,
                            session: call.session.get(),
                            wire: native(head.wire),
                            bytes: b,
                            max_response_bytes: call.max_response_bytes.into(),
                            timeout_ms: call.timeout_ms.get(),
                        },
                        entry,
                    )
                });
                if let Err(e) = result {
                    s.ports.get_mut(&h.id).ok_or(Status::UNREACHABLE)?.push(
                        r.destination.bits(),
                        r.source.bits(),
                        call.session.get(),
                        native(head.wire),
                        Err(if e.is_reply_error() {
                            e
                        } else {
                            Status::PROTOCOL
                        }),
                    );
                }
                Ok(())
            }
            Kind::Send => {
                checked?;
                let (k, _) = self.target(s, r, head)?;
                let cost = s
                    .actor(k)?
                    .budget
                    .reserve(b.len() as u64, self.b.payload_bytes.get())?;
                let mut ids = s.ids;
                let charge = ids.charge(!b.is_empty())?;
                s.enqueue(
                    k,
                    Envelope {
                        source: r.source.bits(),
                        wire: native(head.wire),
                        entry,
                        timeout: 0,
                        reply: ReplyBudget {
                            representation: Representation::Bytes,
                            ceiling: 0,
                        },
                        token: 0,
                        payload: Payload::queued(
                            Body {
                                representation: Representation::Bytes,
                                bytes: b.into(),
                            },
                            &mut RNone,
                            cost,
                            charge,
                        ),
                    },
                    &mut RNone,
                )?;
                s.ids = ids;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn response_belongs_to_one_peer_generation_and_original_call() {
        let link = ResourceHandle {
            id: 4,
            generation: 9,
        };
        let source = ((1u64 << 48) | 3).try_into().unwrap();
        let target = ((2u64 << 48) | 7).try_into().unwrap();
        let call = Call {
            session: NonZeroU64::new(23).unwrap(),
            timeout_ms: NonZeroU64::MIN,
            max_response_bytes: 4,
        };
        let b = Binding {
            link,
            source,
            target,
            call,
        };
        let r = Route {
            source: target,
            destination: source,
            entry: NodeEntry::Service,
        };
        let wire = Contract {
            key: 1,
            codec: [1, 1, 1],
            request: [7; 32],
            response: [8; 32],
        };
        let head = FrameHead {
            wire,
            message: Kind::Response {
                session: call.session,
                error: None,
            },
            body_len: 4,
            frame_len: 124,
        };
        let check = |h, r, f| b.response(h, r, f, native(wire));
        assert_eq!(check(link, r, head), Ok(()));
        assert_eq!(
            check(
                ResourceHandle {
                    generation: 8,
                    ..link
                },
                r,
                head
            ),
            Err(FrameError::Protocol)
        );
        assert_eq!(
            check(
                link,
                Route {
                    source: ((2u64 << 48) | 8).try_into().unwrap(),
                    ..r
                },
                head
            ),
            Err(FrameError::Protocol)
        );
        assert_eq!(
            check(
                link,
                Route {
                    destination: ((1u64 << 48) | 4).try_into().unwrap(),
                    ..r
                },
                head
            ),
            Err(FrameError::Protocol)
        );
        assert_eq!(
            check(
                link,
                r,
                FrameHead {
                    message: Kind::Response {
                        session: NonZeroU64::MIN,
                        error: None
                    },
                    ..head
                }
            ),
            Err(FrameError::Protocol)
        );
        assert_eq!(
            check(
                link,
                r,
                FrameHead {
                    body_len: 5,
                    ..head
                }
            ),
            Err(FrameError::Limit)
        );
        assert_eq!(
            check(
                link,
                r,
                FrameHead {
                    wire: Contract {
                        response: [9; 32],
                        ..wire
                    },
                    ..head
                }
            ),
            Err(FrameError::Protocol)
        );
    }
}
