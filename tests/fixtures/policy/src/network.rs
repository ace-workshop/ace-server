//! 显式启用的回环 owner，用真实 Service 表展示监听、完整帧与关闭凭据。不实现 Gate/准入。
use super::*;
use ace_example_protocol::{EchoReply, echo};
#[cfg(not(feature = "codec_ace"))]
use ace_fixture_codec::FixedCodec as Selected;
use ace_net_proto::{Frame, FrameLimits, Message as FrameKind};
#[cfg(feature = "codec_ace")]
use ace_server_sdk::AceCodec as Selected;
use ace_server_sdk::codec::Limits;

#[derive(Default)]
enum AcceptGate {
    #[default]
    Open,
    HoldOk,
    HoldBusy,
    Refuse,
}
pub(super) struct Owner {
    waiting: Vec<ResourceHandle>,
    status: Status,
    probe: bool,
    refused: bool,
    keep_closed: bool,
    accept_gate: AcceptGate,
    hold: bool,
    capacity: bool,
}
impl Owner {
    pub(super) fn new(probe: bool) -> Self {
        Self {
            waiting: vec![],
            status: Status::OK,
            probe,
            refused: false,
            keep_closed: false,
            accept_gate: AcceptGate::Open,
            hold: true,
            capacity: true,
        }
    }
    fn log(ctx: &Context, label: &str) {
        ctx.command(HostOp::Log(0, label.into()));
    }
    pub(super) fn trace_conn(&self, ctx: &Context, c: Connection, event: &str) {
        if self.probe {
            Self::log(ctx, &format!("net[{}] {event}", c.handle.generation));
        }
    }
    fn frame(&mut self, ctx: &Context, c: Connection, b: &[u8]) -> Result<(), Status> {
        let limits = FrameLimits {
            body_bytes: 64 * 1024,
            response_bytes: 64 * 1024,
            timeout_ms: 30_000,
        };
        let f = Frame::from_player(b, limits).map_err(|_| Status::PROTOCOL)?;
        let FrameKind::Request(call) = f.message else {
            return Err(Status::PROTOCOL);
        };
        let m = echo::<Selected>();
        let q = m
            .decode_frame_input(&f, Limits::default())
            .map_err(|_| Status::PROTOCOL)?;
        if self.probe && self.hold && q.data == b"hold" {
            Self::log(ctx, "net-held");
            return Err(Status::BUSY);
        }
        if self.probe && q.data == b"fault" {
            panic!("回环夹具：已确认实例的受控故障");
        }
        let pause = self.probe && q.data == b"pause";
        let resume = self.probe && q.data == b"resume";
        let accept_gate = match (self.probe, q.data.as_slice()) {
            (true, b"accept-ok") => Some(AcceptGate::HoldOk),
            (true, b"accept-busy") => Some(AcceptGate::HoldBusy),
            (true, b"defer-accept") => Some(AcceptGate::Refuse),
            _ => None,
        };
        let mut body = vec![0; (call.max_response_bytes as usize).min(64 * 1024)];
        let mut scratch = vec![0; Limits::default().decoded_bytes.min(64 * 1024)];
        let body = m
            .encode_response(
                &EchoReply { data: q.data },
                &mut body,
                &mut scratch,
                Limits::default(),
            )
            .map_err(|_| Status::TOO_SMALL)?;
        let reply = Frame {
            wire: f.wire,
            message: FrameKind::Response {
                session: call.session,
                error: None,
            },
            body,
        };
        let mut bytes = vec![0; 104 + body.len()];
        reply
            .encode_into(&mut bytes, limits)
            .map_err(|_| Status::PROTOCOL)?;
        ctx.net_send(c, &bytes)?;
        if let Some(gate) = accept_gate {
            self.accept_gate = gate;
        }
        if resume {
            self.hold = false;
            ctx.wake();
        }
        if pause {
            self.capacity = false;
            self.keep_closed = true; // 仅该探针显式暂扣 Closed；Timer 由验证方在观察完关闭后触发。
            ctx.wake();
        }
        Self::log(ctx, "net-data");
        Ok(())
    }
    pub(super) fn control(&mut self, ctx: &Context, op: Control<'_>) -> Progress {
        let status = match op {
            Control::Prepare => {
                for t in [Transport::Tcp, Transport::Kcp] {
                    match ctx.net_listen(t, "127.0.0.1:0") {
                        Ok(h) => self.waiting.push(h),
                        Err(e) => return Progress::idle(e),
                    }
                }
                Status::PENDING
            }
            Control::LifecycleEvent(LifecycleAction::Prepare, Event::Network(event)) => {
                let (h, status) = match event {
                    NetEvent::ListenReady(h, t, p) => {
                        Self::log(ctx, &format!("net-listen {t:?} {}", p.as_str()));
                        (h, Status::OK)
                    }
                    NetEvent::ListenFailed(h, e) => (h, e),
                    _ => return Progress::idle(Status::PROTOCOL),
                };
                let Some(i) = self.waiting.iter().position(|r| *r == h) else {
                    return Progress::idle(Status::NOT_FOUND);
                };
                self.waiting.swap_remove(i);
                if status != Status::OK {
                    self.status = status;
                }
                if self.waiting.is_empty() {
                    ctx.command(HostOp::LifecycleReady(LifecycleAction::Prepare));
                }
                Status::OK
            }
            Control::LifecyclePoll(LifecycleAction::Prepare, _) if self.waiting.is_empty() => {
                self.status
            }
            Control::Start => Status::OK,
            Control::Poll(_) => {
                if !self.capacity {
                    Self::log(ctx, "net-task-paused");
                }
                return Progress {
                    status: Status::OK,
                    more_ready: 0,
                    task_capacity: u8::from(self.capacity),
                };
            }
            Control::Event(Event::Network(NetEvent::Accepted(listener, c, peer))) => {
                if self.probe {
                    Self::log(
                        ctx,
                        &format!(
                            "net-offer {} {}:{} {}",
                            c.handle.generation,
                            listener.id,
                            listener.generation,
                            peer.as_str()
                        ),
                    );
                }
                match self.accept_gate {
                    AcceptGate::HoldOk => {
                        self.accept_gate = AcceptGate::Open;
                        Self::log(ctx, "accept-ok");
                    }
                    AcceptGate::HoldBusy => {
                        self.accept_gate = AcceptGate::Open;
                        Self::log(ctx, "accept-busy");
                        self.trace_conn(ctx, c, "refused");
                        return Progress::idle(Status::BUSY);
                    }
                    AcceptGate::Refuse => {
                        self.trace_conn(ctx, c, "refused");
                        return Progress::idle(Status::BUSY);
                    }
                    AcceptGate::Open => {}
                }
                self.trace_conn(ctx, c, "accepted");
                Self::log(ctx, "net-accepted");
                Status::OK
            }
            Control::Event(Event::Network(NetEvent::Data(c, b))) => {
                self.trace_conn(ctx, c, "data");
                self.frame(ctx, c, b.as_slice()).err().unwrap_or(Status::OK)
            }
            Control::Event(Event::Network(NetEvent::Writable(_))) => {
                Self::log(ctx, "net-writable");
                Status::OK
            }
            Control::Event(Event::Network(NetEvent::Closed(c, _))) => {
                if self.keep_closed && !self.refused {
                    self.refused = true;
                    Self::log(ctx, "net-refused-closed");
                    return Progress::idle(Status::BUSY);
                }
                self.trace_conn(ctx, c, "closed");
                Self::log(ctx, "net-closed");
                if self.keep_closed {
                    Status::OK
                } else {
                    ctx.net_release_closed(c).err().unwrap_or(Status::OK)
                }
            }
            Control::Event(Event::Network(NetEvent::ListenFailed(..))) => {
                Self::log(ctx, "net-listen-failed");
                Status::OK
            }
            Control::Event(Event::Network(NetEvent::Error(..))) => Status::OK,
            Control::Event(Event::Timer(_)) => {
                self.capacity = true;
                ctx.wake();
                Status::OK
            }
            _ => Status::NOT_READY,
        };
        Progress::idle(status)
    }
}
