//! Gate、Watchdog 的固定字段信封；内层玩家帧仍是独立的原始字节。
use super::*;
use ace_role_gateway::{
    Ack, Bind, Connection, Kick, Online, OnlineCount, SendToSession, Transport,
};
use ace_role_watchdog::{AdmissionDecision, CheckAdmission, Close, Open};

impl Record for Connection {
    const DEPTH: usize = 1;
    fn write(&self, w: &mut &mut [u8], _: Limits) -> Result<()> {
        put(w, &[self.transport as u8])?;
        put(w, &self.id.to_le_bytes())?;
        put(w, &self.generation.to_le_bytes())
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self {
            transport: match r.take()? {
                [1] => Transport::Tcp,
                [2] => Transport::Kcp,
                _ => return Err(CodecError::Invalid),
            },
            id: u64::from_le_bytes(r.take()?),
            generation: u64::from_le_bytes(r.take()?),
        })
    }
}
impl Record for Bind {
    const DEPTH: usize = 2;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        self.conn.write(w, l)?;
        put(w, &self.target.to_le_bytes())
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self {
            conn: Connection::read(r)?,
            target: u64::from_le_bytes(r.take()?),
        })
    }
}
macro_rules! connection_only {
    ($dto:ty) => {
        impl Record for $dto {
            const DEPTH: usize = 2;
            fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
                self.conn.write(w, l)
            }
            fn read(r: &mut Input<'_>) -> Result<Self> {
                Ok(Self {
                    conn: Connection::read(r)?,
                })
            }
        }
    };
}
connection_only!(Kick);
connection_only!(Close);
scalars!(Ack,);
scalars!(Online,);
scalars!(OnlineCount, count: u32);
impl Record for SendToSession {
    const DEPTH: usize = 1;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        put(w, &self.push_session.to_le_bytes())?;
        blob(w, &self.frame, l)
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self {
            push_session: u64::from_le_bytes(r.take()?),
            frame: r.blob()?.into(),
        })
    }
}
impl Record for Open {
    const DEPTH: usize = 2;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        match self {
            Self::Accepted { conn, peer } => {
                put(w, &1u16.to_le_bytes())?;
                conn.write(w, l)?;
                blob(w, peer.as_bytes(), l)
            }
            Self::Handshake { conn, handshake } => {
                put(w, &2u16.to_le_bytes())?;
                conn.write(w, l)?;
                handshake.write(w, l)
            }
        }
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        match u16::from_le_bytes(r.take()?) {
            1 => Ok(Self::Accepted {
                conn: Connection::read(r)?,
                peer: r.string()?,
            }),
            2 => Ok(Self::Handshake {
                conn: Connection::read(r)?,
                handshake: Admission::read(r)?,
            }),
            _ => Err(CodecError::Invalid),
        }
    }
}
impl Record for CheckAdmission {
    const DEPTH: usize = 2;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        self.conn.write(w, l)?;
        blob(w, self.peer.as_bytes(), l)?;
        self.handshake.write(w, l)
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        Ok(Self {
            conn: Connection::read(r)?,
            peer: r.string()?,
            handshake: Admission::read(r)?,
        })
    }
}
impl Record for AdmissionDecision {
    const DEPTH: usize = 1;
    fn write(&self, w: &mut &mut [u8], _: Limits) -> Result<()> {
        match self {
            Self::Admitted { target } => {
                put(w, &1u16.to_le_bytes())?;
                put(w, &target.to_le_bytes())
            }
            Self::Denied { code } => {
                put(w, &2u16.to_le_bytes())?;
                put(w, &code.to_le_bytes())
            }
            Self::Unavailable => put(w, &3u16.to_le_bytes()),
        }
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        match u16::from_le_bytes(r.take()?) {
            1 => Ok(Self::Admitted {
                target: u64::from_le_bytes(r.take()?),
            }),
            2 => Ok(Self::Denied {
                code: u32::from_le_bytes(r.take()?),
            }),
            3 => Ok(Self::Unavailable),
            _ => Err(CodecError::Invalid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn check_admission_fixed_fields_reject_truncation_and_budget_errors() {
        let q = CheckAdmission {
            conn: Connection {
                transport: Transport::Kcp,
                id: 3,
                generation: 7,
            },
            peer: "p".into(),
            handshake: Admission {
                token: "demo".into(),
            },
        };
        let l = Limits::default();
        let mut buf = [0; 30];
        assert_eq!(FixedCodec::encode_into(&q, &mut buf, &mut [], l), Ok(30));
        assert_eq!(
            &buf,
            b"\x02\x03\0\0\0\0\0\0\0\x07\0\0\0\0\0\0\0\x01\0\0\0p\x04\0\0\0demo"
        );
        assert_eq!(FixedCodec::decode(&buf, l), Ok(q));
        assert_eq!(
            <FixedCodec as Codec<CheckAdmission>>::decode(&buf[..29], l),
            Err(CodecError::Invalid)
        );
        let limited = Limits {
            decoded_bytes: size_of::<CheckAdmission>() + 4,
            ..l
        };
        assert_eq!(
            <FixedCodec as Codec<CheckAdmission>>::decode(&buf, limited),
            Err(CodecError::Limit)
        );
        assert_eq!(
            FixedCodec::encode_into(
                &Admission {
                    token: "demo".into()
                },
                &mut buf[..7],
                &mut [],
                l
            ),
            Err(CodecError::Limit)
        );
    }
}
