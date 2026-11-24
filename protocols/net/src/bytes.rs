use crate::{FRAME_REV, FrameError};

pub(crate) struct Reader<'a>(pub &'a [u8]);
impl Reader<'_> {
    pub fn take<const N: usize>(&mut self) -> Result<[u8; N], FrameError> {
        let (a, rest) = self.0.split_first_chunk().ok_or(FrameError::Truncated)?;
        self.0 = rest;
        Ok(*a)
    }
    pub fn u16(&mut self) -> Result<u16, FrameError> {
        Ok(u16::from_le_bytes(self.take()?))
    }
    pub fn u32(&mut self) -> Result<u32, FrameError> {
        Ok(u32::from_le_bytes(self.take()?))
    }
    pub fn u64(&mut self) -> Result<u64, FrameError> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    pub fn prefix(&mut self) -> Result<(u16, u16), FrameError> {
        if self.u32()? != FRAME_REV {
            return Err(FrameError::Protocol);
        }
        Ok((self.u16()?, self.u16()?))
    }
    pub fn end(&self) -> Result<(), FrameError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(FrameError::Protocol)
        }
    }
}

pub(crate) struct Writer<'a>(pub &'a mut [u8]);
impl Writer<'_> {
    pub fn put(&mut self, bytes: &[u8]) -> Result<(), FrameError> {
        let dst = self
            .0
            .split_off_mut(..bytes.len())
            .ok_or(FrameError::Limit)?;
        dst.copy_from_slice(bytes);
        Ok(())
    }
    pub fn prefix(&mut self, flags: u16, kind: u16) -> Result<(), FrameError> {
        self.put(&FRAME_REV.to_le_bytes())?;
        self.put(&flags.to_le_bytes())?;
        self.put(&kind.to_le_bytes())
    }
}
