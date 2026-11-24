use super::*;

/// caller / echo 的线字段；两边各自拥有本地稳定表示，不从帧中借用对象。
#[cfg_attr(feature = "sample_ace", derive(ace_serialize_derive::AceSerialize))]
pub struct PeerSample {
    /// 被调方对其加上样本之和。
    #[cfg_attr(feature = "sample_ace", ace(order = 0))]
    pub id: u64,
    /// 与本地路径相同的 UTF-8 字符串。
    #[cfg_attr(feature = "sample_ace", ace(order = 1))]
    pub text: String,
    /// 按顺序编码的 u32 样本。
    #[cfg_attr(feature = "sample_ace", ace(order = 2))]
    pub samples: Vec<u32>,
}
impl Record for PeerSample {
    const DEPTH: usize = 1;
    fn write(&self, w: &mut &mut [u8], l: Limits) -> Result<()> {
        put(w, &self.id.to_le_bytes())?;
        blob(w, self.text.as_bytes(), l)?;
        count(w, self.samples.len(), l)?;
        for v in &self.samples {
            put(w, &v.to_le_bytes())?;
        }
        Ok(())
    }
    fn read(r: &mut Input<'_>) -> Result<Self> {
        let id = u64::from_le_bytes(r.take()?);
        let text = r.string()?;
        let n = r.count()?.checked_mul(4).ok_or(CodecError::Limit)?;
        let bytes = r.src.split_off(..n).ok_or(CodecError::Invalid)?;
        r.charge(n)?;
        let samples = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .copied()
            .map(u32::from_le_bytes)
            .collect();
        Ok(Self { id, text, samples })
    }
}
