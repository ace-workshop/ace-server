"""固定 codec 回环夹具：每个 host 进程单独记录，不从业务日志推断调用。"""
import os


class FixedCodecTrace:
    def __init__(self, host_log):
        self.directory = host_log.with_suffix(".codec")
        self.directory.mkdir()

    def env(self):
        return dict(os.environ, ACE_FIXTURE_CODEC_TRACE_DIR=str(self.directory))

    def entries(self, dto):
        path = self.directory / dto
        try:
            with path.open("rb") as f:
                entries = f.read((1 << 20) + 1)
        except FileNotFoundError:
            if not self.directory.is_dir():
                raise
            return b""  # 这个进程尚未对该 DTO 调过 codec。
        assert len(entries) <= 1 << 20 and not entries.strip(b"ED"), path
        return entries
