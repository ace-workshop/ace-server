"""从固定 ace-serialize 提交的独立 Python 参考模型和公共帧规格重算样本，不读取 Rust 输出。

用法：python3 -B golden.py <该提交的 tools 目录>；与同目录 golden.txt 比对。
"""
import hashlib
from pathlib import Path
import struct
import sys

sys.path.insert(0, str(Path(sys.argv[1]).resolve()))
from gen_golden import Encoder, Scalar, Seq, obj
from packed_ref import pack


def digest(name):
    return hashlib.sha256(f"demo.{name}/1{{0:data:!seq(u8)(opaque-octets)}}".encode()).digest()


data = b"ACE"
reference = Encoder()
reference.encode_root(obj((0, Seq(tuple(Scalar("u8", x) for x in data)))))
for name, codec, body in [("ace", (1, 1, 1), pack(reference.finish())),
                          ("fixed", (0xf17, 1, 0), struct.pack("<I", len(data)) + data)]:
    for kind, label in [(2, "request"), (3, "response")]:
        header = struct.pack("<IHHIIIII32s32sIQ", 1, 0, kind, 0, 0x402, *codec,
                             digest("Echo"), digest("EchoReply"), len(body), 0x0102030405060708)
        ext = struct.pack("<IQ", 128, 750) if kind == 2 else b""
        print(f"{name}.{label} {(header + ext + body).hex()}")
