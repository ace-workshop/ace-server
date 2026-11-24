#!/usr/bin/env python3
"""在私有回环端点上运行正式 host；管理指令只触发 caller，结果来自独立节点的 SDK Actor。"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

from fixed_codec_trace import FixedCodecTrace

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("release", type=Path)
parser.add_argument("results", type=Path, nargs="?")
parser.add_argument("--roles", choices=("cluster-only", "export", "fixed"), default="cluster-only")
args = parser.parse_args()
RELEASE = args.release.resolve()
WORK = Path(tempfile.mkdtemp(prefix="cluster.", dir=args.results or RELEASE)).resolve()
HOST = str(RELEASE / "ace-server-host")
EXT = "dylib" if sys.platform == "darwin" else "so"
MAX = (1 << 48) - 1
HELLO = struct.pack("<IHHI", 1, 0, 16, 1)
WORD = hashlib.sha256(b"test.runtime.Word/1:u64le").digest()
NODES = []


def setup():
    (WORK / "lib").mkdir()
    cluster_lib = {"export": "export_cluster", "fixed": "fixed_cluster"}.get(args.roles, "ace_service_cluster")
    cluster_impl = "example.export-cluster" if args.roles == "export" else "cluster"
    libraries = [cluster_lib, "runtime_caller", "runtime_echo"]
    if args.roles != "cluster-only":
        prefix = "fixed_" if args.roles == "fixed" else "ace_service_"
        libraries.extend([prefix + "gateway", prefix + "watchdog"])
    for name in libraries:
        f = "lib" + name + "." + EXT
        (WORK / "lib" / f).symlink_to((RELEASE / "examples" if name.startswith("runtime_") else RELEASE) / f)
    # 同时取得互异的空闲端口，不扫描局域网，不连接运行中的其它服务。
    sockets = [socket.socket() for _ in range(4)]
    for s in sockets:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    for n, port in enumerate(ports, 1):
        d = WORK / str(n)
        d.mkdir()
        subprocess.run([HOST, "provision-new", str(n), d / "identity"], check=True, timeout=5)
        actions = [f'{{kind="{kind}",alias="{alias}"}}' for kind in ("create", "prepare", "start") for alias in ("members", "echo", "caller")]
        player_roles = ""
        if args.roles != "cluster-only":
            # 同节点实际创建、校验并启动未替换的两个角色；应用数据仍只走 peer TCP。
            actions.extend(f'{{kind="{kind}",alias="{alias}"}}' for kind, alias in (
                ("create", "sessions"), ("create", "gate"), ("prepare", "gate"), ("prepare", "sessions"), ("start", "sessions"), ("start", "gate")))
            base = Path("config/server.toml").read_text()
            player_roles = base[base.index('[[instances]]\nimplementation = "gateway"'):]
            player_roles = player_roles.replace('"player"', '"echo"')
        (d / "node.toml").write_text(f"""revision=1
node={n}
sequence_block=16
libraries={json.dumps(libraries)}
actions=[{','.join(actions)}]
[peers]
advertised="127.0.0.1:{port}"
allow=[{{ip="127.0.0.1",nodes=[1,4],ports=[{min(ports)},{max(ports)}]}}]
[budgets]
workers=3
instances=8
mailbox_entries=32
payload_bytes=131072
task_slots=16
polls_per_turn=8
control_slots=16
timeout_ms=4000
net_frame_bytes=32768
[[instances]]
implementation="{cluster_impl}"
alias="members"
role="cluster"
grants={{peer_control=true}}
prepare_dependencies=["echo"]
config='''
seed=1
seed_endpoint="127.0.0.1:{ports[0]}"
ports=[{min(ports)},{max(ports)}]
max_members=4
heartbeat_ms=100
lease_ms=1200
retry_max_ms=400
notify_fanout=1
leave_with="echo"
'''
[[instances]]
implementation="test.echo"
alias="echo"
config="peer-echo"
[[instances]]
implementation="test.caller"
alias="caller"
config="peer-probe"
grants={{player_entry={str(n == 2).lower()}}}
manage=["test.echo","{cluster_impl}"]
{player_roles}
""")

    return ports


class Node:
    def __init__(self, n):
        self.n = n
        self.log = []
        self.gate = threading.Condition()
        self.client = None
        self.eof = False
        self.paused = False
        d = WORK / str(n)
        self.path = d / f"run-{sum(p.n == n for p in NODES) + 1}.log"
        command = [HOST, "run-manifest", d / "node.toml", WORK / "lib", d / "identity"]
        self.codec = FixedCodecTrace(self.path) if args.roles == "fixed" else None
        self.p = subprocess.Popen(command, env=self.codec.env() if self.codec else None, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        NODES.append(self)
        self.reader = threading.Thread(target=self.collect)
        self.reader.start()
        print(f"start node={n} pid={self.p.pid} command={' '.join(map(str, command))} log={self.path}", flush=True)

    def collect(self):
        with self.path.open("w") as f:
            for line in self.p.stdout:
                f.write(line)
                f.flush()
                with self.gate:
                    self.log.append(line.rstrip())
                    self.gate.notify_all()
        with self.gate:
            self.eof = True
            self.gate.notify_all()

    def wait(self, pattern, start=0, timeout=8):
        deadline = time.monotonic() + timeout
        with self.gate:
            while True:
                for i in range(start, len(self.log)):
                    if m := re.search(pattern, self.log[i]):
                        return i, m
                start = len(self.log)
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.eof:
                    raise AssertionError(f"node={self.n} missing={pattern} see {self.path}: {self.log[-12:]}")
                self.gate.wait(remaining)

    def published(self):
        self.wait("清单实例已发布")
        _, m = self.wait(r"probe-listen 127\.0\.0\.1:(\d+)")
        self.client = socket.create_connection(("127.0.0.1", int(m[1])), timeout=3)
        self.client.sendall(HELLO)
        hello = b""
        while len(hello) < len(HELLO):
            b = self.client.recv(len(HELLO) - len(hello))
            assert b
            hello += b
        assert hello == HELLO
        return self

    def startup_timeout(self):
        assert self.p.wait(8) != 0
        self.reader.join(2)
        assert not any("清单实例已发布" in line or "probe-listen" in line for line in self.log)
        self.wait(r"Instance\(Status\(10\)\)")

    def submit(self, word):
        start = len(self.log)
        self.client.sendall(struct.pack("<IHHIIIII", 1, 0, 1, 0, 105, 0xa31, 1, 0) + WORD + bytes(32) + struct.pack("<IQQ", 8, 0, word))
        return word, start

    def result(self, request, expected=None):
        word, start = request
        _, m = self.wait(rf"probe {word} (ok|error) (.+)$", start)
        val = int(m[2]) if m[1] == "ok" else m[2]
        if expected is not None:
            assert val == expected, (self.n, word, val, expected, self.path)
        return val

    def command(self, node, offset, expected=None):
        return self.result(self.submit((node << 48) | (MAX - offset)), expected)

    def ready(self, other, start=0, canonical=False):
        while True:
            i, m = self.wait(rf"peer-ready node={other} link=(\d+) boot=(\d+)", start)
            if not canonical or any(re.search(rf"peer-selected node={other} .*dialer-low=true .*link={m[1]} ", line) for line in self.log[:i]):
                return i, int(m[2])
            start = i + 1

    def stop(self, crash=False):
        if self.paused:
            self.p.send_signal(signal.SIGCONT)
            self.paused = False
        if self.p.poll() is None:
            if crash:
                self.p.kill()
            else:
                self.p.stdin.close()
            self.p.wait(6)
        if self.client:
            self.client.close()
        self.reader.join(2)
        assert self.p.returncode == (-signal.SIGKILL if crash else 0), (self.n, self.p.returncode, self.path)


def call(src, dst, *, check_sample=False):
    address = src.command(dst.n, 0)
    assert isinstance(address, int) and address >> 48 == dst.n, (src.n, dst.n, address)
    before, submitted = len(dst.log), len(src.log)
    sample = "ace_fixture_codec::sample::PeerSample"
    offsets = {p: len(p.codec.entries(sample)) for p in (src, dst)} if src.codec and check_sample else {}
    src.command(dst.n, 1, 42)
    _, m = dst.wait(rf"peer-input source=(\d+) target={address} method=101", before)
    assert int(m[1]) >> 48 == src.n
    counts = "0/0" if src == dst else "1/1"
    src.wait(rf"peer-result target={address} value=42 codec={counts}", submitted)
    # peer-result 在本次回复被调用方消费之后；被调用方此前已处理输入并产生回复。
    for host, first in offsets.items():
        entries = host.codec.entries(sample)[first:]
        expected = b"" if src == dst else (b"ED" if host == src else b"DE")
        assert entries == expected, (src.n, dst.n, host.p.pid, entries, expected)
        print(f"PeerSample: node={host.n} pid={host.p.pid} {host.path.stem}.codec bytes[{first}:{first+len(entries)}]={entries!r}", flush=True)
    print(f"resolve+rpc pid={src.p.pid} node={src.n} -> pid={dst.p.pid} node={dst.n} target={address} result=42 caller-codec={counts}", flush=True)
    return address


def run(ports):
    # 全新目录、从未加入的 NodeId；不仅是有历史记录的节点断线重试。
    Node(4).startup_timeout()
    print("first boot without seed: no business listener or publication; startup deadline and exit", flush=True)
    a = Node(1).published()
    occupied = subprocess.run([HOST, "run-manifest", WORK / "1/node.toml", WORK / "lib", WORK / "1/identity"], input="", capture_output=True, text=True, timeout=5)
    assert occupied.returncode != 0 and "Identity(Locked)" in occupied.stderr, occupied.stderr
    b = Node(2).published()
    a.ready(2, canonical=True)
    b.ready(1, canonical=True)
    _, elected = a.wait(r"peer-selected node=2 .*dialer-low=true attempt=(\d+) ")
    b.wait(rf"peer-selected node=1 .*dialer-low=true attempt={elected[1]} ")
    a.wait("directory epoch=1 version=2 count=2")
    call(a, b, check_sample=True)
    old_a = call(b, a, check_sample=True)
    call(b, b, check_sample=True)  # 同进程两独立库，本地请求和回复均不调用 codec。
    if args.roles == "export":
        start = len(a.log)
        b.command(1, 12, "Status(Status(8))")
        a.wait(r"export-hidden source=\d+ node=1", start)
        assert b.command(2, 12) >> 48 == 2  # 本机仍可解析 caller。
    else:
        assert b.command(1, 12) >> 48 == 1  # 默认目录没有该导出限制。
    b.command(1, 2)
    a.wait("oneway")
    b.command(1, 6, 9)  # 普通 Actor 不可取得 peer_control
    c = Node(3).published()
    a.ready(3, canonical=True)
    c.ready(1, canonical=True)
    b.ready(3)
    c.ready(2)
    old_c = call(b, c, check_sample=True)
    call(c, b, check_sample=True)
    call(a, c, check_sample=True)
    call(c, a, check_sample=True)
    before = len(c.log)
    b.command(3, 9, 42)
    _, pushed = c.wait(r"peer-player source=(\d+) push=71", before)
    assert int(pushed[1]) >> 48 == b.n
    b.command(3, 10, "Status(Status(2))")  # 已有入口授权，但不是 client-callable 的方法
    a.command(3, 9, "Status(Status(9))")   # 未授权的源实例
    b.command(3, 11, "Status(Status(7))")  # 已接纳，不能用空成功冒充预算不足
    b.command(3, 4, "Status(Status(10))")
    c.command(3, 8, 11)
    call(b, c)
    before = len(b.log)
    c.command(3, 5, 0)
    a.wait("member-removed node=3 reason=leave")
    b.wait("peer-closed node=3", before)
    b.result(b.submit(old_c), "Status(Status(11))")
    c.stop()
    print("normal leave withdrew route; late reply did not complete another call", flush=True)
    starts = [len(p.log) for p in (a, b)]
    c, d = Node(3), Node(4)  # 同时加入，而非重启 A/B
    c.published()
    d.published()
    a.ready(3, starts[0], True)
    a.ready(4, starts[0], True)
    b.ready(3, starts[1])
    c.ready(4)
    d.ready(3)
    assert call(b, c) != old_c
    b.result(b.submit(old_c), "Status(Status(8))")
    call(c, d)
    before, c_before = len(a.log), len(c.log)
    d.command(4, 5, 0)
    removed, _ = a.wait("member-removed node=4 reason=leave", before)
    _, change = a.wait(r"directory epoch=(\d+) version=(\d+) count=3$", removed)
    _, catchup = a.wait(rf"heartbeat-repair node=3 from=(\d+) to={change[2]}$", removed)
    assert int(catchup[1]) < int(change[2])
    c.wait(rf"members epoch={change[1]} version={change[2]} count=3 via=1$", c_before)
    d.stop()
    print("concurrent joins, elected duplicate links, missed notification repaired from full snapshot, old address NotFound", flush=True)
    before = len(a.log)
    _, boot = a.ready(3, starts[0], True)
    c.paused = True
    c.p.send_signal(signal.SIGSTOP)
    a.wait("member-removed node=3 reason=heartbeat-expired", before)
    starts = [len(p.log) for p in (a, b, c)]
    c.p.send_signal(signal.SIGCONT)
    c.paused = False
    _, same_boot = a.ready(3, starts[0], True)
    b.ready(3, starts[1])
    assert same_boot == boot
    call(b, c)
    print(f"heartbeat-only eviction, same pid={c.p.pid} same boot={same_boot} rejoined", flush=True)
    before = len(c.log)
    request = b.submit((3 << 48) | (MAX - 3))
    c.wait("holding-input", before)
    gone = len(a.log)
    c.stop(crash=True)
    b.result(request, "Status(Status(11))")
    starts = [len(p.log) for p in (a, b)]
    c = Node(3).published()
    ready, new_boot = a.ready(3, starts[0], True)
    expired, _ = a.wait("member-removed node=3 reason=heartbeat-expired", gone)
    chosen, _ = a.wait(rf"peer-selected node=3 boot={new_boot} ", gone)
    assert new_boot > same_boot and expired < chosen < ready
    b.ready(3, starts[1])
    call(b, c)
    print(f"abrupt disconnect terminated pending Unreachable; new boot={new_boot}", flush=True)
    before = len(b.log)
    a.stop(crash=True)
    b.wait("peer-closed node=1", before)
    call(b, c)
    call(c, b)
    Node(4).startup_timeout()
    starts = [len(p.log) for p in (b, c)]
    # 隔离新 seed 的磁盘恢复与 C 的重新登记：观测低 boot 拒绝前不让 live C 建新握手。
    c.paused = True
    c.p.send_signal(signal.SIGSTOP)
    a = Node(1).published()
    # 复送本试验 C 已退休的固定引导身份；只连本次建立的 seed，绝不发送业务。
    before = len(a.log)
    with socket.create_connection(("127.0.0.1", ports[0]), timeout=3) as stale:
        stale.sendall(struct.pack("<IHHHQHQQ", 1, 0, 0x110, 3, same_boot, 3, same_boot, 1))
        a.wait(rf"peer-stale node=3 boot={same_boot} ", before)
        prefix = b""
        while chunk := stale.recv(64):
            prefix += chunk
            assert len(prefix) <= 36  # 只可能有固定答头，不建立角色或数据通路。
    assert not any("peer-selected node=3 " in line for line in a.log)
    c.p.send_signal(signal.SIGCONT)
    c.paused = False
    b.ready(1, starts[0], True)
    c.ready(1, starts[1], True)
    a.ready(2, canonical=True)
    a.ready(3, canonical=True)
    b.wait("members epoch=17 .*count=3", starts[0])
    c.wait("members epoch=17 .*count=3", starts[1])
    assert call(b, a) != old_a
    b.ready(3, starts[0])
    c.ready(2, starts[1])
    call(b, c)
    print("seed absent: healthy direct calls survive; first join not published and timed out; new seed epoch converged", flush=True)
    # 仅本试验专属目录。保留原记录，令下一次同目录临时写入失败；不能假报接纳或发布。
    before = len(a.log)
    request = b.submit((1 << 48) | (MAX - 3))
    a.wait("holding-input", before)
    (WORK / "1/identity").rename(WORK / "1/retained-identity")
    failed = Node(4)
    a.wait("peer-watermark save failed node=4", before)
    b.result(request, "Status(Status(11))")
    failed.startup_timeout()
    assert not any("peer-selected node=4 " in line or "directory epoch=" in line for line in a.log[before:])
    a.command(1, 7, "Status(Status(8))")  # Faulted 控制器已 destroy；宿主仍能驱动其它 Actor。
    call(b, c)
    before = len(b.log)
    c.command(3, 7, 0)
    b.wait("peer-closed node=3", before)
    b.command(3, 1, "Status(Status(11))")
    c.stop()
    b.stop()
    a.stop()
    print("durability failure did not publish; Faulted and normal controller stops revoked routes; all owned hosts reaped", flush=True)


print(f"cluster smoke root={WORK}", flush=True)
try:
    run(setup())
    if args.roles == "fixed":
        for n in (1, 2, 3):
            boots = [p for p in NODES if p.n == n]
            for dto in ("Join", "JoinResult", "Members", "Leave", "Heartbeat", "Resolve", "Resolved", "PeerSample"):
                # JoinResult/Members 由 seed 向成员发送；Join/心跳等依各自职责收发。
                directions = {"Join": ["decode" if n == 1 else "encode"],
                              "Leave": {1: ["decode"], 2: [], 3: ["encode"]}[n],
                              "JoinResult": ["encode" if n == 1 else "decode"],
                              "Members": ["encode" if n == 1 else "decode"]}.get(dto, ["encode", "decode"])
                name = ("ace_fixture_codec::sample::" if dto == "PeerSample" else "ace_role_cluster::") + dto
                for direction in directions:
                    marker = b"E" if direction == "encode" else b"D"
                    per_boot = [(p, p.codec.entries(name).count(marker)) for p in boots]
                    hits = sum(count for _, count in per_boot)
                    assert hits > 0, (n, direction, dto)
                    detail = ", ".join(f"{p.path.stem}/pid={p.p.pid}:{count}" for p, count in per_boot)
                    print(f"node={n} fixed {dto} {direction}={hits} ({detail})", flush=True)
finally:
    for p in NODES:
        if p.p.poll() is None:
            p.p.kill()
            p.p.wait(5)
        if p.client:
            p.client.close()
