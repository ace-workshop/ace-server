#!/usr/bin/env python3
"""独立 release host + 两个独立 CLI。保留本次身份、实际清单、子进程输出，失败不清理证据。"""
from contextlib import contextmanager
import argparse
import os
from pathlib import Path
import re
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

from fixed_codec_trace import FixedCodecTrace

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("release", type=Path)
parser.add_argument("results", type=Path, nargs="?")
parser.add_argument("--case", choices=("default", "gateway", "watchdog", "policy", "fixed", "fixed-bytes", "assembly", "shutdown"), default="default")
args = parser.parse_args()
RELEASE = args.release.resolve()
WORK = Path(tempfile.mkdtemp(prefix="player.", dir=args.results or RELEASE)).resolve()
HOST, CLI = RELEASE / "ace-server-host", RELEASE / "ace-test-client"
# 身份和监听都属于本次试验；不依赖手工部署示例的固定 seed 端口。
with socket.socket() as seed:
    seed.bind(("127.0.0.1", 0))
    DEFAULT = Path("config/server.toml").read_text().replace("44001", str(seed.getsockname()[1]))
HELLO = struct.pack("<IHHI", 1, 0, 16, 1)
SESSION = 0x0102030405060708
WAIT = 6


def run(*args, **kwargs):
    return subprocess.run(args, check=True, timeout=WAIT, **kwargs)


class Host:
    def __init__(self, child, log, codec=None):
        self.child, self.log, self.data = child, log, b""
        self.codec = codec

    def wait(self, regex, start=0):
        end = time.monotonic() + WAIT
        while not (match := re.search(regex, self.data[start:])):
            left = end - time.monotonic()
            assert left > 0 and select.select([self.child.stdout], [], [], left)[0], (regex, self.log.name)
            chunk = os.read(self.child.stdout.fileno(), 8192)
            assert chunk and len(self.data) < 1 << 20, (regex, self.log.name)
            self.log.write(chunk)
            self.data += chunk
        return match


def provision(name, text, libraries=RELEASE):
    d = WORK / name
    d.mkdir()
    cfg = d / "node.toml"
    cfg.write_text(text)
    run(HOST, "provision-new", "1", d / "identity")
    cmd = [HOST, "run-manifest", cfg, libraries, d / "identity"]
    print(f"{name}: command={' '.join(map(str, cmd))}", flush=True)
    return cmd, d / "host.log"


@contextmanager
def host(name, text):
    command, path = provision(name, text)
    codec = FixedCodecTrace(path) if args.case.startswith("fixed") else None
    with path.open("wb", buffering=0) as log, subprocess.Popen(command, env=codec.env() if codec else None, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as p:
        h = Host(p, log, codec)
        try:
            h.wait("清单实例已发布".encode())
            gate = rb"actor\[(\d+)\] gate-listen "
            tcp, kcp = [h.wait(gate + t + rb" (127\.0\.0\.1:\d+)\n") for t in (b"Tcp", b"Kcp")]
            assert tcp[1] == kcp[1]
            print(f"{name}: host={p.pid} gate={tcp[1].decode()} tcp={tcp[2].decode()} kcp={kcp[2].decode()}", flush=True)
            yield h, tcp[2].decode(), kcp[2].decode()
        finally:
            try:
                out, _ = p.communicate(input=b"x", timeout=WAIT)
                log.write(out)
                h.data += out
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(WAIT)
                raise
        assert p.returncode == 0, (p.returncode, path)
        print(f"{name}: host exit={p.returncode}", flush=True)


def both(h, tcp, kcp, token="demo", checked=False):
    clients = []
    try:
        for mode, endpoint in [("player-tcp", tcp), ("player-kcp", kcp)]:
            p = subprocess.Popen([CLI, mode, endpoint, *(["--token", token] if token != "demo" else []), "--barrier"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            clients.append((mode, p))
        for _, p in clients:
            assert select.select([p.stdout], [], [], WAIT)[0] and p.stdout.readline() == b"transport-ready\n"
        # 两个真实 socket 此刻均完成传输握手；没有先结束 TCP 再启第二个节点。
        for _, p in clients:
            p.stdin.write(b"x")
            p.stdin.flush()
        for mode, p in clients:
            out, _ = p.communicate(timeout=WAIT)
            (Path(h.log.name).parent / (mode + ".log")).write_bytes(out)
            assert p.returncode == 0 and f"player-response count=1 session={SESSION}".encode() in out, (mode, out)
            print(f"{mode}: client={p.pid} exit={p.returncode} session={SESSION} one golden reply", flush=True)
    finally:
        for _, p in clients:
            if p.poll() is None:
                p.kill()
                p.wait(WAIT)
    wd = h.wait(rb"actor\[(\d+)\] watchdog-prepared gate=(\d+)\n")
    targets, policies = set(), set()
    for transport, reason in [(b"Tcp", b"0"), (b"Kcp", b"10")]:
        m = h.wait(rb"actor\[" + wd[2] + rb"\] gate-accepted " + transport + rb" conn=(\d+:\d+) watchdog=" + wd[1] + rb"\n")
        conn = m[1]
        tail = rb" conn=" + conn
        patterns = [b"gate-accepted " + transport + tail, b"watchdog-open gate=" + wd[2] + tail,
                    b"watchdog-admitted" + tail, b"gate-bound" + tail + rb" target=(\d+)\n",
                    rb"(?m)^actor\[" + wd[2] + rb"\] gate-route" + tail + rb" target=\d+" + f" session={SESSION}\n".encode(),
                    b"gate-response" + tail + f" session={SESSION} status=0\n".encode()]
        if checked:
            m = h.wait(rb"actor\[(\d+)\] policy-query " + transport + tail + b" watchdog=" + wd[1] + rb"\n")
            policies.add(m[1])
            patterns[2:2] = [rb"actor\[" + m[1] + rb"\] policy-" + action + b" " + transport + tail + b" watchdog=" + wd[1] for action in (b"query", b"admitted")]
        h.wait(b"gate-release " + transport + tail + b" reason=" + reason + rb" release=0\n")
        h.wait(b"watchdog-close" + tail + rb" remaining=\d+\n")
        at = 0
        for pattern in patterns:
            found = re.search(pattern, h.data[at:])
            assert found, (pattern, h.log.name)
            if found.groups():
                targets.add(found[1])
            at += found.end()
        assert len(re.findall(patterns[-1], h.data)) == 1
    assert len(targets) == 1 and len(policies) == int(checked)
    assert h.data.count(b"actor[" + targets.pop() + b"] player-echo gate=" + wd[2] + b"\n") == 2
    assert b"watchdog-close" in h.data and b"remaining=0" in h.data


def sample(mode, text):
    p = WORK / (mode + "-" + text + ".bin")
    run(CLI, "sample-" + mode, p, text)
    return p.read_bytes()


def exact(s, n):
    data = b""
    while len(data) < n:
        b = s.recv(n - len(data))
        assert b, "提前 EOF"
        data += b
    return data


def connect(endpoint):
    ip, port = endpoint.split(":")
    s = socket.create_connection((ip, int(port)), timeout=WAIT)
    s.sendall(HELLO)
    assert exact(s, len(HELLO)) == HELLO
    return s


def request(data, session, timeout=750, budget=128, key=None):
    frame = bytearray(data)
    struct.pack_into("<QIQ", frame, 96, session, budget, timeout)
    if key is not None:
        struct.pack_into("<I", frame, 12, key)
    return frame


def response(s, req, status=0):
    h = exact(s, 104)
    assert struct.unpack_from("<IHHI", h) == (1, 0, 3, status)
    assert h[12:92] == req[12:92] and h[96:104] == req[96:104]
    body = exact(s, struct.unpack_from("<I", h, 92)[0])
    assert body == (b"" if status else req[116:])


def business(h, tcp):
    login, echo, slow = sample("admission", "demo"), sample("request", "ACE"), sample("request", "slow")
    with connect(tcp) as s:
        s.sendall(login + echo)
        response(s, echo)
        for q, code in [(request(echo, 15, key=0x7fff), 12), (request(echo, 16, budget=0), 7), (request(slow, 17, timeout=80), 10)]:
            s.sendall(q)
            response(s, q, code)
        reused = request(echo, 17)
        s.sendall(reused)
        response(s, reused)
        for cmd, expected in [("tap", b"player-oneway"), ("online", b"player-online count=1"), ("bad-token", b"player-bad-token-denied")]:
            s.sendall(sample("notice", cmd))
            h.wait(expected)
        s.sendall(sample("notice", "push"))
        push = sample("notice", "pushed")
        assert exact(s, len(push)) == push  # 公共单向推送，无响应 session。
        h.wait(b"player-pushed")
        h.wait(b"player-slow-finished")
        assert not select.select([s], [], [], 0.18)[0], "迟到回复或虚构的单向回执"
        q = request(slow, 31)
        start = len(h.data)
        s.sendall(q)
        h.wait(b"player-echo", start)
        s.sendall(q)
        assert s.recv(1) == b"", "重复活跃 session 必须无歧义地拒绝"
    with connect(tcp) as s:
        q = request(echo, 31)
        s.sendall(login + q)
        response(s, q)
        h.wait(b"player-slow-finished", start)
        assert not select.select([s], [], [], 0.18)[0], "旧代次结果复活"
    print("普通 RPC: UnknownMethod/TooSmall/Timeout 空体、超时后复用、重复活跃 session 拒绝；单向无回执和显式 push 分开验证", flush=True)


def policy(text):
    actions = ''.join(f'  {{ kind = "{action}", alias = "admission" }},\n' for action in ("create", "prepare", "start"))
    text = text.replace('  { kind = "create", alias = "sessions" }', actions + '  { kind = "create", alias = "sessions" }')
    text = text.replace('default_target_alias = "player"', 'admission_policy_alias = "admission"\nadmission_timeout_ms = 300')
    return text + '\n[[instances]]\nimplementation="example.player"\nalias="admission"\nconfig="policy"\n'


def decisions(h, tcp):
    echo = sample("request", "ACE")
    for token in ("permissions", "deny", "unavailable", "late", "elsewhere"):
        begin = len(h.data)
        with connect(tcp) as s:
            s.sendall(sample("admission", token) + echo)
            h.wait(rb"policy-check\n", begin)
            if token == "permissions":
                h.wait(b"policy-bind-kick-online-denied", begin)
                response(s, echo)
                s.sendall(sample("notice", "other-caller"))
                h.wait(b"policy-other-push-denied", begin)
                assert not select.select([s], [], [], 0.18)[0], "非绑定来源获准推送"
            elif token == "elsewhere":
                # 无远端可靠订阅。实际 raw_call 遇到 Unreachable 后关连接，错误入本地出口不保证送达。
                h.wait(rb"gate-response conn=\d+:\d+ session=\d+ status=11\n", begin)
                received = b""
                while chunk := s.recv(128):
                    received += chunk
                    assert len(received) <= 104
                assert not received or (len(received) == 104 and struct.unpack_from("<IHHI", received) == (1, 0, 3, 11))
            else:
                assert s.recv(1) == b"", ("策略失败没有关闭", token)
        h.wait(rb"watchdog-close conn=\d+:\d+ remaining=0\n", begin)
        if token == "late":
            h.wait(rb"policy-late\n", begin)
        assert (b"gate-bound" in h.data[begin:]) == (token in ("permissions", "elsewhere"))
    # Close 先发生，但策略在自身 RPC 的期限内仍确实返回 Admitted；不能只靠超时丢弃来过关。
    start = len(h.data)
    with connect(tcp) as s:
        s.sendall(sample("admission", "closed"))
        h.wait(rb"policy-check\n", start)
    h.wait(rb"watchdog-close conn=\d+:\d+ remaining=0\n", start)
    h.wait(rb"policy-after-close\n", start)
    # 下一代的调用形成进展屏障：已关闭一代绝不能另有 Bind。
    old = h.wait(rb"gate-accepted Tcp conn=(\d+:\d+)", start)[1]
    with connect(tcp) as s:
        s.sendall(sample("admission", "demo") + echo)
        response(s, echo)
    h.wait(rb"gate-response conn=\d+:\d+ session=\d+ status=0\n", start)
    assert b"gate-bound conn=" + old + b" " not in h.data[start:]
    print("策略：CheckAdmission → Admitted 才 Bind；Denied/Unavailable/迟到均关闭。拒绝未授权 Bind/Kick/Online 与非绑定来源的真实令牌推送；远端不可达按调用结果关闭", flush=True)


def failed(name, text, status):
    cmd, path = provision(name, text)
    p = subprocess.run(cmd, input=b"", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=WAIT)
    path.write_bytes(p.stdout)
    assert p.returncode > 0 and f"Instance(Status({status}))".encode() in p.stdout, (name, p.stdout)
    assert b"gate-accepted" not in p.stdout and b"watchdog-open" not in p.stdout and "清单实例已发布".encode() not in p.stdout
    print(f"{name}: expected startup failure status={status} exit={p.returncode}; no Open", flush=True)
    return p.stdout


def startup_peers():
    def subscribe_to(text, alias):
        return text.replace('prepare_dependencies = ["gate"]', f'prepare_dependencies = ["{alias}"]').replace('gate_alias = "gate"', f'gate_alias = "{alias}"')

    log = failed("gate-to-player", DEFAULT.replace('watchdog_alias = "sessions"', 'watchdog_alias = "player"'), 2)
    assert b"gate-watchdog-check failed" in log and b"gate-listen" not in log
    log = failed("watchdog-to-player", subscribe_to(DEFAULT, "player"), 2)
    assert b"watchdog-gate-check failed" in log and b"watchdog-prepared" not in log and b"gate-listen" not in log

    # 两个真实的同角色实例。Watchdog 已成功订阅另一个 Gate，但要开监听的是原 Gate。
    other = next(block for block in DEFAULT.split('[[instances]]') if 'implementation = "gateway"' in block)
    text = subscribe_to(DEFAULT, "annex") + '\n[[instances]]' + other.replace('alias = "gate"', 'alias = "annex"')
    text = text.replace('actions = [', 'actions = [\n  { kind = "create", alias = "annex" },').replace(
        '{ kind = "start", alias = "gate" },', '{ kind = "start", alias = "gate" },\n  { kind = "prepare", alias = "annex" }, { kind = "start", alias = "annex" },')
    log = failed("watchdog-subscribed-other-gate", text, 4)
    prepared = re.search(rb'actor\[(\d+)\] watchdog-prepared gate=(\d+)', log)
    refused = re.search(rb'actor\[(\d+)\] gate-watchdog-check failed target=(\d+) status=4', log)
    assert prepared and refused and prepared[1] == refused[2] and prepared[2] != refused[1] and b"gate-listen" not in log


def manifest_gate():
    # 在 Gate 启动后挂起最后一个实例的就绪动作，验证两端口已绑定仍不接纳玩家。
    lib = WORK / "lib"
    lib.mkdir()
    ext = "dylib" if sys.platform == "darwin" else "so"
    for name in ("ace_service_gateway", "ace_service_watchdog", "ace_service_cluster", "ace_fixture_player", "runtime_echo"):
        file = f"lib{name}.{ext}"
        (lib / file).symlink_to((RELEASE / "examples" if name == "runtime_echo" else RELEASE) / file)
    text = DEFAULT.replace('"ace_fixture_player"]', '"ace_fixture_player", "runtime_echo"]').replace(
        '{ kind = "start", alias = "gate" },', '{ kind = "start", alias = "gate" },\n' +
        ''.join(f'  {{ kind = "{op}", alias = "last" }},\n' for op in ("create", "prepare", "start")))
    text = text.replace('[budgets]', '[budgets]\ntimeout_ms = 1800').replace('gate_alias = "gate"', 'gate_alias = "gate"\nadmission_timeout_ms = 1000')
    text += '\n[[instances]]\nimplementation="test.echo"\nalias="last"\nconfig="never-ready"\n'
    cmd, log = provision("manifest-not-ready", text, lib)
    with log.open("wb", buffering=0) as output, subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as p:
        h = Host(p, output)
        try:
            # 最后一个实例已 create：此刻 Watchdog 与 Gate 都已 Start、两个 listener 都已绑定。
            h.wait(rb"actor\[\d+\] create\n")
            tcp, kcp = [h.wait(rb"gate-listen " + t + rb" 127\.0\.0\.1:(\d+)\n")[1] for t in (b"Tcp", b"Kcp")]
            with socket.create_connection(("127.0.0.1", int(tcp)), timeout=WAIT) as s, socket.socket(type=socket.SOCK_DGRAM) as u:
                s.sendall(HELLO)
                u.sendto(struct.pack("<IHHQ", 1, 0, 0x301, 1), ("127.0.0.1", int(kcp)))
                assert not select.select([s, u], [], [], 0.2)[0], "总清单未就绪却开始玩家握手"
                h.wait(rb"Instance\(Status\(10\)\)")
            assert p.wait(WAIT) != 0 and b"gate-accepted" not in h.data and "清单实例已发布".encode() not in h.data
        finally:
            if p.poll() is None:
                p.kill()
                p.wait(WAIT)
        print(f"manifest-not-ready: 两监听已绑定，实际 TCP/KCP Hello 均未接纳；最后动作超时 exit={p.returncode}", flush=True)


@contextmanager
def signal_host(name, text, libs, stdin=subprocess.PIPE):
    command, path = provision(name, text, libs)
    # 默认保持 stdin 打开；独立的 daemon 用空设备。先 wait，不用 communicate 的 EOF 触发收尾。
    with path.open("wb", buffering=0) as output, subprocess.Popen(command, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as p:
        h = Host(p, output)
        try:
            yield h
        finally:
            if p.poll() is None:
                p.kill()
                p.wait(WAIT)
            tail = p.stdout.read()
            h.data += tail
            output.write(tail)


def shutdown_suite():
    import resource
    resource.setrlimit(resource.RLIMIT_CORE, (0, resource.getrlimit(resource.RLIMIT_CORE)[1]))
    lib = WORK / "lib"
    lib.mkdir()
    ext = "dylib" if sys.platform == "darwin" else "so"
    for name in ("runtime_echo", "ace_service_gateway", "ace_service_watchdog", "ace_service_cluster", "ace_fixture_player"):
        f = "lib" + name + "." + ext
        (lib / f).symlink_to((RELEASE / "examples" if name == "runtime_echo" else RELEASE) / f)
    cfg = explicit_policy(DEFAULT).replace('"sessions"', '"logins"').replace('"ace_fixture_player"]', '"ace_fixture_player", "runtime_echo"]').replace(
        'actions = [', 'actions = [\n' + ''.join(f'  {{ kind = "{op}", alias = "observer" }},\n' for op in ("create", "prepare", "start"))).replace(
        'first = ["gate"]', 'first = ["gate", "observer"]').replace('config = "echo"', 'manage = ["example.player"]\nconfig = "echo"')
    cfg += '\n[[instances]]\nimplementation="test.echo"\nalias="observer"\nconfig="pause-cancel"\n'
    cfg = cfg.replace("admission_timeout_ms = 300", "admission_timeout_ms = 3000")
    with signal_host("signal-active", cfg, lib) as h:
        h.wait("清单实例已发布".encode())
        tcp, kcp = [int(h.wait(rb"gate-listen " + t + rb" 127\.0\.0\.1:(\d+)\n")[1]) for t in (b"Tcp", b"Kcp")]
        login, pending = sample("admission", "permit"), sample("request", "pending-stop")
        with connect(f"127.0.0.1:{tcp}") as s, subprocess.Popen([CLI, "player-kcp", f"127.0.0.1:{kcp}", "--token", "permit", "--barrier"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as guest:
            assert select.select([guest.stdout], [], [], WAIT)[0] and guest.stdout.readline() == b"transport-ready\n"
            kcp_id = h.wait(rb"gate-accepted Kcp conn=(\d+:\d+) ")[1]
            s.sendall(login)
            tcp_id = h.wait(rb"gate-bound conn=(\d+:\d+) ")[1]
            s.sendall(pending)
            h.wait(rb"player-stop-pending child=\d+\n")
            begin = time.monotonic()
            h.child.send_signal(signal.SIGTERM)
            h.wait(rb"native-cancel\n")  # Gate 的 socket 已经析构，host 仍在有界夹具回调内。
            assert s.recv(1) == b""
            try:
                with socket.create_connection(("127.0.0.1", tcp), timeout=1):
                    raise AssertionError("已销毁 Gate 的监听仍可连接")
            except ConnectionRefusedError:
                pass
            with socket.socket(type=socket.SOCK_DGRAM) as u:
                u.sendto(struct.pack("<IHHQ", 1, 0, 0x301, 123), ("127.0.0.1", kcp))
                assert not select.select([u], [], [], 0.05)[0]
            assert h.child.poll() is None
            assert h.child.wait(WAIT) == 0
            elapsed = time.monotonic()-begin
            # 旧 KCP 客户端仍持有真实 token/conv；关闭后不会交付应用消息（UDP 没有 FIN）。
            out, _ = guest.communicate(input=b"x", timeout=WAIT)
            assert guest.returncode > 0 and b"player-response" not in out
    trace = h.data
    assert re.search(rb"entry=closed ready=false actors=7 pending=[1-9]", trace), trace
    names = re.findall(rb'^shutdown gone alias="([^"\n]+)"', trace, re.M)
    assert names == [b"gate", b"observer", b"created-player", b"admission", b"player", b"logins", b"members"], names
    assert trace.count(b"player-stop-task-dropped") == trace.count(b"watchdog-gate-gone") == 1
    assert sorted(re.findall(rb"gate-cleanup conn=(\d+:\d+) release=0\n", trace)) == sorted((tcp_id, kcp_id))
    assert trace.index(b"watchdog-gate-gone") < trace.index(b'shutdown stop alias="logins"')
    assert b"player-stop-resumed" not in trace and b"runtime=closed pending=0 actors=0 forced=false" in trace
    print(f"SIGTERM pid={h.child.pid} active+pending: closed TCP/KCP, dropped once, aliases={names}, exit=0 seconds={elapsed:.3f}", flush=True)

    with signal_host("signal-daemon", DEFAULT.replace("[shutdown]", "[shutdown]\nstdin_stop=false"), lib, subprocess.DEVNULL) as h:
        h.wait("清单实例已发布".encode())
        endpoint = h.wait(rb"gate-listen Tcp (127\.0\.0\.1:\d+)\n")[1].decode()
        login, echo = sample("admission", "demo"), sample("request", "ACE")
        with connect(endpoint) as s:
            s.sendall(login)
            h.wait(rb"gate-bound conn=")
            s.sendall(echo)
            response(s, echo)
            h.child.send_signal(signal.SIGTERM)
            assert h.child.wait(WAIT) == 0
    assert b"runtime=closed pending=0 actors=0 forced=false" in h.data
    print(f"SIGTERM pid={h.child.pid} stdin_stop=false: EOF 后仍有真实业务回复，信号退出 0", flush=True)

    # Prepare 不依赖 stdin，硬期限也不能依赖这唯一一个被 native 占住的 worker。
    for name, mode, grace, hard, expected in [("pending", "never-ready", 1500, 3000, 0), ("fault", "pause-prepare", 90, 2500, 1), ("hard", "pause-prepare", 60, 220, -signal.SIGABRT), ("cumulative", "pause-cancel", 1000, 3000, 1)]:
        aliases = ("boot", "second") if name == "cumulative" else ("boot",)
        actions = ','.join(f'{{kind="{op}",alias="{alias}"}}' for alias in aliases for op in ("create", "prepare", "start"))
        cfg = f'''revision=1
node=1
sequence_block=4
libraries=["runtime_echo"]
actions=[{actions}]
[shutdown]
first=["boot"]
cancel_after_ms={grace}
timeout_ms={hard}
[budgets]
workers=1
mailbox_entries=4
task_slots=2
polls_per_turn=1
payload_bytes=8192
''' + ''.join(f'[[instances]]\nimplementation="test.echo"\nalias="{alias}"\nconfig="{mode}"\n' for alias in aliases)
        with signal_host("signal-"+name, cfg, lib) as h:
            h.wait("清单实例已发布".encode() if name == "cumulative" else rb"native-prepare\n" if mode == "pause-prepare" else rb"prepare-pending\n")
            begin = time.monotonic()
            h.child.send_signal(signal.SIGTERM)
            if name == "cumulative":
                # 每次 Cancel 各停留 600ms；单项小于协作期限，总和超过。第二次信号不能续期。
                h.wait(rb'shutdown gone alias="boot"[^\n]+phase=Gone\(Status\(5\)\)')
                h.child.send_signal(signal.SIGTERM)
            assert h.child.wait(WAIT) == expected
        elapsed = time.monotonic()-begin
        if name == "cumulative":
            assert re.search(rb'shutdown gone alias="second"[^\n]+phase=Gone\(Status\(3\)\)', h.data)
            assert b"deadline=fault" in h.data and b"forced=true" in h.data
            assert h.data.count(b" cancel\n") == h.data.count(b" destroy\n") == 2
            print(f"SIGTERM twice pid={h.child.pid} two sequential cancels: cumulative deadline {grace}/{hard}ms exit=1 seconds={elapsed:.3f}", flush=True)
            continue
        assert "清单实例已发布".encode() not in h.data and b"startup=interrupted" in h.data
        if expected == 0:
            assert b"runtime=closed" in h.data and h.data.count(b" destroy\n") == 1
        elif expected == 1:
            assert b"native-returned" in h.data and b"deadline=fault" in h.data and b"forced=true" in h.data
            assert h.data.count(b" cancel\n") == h.data.count(b" destroy\n") == 1
        else:
            assert b"deadline=fault" in h.data and all(x not in h.data for x in (b"native-returned", b" destroy\n", b"runtime=closed"))
        print(f"SIGTERM pid={h.child.pid} startup={name} exit={expected} seconds={elapsed:.3f} grace/hard_ms={grace}/{hard}", flush=True)


def default_suite():
    print(f"player smoke: work={WORK}", flush=True)
    stopping = None
    try:
        with host("default", DEFAULT) as (h, tcp, kcp):
            both(h, tcp, kcp)
            business(h, tcp)
            start = len(h.data)
            stopping = connect(tcp)
            echo = sample("request", "ACE")
            stopping.sendall(sample("admission", "demo") + echo)
            response(stopping, echo)
            conn = h.wait(rb"gate-bound conn=(\d+:\d+)", start)[1]
        assert stopping.recv(1) == b"" and h.data.count(b"gate-cleanup conn=" + conn + b" release=0\n") == 1
        assert b"watchdog-gate-gone" in h.data[start:]
        print("正常停止：带活跃绑定的 Gate 只经专用 Cleanup 归还；Watchdog 收到 ResourceGone", flush=True)
    finally:
        if stopping is not None:
            stopping.close()
    with host("policy", policy(DEFAULT).replace('"sessions"', '"policy-sessions"')) as (h, tcp, _):
        decisions(h, tcp)
    startup_peers()
    with socket.socket(type=socket.SOCK_DGRAM) as occupied:
        occupied.bind(("127.0.0.1", 0))
        failed("second-listen-failed", DEFAULT.replace('kcp = "127.0.0.1:0"', f'kcp = "127.0.0.1:{occupied.getsockname()[1]}"'), 11)
    # 用现有 Cluster 的 leave_with 先占据 Gate 唯一的终态订阅槽。Watchdog 必须在 Prepare 拒绝。
    first = '{ kind = "create", alias = "gate" },'
    saturated = DEFAULT.replace(first, "").replace('actions = [', 'actions = [\n  ' + first)
    saturated = saturated.replace('[budgets]', '[budgets]\ncontrol_slots = 1').replace('role = "cluster"', 'role = "cluster"\nprepare_dependencies = ["gate"]').replace('seed = 1', 'leave_with = "gate"\nseed = 1')
    failed("watchdog-subscription-full", saturated, 1)
    manifest_gate()


def select_role(text, role, library, implementation):
    return text.replace(f'"ace_service_{role}"', f'"{library}"').replace(f'implementation = "{role}"', f'implementation = "{implementation}"')


def explicit_policy(text):
    # 保留 demo 的合法目标；策略装配失败绝不能靠缺目标来碰巧关闭。
    return policy(text).replace('admission_policy_alias =', 'default_target_alias = "player"\nadmission_policy_alias =').replace('config="policy"', 'config="explicit-policy"')


def policy_failures(h, endpoints, outcomes):
    wd = h.wait(rb"actor\[(\d+)\] watchdog-prepared gate=(\d+)\n")
    policy_id = h.wait(rb"actor\[(\d+)\] policy-query Tcp")[1]
    for outcome in outcomes:
        label = outcome.decode()
        start = len(h.data)
        clients = []
        try:
            for transport, endpoint in endpoints:
                mode = "player-" + transport.lower()
                p = subprocess.Popen([CLI, mode, endpoint, "--token", "demo"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                clients.append((transport, p))
            for transport, p in clients:
                t = transport.encode()
                conn = h.wait(rb"actor\[" + wd[2] + rb"\] gate-accepted " + t + rb" conn=(\d+:\d+) watchdog=" + wd[1], start)[1]
                tail = b" " + t + b" conn=" + conn + b" watchdog=" + wd[1] + b"\n"
                sender = rb"actor\[" + policy_id + rb"\] policy-"
                query = h.wait(sender + b"query" + tail, start)
                decision = h.wait(sender + outcome + tail, start + query.end())
                at = start + query.end() + decision.end()
                # 两端仍等待业务响应。reason=0 是本机 Gate 主动关闭；KCP idle 的 reason 是 10。
                release = h.wait(rb"actor\[" + wd[2] + rb"\] gate-release " + t + b" conn=" + conn + b" reason=0 release=0\n", at)
                h.wait(rb"actor\[" + wd[1] + rb"\] watchdog-close conn=" + conn + rb" remaining=\d+\n", at)
                if outcome == b"wait":
                    h.wait(sender + b"admitted" + tail, at + release.end())
                out, _ = p.communicate(timeout=WAIT)
                (Path(h.log.name).parent / f"{label}-{transport}.log").write_bytes(out)
                assert p.returncode != 0 and b"player-response" not in out, (label, transport, out)
                assert (b"UnexpectedEof" if transport == "Tcp" else "KCP 应答超时".encode()) in out, out
                for event in (b"watchdog-admitted", b"gate-bound", b"gate-route", b"gate-response"):
                    assert event + b" conn=" + conn + b" " not in h.data[start:]
                print(f"policy demo/{label}/{transport}: pid={p.pid} exit={p.returncode}, policy={policy_id.decode()} watchdog={wd[1].decode()} gate={wd[2].decode()} conn={conn.decode()} local close/release; no Bind or business", flush=True)
            assert b"remaining=0\n" in h.data[start:] and b"player-echo" not in h.data[start:]
        finally:
            for _, p in clients:
                if p.poll() is None:
                    p.kill()
                    p.wait(WAIT)


def fixed_decode_errors(h, tcp):
    # 本机夹具的两份已知类型负例：缺字段的固定记录和完好的另一种格式。
    # 目标仍是同一真实 Echo handler；不能用 ace 成功解出第二份、也不能发半个成功体。
    golden = dict(line.split() for line in Path("tools/test-client/golden.txt").read_text().splitlines())
    good = bytes.fromhex(golden["fixed.request"])
    packed = bytes.fromhex(golden["ace.request"])
    initial = len(h.data)
    with connect(tcp) as peer:
        peer.sendall(sample("admission", "demo") + good)
        response(peer, good)
        h.wait(rb"gate-response conn=\d+:\d+ session=" + str(SESSION).encode() + b" status=0\n", initial)
        for index, (body, error) in enumerate(((good[116:120], "Invalid"), (packed[116:], "Limit")), 51):
            req = request(good, index)
            req[116:] = body
            struct.pack_into("<I", req, 92, len(body))
            start = len(h.data)
            echo_at = len(h.codec.entries("ace_example_protocol::Echo"))
            reply_at = len(h.codec.entries("ace_example_protocol::EchoReply"))
            peer.sendall(req)
            response(peer, req, status=2)
            h.wait(f"方法 1026 编解码失败：{error}".encode(), start)
            h.wait(rb"gate-response conn=\d+:\d+ session=" + str(index).encode() + b" status=2\n", start)
            assert h.codec.entries("ace_example_protocol::Echo")[echo_at:] == b"D"
            assert h.codec.entries("ace_example_protocol::EchoReply")[reply_at:] == b""
            assert b"player-echo" not in h.data[start:]
        peer.sendall(good)
        response(peer, good)
    print("固定 codec 实库：缺字段 Invalid、已知 packed body 超固定字段预算 Limit；两次均无体 Protocol、未进入业务。随后合法请求成功", flush=True)


def fixed_counts(h, bytes_bind):
    # 此窗口只有两位玩家各一次完整成功生命周期。两端的实际 codec 入口分别计数。
    binds = 2 if bytes_bind else 0
    for dto, enc, dec in [("ace_example_protocol::Admission", 0, 2), ("ace_role_watchdog::Open", 4, 4),
                          ("ace_role_watchdog::Close", 2, 2), ("ace_role_gateway::Bind", binds, binds),
                          ("ace_role_gateway::Ack", binds, binds), ("ace_example_protocol::Echo", 0, 2),
                          ("ace_example_protocol::EchoReply", 2, 0), ("ace_role_watchdog::CheckAdmission", binds, binds),
                          ("ace_role_watchdog::AdmissionDecision", binds, binds)]:
        entries = h.codec.entries(dto)
        counts = entries.count(b"E"), entries.count(b"D")
        assert counts == (enc, dec), (dto, counts, enc, dec, h.codec.directory)
        print(f"{dto}: fixed host-side encode/decode={counts[0]}/{counts[1]} bytes[0:{len(entries)}]={entries!r}", flush=True)


def incompatible(text):
    for drift in ("role", "options", "request", "response", "codec"):
        if drift == "codec":
            cfg = select_role(text, "gateway", "fixed_gateway", "gateway")
        else:
            cfg = select_role(text, "watchdog", "incompatible_watchdog", "example.candidate")
        cmd, log = provision("incompatible-" + drift, cfg)
        env = dict(os.environ, ACE_FIXTURE_CANDIDATE=drift)
        expected = ("所需角色、修订或 codec 不匹配" if drift == "role" else "同方法键 wire 声明冲突").encode()
        for action, command in [("inspect", [HOST, "inspect-manifest", cmd[2], cmd[3]]), ("run", cmd)]:
            p = subprocess.run(command, input=b"", capture_output=True, env=env, timeout=WAIT)
            output = p.stdout + p.stderr
            log.with_name(action + ".log").write_bytes(output)
            assert p.returncode > 0 and expected in output and b"actor[" not in output and "清单实例已发布".encode() not in output, (drift, output)
            print(f"{drift}: {action} exit={p.returncode}, rejected before create", flush=True)


def main():
    if args.case == "shutdown":
        return shutdown_suite()
    if args.case == "default":
        return default_suite()
    print(f"{args.case}: work={WORK}", flush=True)
    if args.case == "assembly":
        startup_peers()
        return incompatible(DEFAULT)
    if args.case == "gateway":
        cfg = select_role(DEFAULT, "gateway", "bounded_gateway", "example.bounded-gate")
        with host(args.case, cfg) as (h, tcp, kcp):
            both(h, tcp, kcp)
            assert h.data.count(b"bounded-bind input=LocalOwned reply=LocalOwned transferred=true\n") == 2
            large = sample("request", "q" * 170)
            assert len(large) > 256
            with connect(tcp) as s:
                s.sendall(sample("admission", "demo") + sample("request", "ACE"))
                response(s, sample("request", "ACE"))
                start = len(h.data)
                s.sendall(large)
                assert s.recv(1) == b""
                conn = h.wait(rb"bounded-frame conn=(\d+:\d+) bytes=" + str(len(large)).encode() + b" limit=256", start)[1]
                h.wait(b"gate-release Tcp conn=" + conn + b" reason=0 release=0", start)
                h.wait(b"watchdog-close conn=" + conn + b" remaining=0", start)
        return
    cfg = DEFAULT
    if args.case.startswith("fixed"):
        for role in ("gateway", "watchdog", "cluster"):
            cfg = select_role(cfg, role, "fixed_" + role, role)
    if args.case in ("watchdog", "fixed-bytes"):
        cfg = cfg.replace('"fixed_watchdog"' if args.case == "fixed-bytes" else '"ace_service_watchdog"', '"policy_watchdog"').replace('implementation = "watchdog"', 'implementation = "example.policy-watchdog"')
        if args.case == "watchdog":
            assert b"policy-watchdog requires admission_policy_alias" in failed("no-external-policy", cfg, 9)
    if args.case != "fixed":
        cfg = explicit_policy(cfg)
    if args.case == "policy":
        # 与后一个清单仅相差一行。扩展实例本就存在，但未被该 Watchdog 选择。
        with host("unselected-policy", cfg.replace('admission_policy_alias = "admission"\n', '')) as (h, tcp, kcp):
            both(h, tcp, kcp)
            assert b"policy-query" not in h.data
    with host(args.case, cfg) as (h, tcp, kcp):
        selected = args.case != "fixed"
        both(h, tcp, kcp, "permit" if selected else "demo", selected)
        if args.case.startswith("fixed"):
            fixed_counts(h, args.case == "fixed-bytes")
            if args.case == "fixed":
                business(h, tcp)
                fixed_decode_errors(h, tcp)
                for dto in ("Online", "OnlineCount", "SendToSession", "Ack"):
                    entries = h.codec.entries("ace_role_gateway::" + dto)
                    assert b"E" in entries and b"D" in entries, (dto, entries)
        if args.case == "policy":
            policy_failures(h, [("Tcp", tcp), ("Kcp", kcp)], (b"denied", b"unavailable", b"wait"))
        if args.case == "fixed-bytes":
            start = len(h.codec.entries("ace_role_gateway::Kick"))
            policy_failures(h, [("Tcp", tcp)], (b"denied",))
            assert h.codec.entries("ace_role_gateway::Kick")[start:] == b"ED"


if __name__ == "__main__":
    main()
