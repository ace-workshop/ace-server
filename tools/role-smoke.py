#!/usr/bin/env python3
"""从源码构建并实际运行单角色替换、外部策略与整组 codec。保留每个部署、清单及二进制身份。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import time


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base-release", type=Path, help="只读：同布局的已验收 host 和三默认库；必须在本次 Cargo target 之外")
parser.add_argument("--results", type=Path, default=os.environ.get("ACE_SMOKE_RESULTS"), help="已有的结果父目录；其下为本次新建专属目录")
args = parser.parse_args()
os.chdir(Path(__file__).resolve().parents[1])
target = Path(json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]))["target_directory"]).resolve()
if sys.platform not in ("darwin", "linux"):
    sys.exit("只验证 macOS / Linux")
if args.base_release:
    delivery = args.base_release.resolve()
    if delivery == target or target in delivery.parents or delivery.parent == target or delivery.parent in target.parents:
        sys.exit("基准交付与 Cargo 输出须使用不同的 target 目录")
    if args.results and (args.results.resolve() == delivery.parent or delivery.parent in args.results.resolve().parents):
        sys.exit("结果目录不能写入只读基准 target")
if args.results is None:
    target.mkdir(exist_ok=True, parents=True)
work = Path(tempfile.mkdtemp(prefix="roles.", dir=args.results or target)).resolve()
print(f"role smoke results={work} cargo-target={target}", flush=True)


def run(*command, env=None, timeout=1200):
    text = shlex.join(map(str, command))
    print(f"+ {text}", flush=True)
    start = time.monotonic()
    rc = subprocess.run(command, env=env, timeout=timeout).returncode
    print(f"exit={rc} seconds={time.monotonic()-start:.2f}: {text}", flush=True)
    if rc != 0:
        raise SystemExit(rc)


def library(name):
    return f"lib{name}.{'dylib' if sys.platform == 'darwin' else 'so'}"


def sha(path):
    with path.open("rb") as file:
        digest = hashlib.sha256()
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
        return digest.hexdigest()


def snapshot(paths):
    return ''.join(f"{sha(p)}  {p}\n" for p in sorted(paths))


base = (args.base_release or target / "release").resolve()
invariants = [base / "ace-server-host", *(base / library("ace_service_" + role) for role in ("gateway", "watchdog", "cluster"))]
original = snapshot(invariants) if args.base_release else None
# 保留 ABI、运行时、两种传输、原多节点和启动对端的全部断言。
run("sh", "tools/smoke-abi.sh", env=dict(os.environ, ACE_SMOKE_RESULTS=str(work)))
original = original or snapshot(invariants)
(work / "base-before.sha256").write_text(original)
assert original == snapshot(invariants)

# 分别求解，替换库的 build 从不以固定的 release 目录为输出。
variants = target / "role-variants"
fixed = target / "fixed-codec"

def build(directory, packages, *options):
    run("cargo", "build", "--locked", "--release", "--target-dir", directory, *[arg for p in packages for arg in ("-p", p)], *options)

build(variants, ["ace-fixture-policy"], "--examples")
build(fixed, ["ace-fixture-policy"], "--examples", "--no-default-features")
build(fixed, ["ace-fixture-caller", "ace-fixture-echo"], "--examples", "--no-default-features")
build(fixed, ["ace-fixture-player"], "--lib", "--no-default-features")
# 客户端只有纯协议与传输，不能和上面的角色/native features 统一求解。
build(fixed, ["ace-test-client"], "--no-default-features")
run("cargo", "test", "--locked", "-p", "ace-fixture-codec", "--no-default-features", "--features", "player,cluster")

def stage(src, dst):
    dst.parent.mkdir(exist_ok=True, parents=True)
    shutil.copy2(src, dst)
    assert sha(src) == sha(dst)
    print(f"stage {sha(dst)} {src} -> {dst}", flush=True)

ace, alt = work / "ace", work / "fixed"
for p in invariants:
    stage(p, ace / p.name)
alt.mkdir()
(alt / "ace-server-host").symlink_to(ace / "ace-server-host")
for deploy, source in ((ace, target / "release"), (alt, fixed / "release")):
    for name in ("ace-test-client", library("ace_fixture_player"), *("examples/" + library("runtime_" + n) for n in ("caller", "echo"))):
        stage(source / name, deploy / name)
for name in ("bounded_gateway", "policy_watchdog", "export_cluster", "incompatible_watchdog"):
    stage(variants / "release/examples" / library(name), ace / library(name))
for name in ("fixed_gateway", "fixed_watchdog", "fixed_cluster", "policy_watchdog"):
    stage(fixed / "release/examples" / library(name), alt / library(name))
(ace / library("fixed_gateway")).symlink_to(alt / library("fixed_gateway"))  # 仅装配反例。
files = [p for deploy in (ace, alt) for p in deploy.rglob("*") if p.is_file()]
before = snapshot(files)
(work / "binaries-before.sha256").write_text(before)
for old, new in [("gateway", "bounded_gateway"), ("watchdog", "policy_watchdog"), ("cluster", "export_cluster")]:
    assert sha(ace / library("ace_service_" + old)) != sha(ace / library(new))


def case(deploy, script, option, choice):
    output = work / (script + "-" + choice)
    output.mkdir()
    run(sys.executable, "-B", f"tools/{script}-smoke.py", deploy, output, option, choice, timeout=180)
    # 取实际交给 host 的清单与库目录，而非从 case 名推测装载了哪几个文件。
    for cfg in sorted(output.glob("**/node.toml")):
        root = cfg.parent.parent / "lib" if script == "cluster" or cfg.parent.name == "manifest-not-ready" else deploy
        # 这些脚本生成的 libraries 恰为一行 JSON 字符串数组；TOML 的权威解析仍在 host。
        listing = next(line.split("=", 1)[1] for line in cfg.read_text().splitlines() if line.partition("=")[0].strip() == "libraries")
        libs = [root / library(n) for n in json.loads(listing)]
        paths = [deploy / "ace-server-host", *libs]
        (cfg.parent / "deployment.sha256").write_text(snapshot(p.resolve(strict=True) for p in paths))


# 补同一固定 codec 的独立 caller/echo：含单独选择的 Local 请求 / Bytes 响应。
run(target / "release/examples/loader-check", "runtime", "tests/fixtures/runtime.toml", alt / "examples")
case(ace, "player", "--case", "default")
case(ace, "player", "--case", "gateway")
case(ace, "player", "--case", "watchdog")
case(ace, "cluster", "--roles", "export")
case(ace, "player", "--case", "assembly")
case(ace, "player", "--case", "policy")
case(alt, "player", "--case", "fixed")
case(alt, "player", "--case", "fixed-bytes")
case(alt, "cluster", "--roles", "fixed")
after, final_base = snapshot(files), snapshot(invariants)
(work / "binaries-after.sha256").write_text(after)
(work / "base-after.sha256").write_text(final_base)
assert final_base == original and before == after
print(f"同一 host、逐项交付的前后 SHA-256 均相同。实际部署见 {work}", flush=True)
