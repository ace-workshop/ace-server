#!/usr/bin/env python3
"""本机 CPU 上的有限 release 实测；不连接外网服务，不设置吞吐门槛。"""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--results", type=Path, required=True, help="已有的结果父目录；每次新建子目录")
p.add_argument("--workers", type=int, default=None, help="多 worker 对照，2 到可用数（最多 8，默认至多 4）")
args = p.parse_args()
root = Path(__file__).resolve().parents[1]
os.chdir(root)
available = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
workers = min(4, available) if args.workers is None else args.workers
if not 2 <= workers <= min(8, available):
    p.error("多核对照需要至少两个可用核；--workers 不得超过 8 或可用数")
work = Path(tempfile.mkdtemp(prefix="measure.", dir=args.results.resolve()))
print(f"results={work} OS={sys.platform} arch={platform.machine()} CPUs={available} multi={workers}", flush=True)
subprocess.run(["rustc", "-Vv"], check=True)
if sys.platform == "darwin":
    subprocess.run(["sysctl", "machdep.cpu.brand_string", "hw.physicalcpu", "hw.logicalcpu"], check=True)
else:
    subprocess.run(["lscpu"], check=True)


def cargo(*command, capture=False):
    print("+ cargo", *command, flush=True)
    return subprocess.run(["cargo", *command], stdout=subprocess.PIPE if capture else None, text=True, check=True, timeout=900).stdout


for role in ("caller", "echo"):
    cargo("build", "--locked", "--release", "-p", "ace-fixture-" + role, "--example", "runtime_" + role)
target = Path(json.loads(cargo("metadata", "--no-deps", "--locked", "--format-version", "1", capture=True))["target_directory"])
artifacts = cargo("test", "--release", "--locked", "-p", "ace-server-host", "--lib", "--no-run", "--message-format=json-render-diagnostics", capture=True)
[binary] = [m["executable"] for m in map(json.loads, artifacts.splitlines()) if m.get("executable") and m["target"]["kind"] == ["lib"]]


def case(test, backend, shape, count):
    d = work / f"{backend}-{shape}-{count}"
    d.mkdir()
    if test == "actor_rpcs":
        actors = [("echo", 0)] if shape == "single" else [(role, i) for i in range(4) for role in ("caller", "echo")]
        actions = ','.join('{kind="%s",alias="%s%d"}' % (op, r, i) for r, i in actors for op in ("create", "prepare", "start"))
        (d / "node.toml").write_text(f'''revision=1
node=1
sequence_block=256
libraries=["runtime_caller","runtime_echo"]
actions=[{actions}]
[budgets]
workers={count}
instances=8
mailbox_entries=128
control_slots=128
task_slots=128
polls_per_turn=16
payload_bytes=131072
''' + ''.join(f'[[instances]]\nimplementation="test.{r}"\nalias="{r}{i}"\nconfig="measure-{backend}"\n' for r, i in actors))
    command = [binary, "--ignored", "--exact", "runtime::measurements::" + test, "--nocapture"]
    with (d / "result.log").open("w") as log:
        env = dict(os.environ, ACE_LOAD=shape, ACE_BACKEND=backend, ACE_WORKERS=str(count), ACE_MEASURE_DIR=str(d), ACE_RUNTIME_LIBS=str(target / "release/examples"))
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=60)
    lines = (d / "result.log").read_text().splitlines()
    print(d.name, *[line for line in lines if line.startswith(("rpc ", "backend ", "measurement "))], "exit=" + str(result.returncode), sep="\n", flush=True)
    result.check_returncode()


for count in (1, workers):
    for shape in ("single", "uniform", "hot"):
        for backend in ("local", "bytes"):
            case("actor_rpcs", backend, shape, count)
for shape in ("uniform", "hot"):
    for backend in ("array", "mpsc"):
        case("mailbox_backends", backend, shape, workers)

dto = cargo("test", "--locked", "--release", "-p", "ace-fixture-model", "--lib", "runtime::dto_conversion", "--", "--ignored", "--exact", "--nocapture", capture=True)
(work / "dto.log").write_text(dto)
print(dto, end="", flush=True)
