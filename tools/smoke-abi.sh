#!/bin/sh
# 从任意目录运行同一套本地/CI 验证。任何必要命令失败即退出。
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
cargo build --workspace --release --locked
cargo build -p ace-server-host --example loader-check --release --locked
# 读取 Cargo 实际 target，支持 CARGO_TARGET_DIR；不假定在仓内。
metadata=$(cargo metadata --locked --no-deps --format-version 1)
target=$(printf '%s' "$metadata" | python3 -Bc 'import json,sys;print(json.load(sys.stdin)["target_directory"])')/release
case "$(uname -s)" in Darwin) ext=dylib;; Linux) ext=so;; *) echo '只验证 Linux 与 macOS' >&2; exit 1;; esac
set -- "$target/libace_service_gateway.$ext" "$target/libace_service_watchdog.$ext" "$target/libace_service_cluster.$ext" "$target/libace_fixture_policy.$ext" "$target/libace_fixture_player.$ext"
"$target/ace-server-host" inspect-manifest config/server.toml "$target"
"$target/examples/loader-check" "$@"
ACE_FIXTURE_WRONG_VERSION=1 "$target/examples/loader-check" "$@"
"$target/ace-abi-smoke" "$target/libace_fixture_caller.$ext" "$target/libace_fixture_echo.$ext"

exchange_dir=$(mktemp -d "$target/exchange.XXXXXX")
host="$target/ace-server-host"
driver="$target/examples/loader-check"
identity() { python3 -Bc 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$1"; }
host_sha=$(identity "$host")
driver_sha=$(identity "$driver")
exchange() {
    client=$1; policy=$2; cluster=$3
    "$host" inspect "$policy" "$cluster"
    "$client" sample-request "$exchange_dir/request"
    "$driver" exchange "$exchange_dir/request" "$exchange_dir/response" "$policy" "$cluster"
    "$client" check-response "$exchange_dir/response"
}
# 默认纯调用方也不依靠全 workspace 统一开启的 native/角色 feature。
cargo build -p ace-test-client --release --locked
exchange "$target/ace-test-client" "$target/libace_fixture_policy.$ext" "$target/libace_service_cluster.$ext"
fixed="${target%/release}/fixed-codec"
# 三个独立求解，不能让纯客户端因其它测试产物的 feature 统一而链接角色/native。
cargo build -p ace-test-client --no-default-features --target-dir "$fixed" --release --locked
cargo build -p ace-fixture-policy --lib --no-default-features --target-dir "$fixed" --release --locked
cargo build -p ace-fixture-policy --example fixed_cluster --no-default-features --target-dir "$fixed" --release --locked
exchange "$fixed/release/ace-test-client" "$fixed/release/libace_fixture_policy.$ext" "$fixed/release/examples/libfixed_cluster.$ext"
test "$host_sha" = "$(identity "$host")" && test "$driver_sha" = "$(identity "$driver")" || exit 1
printf '两次装载同一宿主 SHA-256=%s，驱动 SHA-256=%s（文件字节互通，不是网络 E2E）\n' "$host_sha" "$driver_sha"

# 通过正式服务根表分别装载，而非以旧布局探针代替运行时。禁止测试 abort 写 core。
cargo build -p ace-fixture-caller --example runtime_caller --release --locked
cargo build -p ace-fixture-echo --example runtime_echo --release --locked
"$driver" runtime tests/fixtures/runtime.toml "$target/examples"
"$host" provision-new 1 "$exchange_dir/identity"
"$host" run-manifest tests/fixtures/runtime.toml "$target/examples" "$exchange_dir/identity" </dev/null
# 保留真正的缺依赖反例：无受管 peer 端点的 Cluster 不得发布。
sed '/^\[peers\]/,/^\[budgets\]/{ /^\[budgets\]/!d; }' config/server.toml >"$exchange_dir/no-peers.toml"
if "$host" run-manifest "$exchange_dir/no-peers.toml" "$target" "$exchange_dir/identity" </dev/null >"$exchange_dir/unready.log" 2>&1; then
    echo '缺少 peer 部署的默认组合不应发布' >&2; exit 1
fi
grep -F 'Instance(Status(4))' "$exchange_dir/unready.log"
(ulimit -c 0; ACE_RUNTIME_LIBS="$target/examples" cargo test -p ace-server-host --lib --release --locked runtime::tests::runtime_real_plugins -- --ignored --exact --nocapture)

(ulimit -c 0; ACE_RUNTIME_LIBS="$target/examples" cargo test -p ace-server-host --lib --release --locked runtime::tests::first_cleanup_fault_retires_instance -- --ignored --exact --nocapture)
ACE_RUNTIME_LIBS="$target/examples" cargo test -p ace-server-host --lib --release --locked runtime::tests::shutdown_rejects_ready_ticket_and_new_entry -- --ignored --exact --nocapture

# 前置算术域也用开启溢出检查的 release 生产适配器验证；实库双传输测试另起进程。
cargo test -p ace-transport --release --locked
ACE_NETWORK_LIBS="$target" ACE_NETWORK_CLIENT="$target/ace-test-client" cargo test -p ace-server-host --lib --release --locked runtime::network::tests::dual_transport_real_owner -- --ignored --exact --nocapture

ACE_NETWORK_LIBS="$target" cargo test -p ace-server-host --lib --release --locked runtime::network::tests::gateway_close_keeps_slot_until_watchdog_accepts -- --ignored --exact --nocapture

# 节点控制与数据走独立进程的正式 host 和 Actor 动态库；驱动只连接本次回环端口。
python3 -B tools/cluster-smoke.py "$target" "${ACE_SMOKE_RESULTS:-$target}"

# 正式默认清单：同一 Gate 两个真实客户端及角色失败边界。
python3 -B tools/player-smoke.py "$target" "${ACE_SMOKE_RESULTS:-$target}"

# 独立进程发送 SIGTERM（保持 stdin 打开），包括初始化、活动会话、协作故障和硬期限。
python3 -B tools/player-smoke.py "$target" "${ACE_SMOKE_RESULTS:-$target}" --case shutdown
