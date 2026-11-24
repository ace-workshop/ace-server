#!/bin/sh
# 从任意目录运行同一套本地/CI 验证。任何必要命令失败即退出。
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
# release 全 workspace 构建与实际装载在末尾的 smoke-abi.sh 一起执行。
cargo test -p ace-server-sdk -p ace-codec --no-default-features --locked
# 生产宿主的正常 feature、纯帧、纯调用方与无默认服务分别求解。
check_tree() {
    pkg=$1; forbidden=$2; shift 2
    tree=$(cargo tree --locked --edges normal --prefix none -p "$pkg" "$@")
    if printf '%s\n' "$tree" | grep -Eq "^($forbidden) "; then
        printf '依赖方向错误：%s %s\n' "$pkg" "$*" >&2; exit 1
    fi
}
ace='ace-serialize(-derive)?'
roles='ace-role-(gateway|watchdog|cluster)'
for pkg in ace-server-host ace-server-sdk ace-codec ace-net-proto; do
    check_tree "$pkg" "$ace|$roles" --no-default-features
done
native="ace-server-(host|abi|local|sdk)|abi_stable|$roles|tokio|bevy(_.*)?"
check_tree ace-server-host "ace-codec|$ace|$roles|ace-(gateway|watchdog|cluster|server-sdk|example-protocol)"
check_tree ace-net-proto "ace-transport|kcp|ace-codec|$ace|$native"
check_tree ace-transport "ace-codec|$ace|$native"
check_tree ace-example-protocol "$ace|$native"
check_tree ace-test-client "$native"
check_tree ace-test-client "$ace|$native" --no-default-features
for pkg in ace-gateway ace-watchdog ace-cluster ace-fixture-player ace-fixture-caller ace-fixture-echo ace-fixture-policy; do
    check_tree "$pkg" "$ace|ace-server-host" --no-default-features
done
# 角色薄入口是 examples，连同 dev 边求解禁默认构建。
check_tree ace-fixture-policy "$ace|ace-server-host" --no-default-features --edges normal,dev
cargo test -p ace-fixture-policy --no-default-features --locked
cargo test -p ace-test-client --no-default-features --locked
python3 -B tools/role-smoke.py
