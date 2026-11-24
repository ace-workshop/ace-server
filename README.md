# ace-server

Rust Actor 服务端。三个内置服务分别负责玩家路由（Gateway）、准入／会话（Watchdog）和单 seed 成员目录（Cluster）。同一 Gate 同时接 TCP 与 KCP；业务 RPC 可在本地转移布局兼容的稳定对象，跨节点走 host 间 TCP 直连。宿主只在启动时装载受信库；支持运行中创建／停止实例，不支持热换库。示例不是生产鉴权、裸 UDP 游戏接口或跨实体机性能证明。

## 构建与运行

从本仓独立工作树的根目录运行，需 Rust 1.98.1、系统链接器和 Python 3.9+。Cargo 按本仓锁从 crates.io 和[固定的公开序列化依赖](https://github.com/ace-workshop/ace-serialize/tree/4a18e69aef27d79c155a30bec4b13abb16c635b2)构建；无需相邻仓、私有缓存或路径覆盖。

```sh
cargo build --workspace --release --locked
host=target/release/ace-server-host
"$host" inspect-manifest config/server.toml target/release   # 核验真实库，不发号、不开端口
work=$(mktemp -d)
"$host" provision-new 1 "$work/identity"                    # 仅为此前未用的演示节点
"$host" run-manifest config/server.toml target/release "$work/identity"
```

默认清单需要回环 TCP 44001 空闲。等待「清单实例已发布」，将同一 `gate-listen Tcp` / `gate-listen Kcp` 行中的实际 `IP:port` 分别赋给 `TCP_ENDPOINT` / `KCP_ENDPOINT`，在另一终端运行：

```sh
target/release/ace-test-client player-tcp "$TCP_ENDPOINT"
target/release/ace-test-client player-kcp "$KCP_ENDPOINT"
```

两条路径均经过同一 Watchdog 的 demo 准入，收到样例业务 handler 的一份关联自动回复。`example.player` 是夹具，不是第四种内置。已投产的身份目录不得重置、回滚或复制；重启只用 `run-manifest`。向**该 host PID** 发送 `SIGTERM` 即按清单有界收尾；默认发布后输入字节或 EOF 也会停止。进程管理器关闭 stdin 时可配置 `stdin_stop = false`。别名、期限、步骤和强制失败见[部署与关闭](docs/architecture.md#部署与有界关闭)。

## 验证与扩展

```sh
sh tools/check.sh                 # Linux/macOS 共用：标准 Cargo 门、依赖方向、release 实库和回环
sh tools/smoke-abi.sh             # 仅直接集成：ABI、调度/RPC、TCP/KCP、三节点、默认玩家和 SIGTERM
python3 -B tools/role-smoke.py     # 可独立从空 target 构建；含上项，另逐库替换／策略／整组 codec
results=$(mktemp -d)
python3 -B tools/measure.py --results "$results"   # 可选：同机 Actor 和队列对照，不是 CI 速度门
```

实测有意触发可捕获的 panic 和仅针对受控子进程的 fail-stop，以断言和整条命令的退出码判断。`CARGO_TARGET_DIR` 可选择独立构建目录；`role-smoke.py --results <已有目录>` 保留各实际清单、进程日志和产物摘要。共享 runner 的结果只代表该次提交／平台，不代替指定机器验证。

| 入口 | 用途 |
| --- | --- |
| [架构](docs/architecture.md) · [需求与非目标](docs/requirements.md) | 系统图、Actor 调度、玩家准入、运行范围及测量条件 |
| [实例与本地消息](docs/plugin-abi.md) | native 契约、create/stop、两方向选路与所有权 |
| [替换与 codec 装配](docs/architecture.md#替换与-codec-装配) | 只换一个角色、只换策略，或重编匹配的服务和客户端 |
| [传输](docs/transport.md) · [动态节点](docs/cluster.md) | 双协议、预算、地址持久化、回环命令与故障边界 |
| [开发约束](AGENTS.md) | 本仓写域、依赖与验证要求 |

本仓是一个 Cargo workspace：`kernel/` 为通用 host／ABI／稳定对象及原生传输，`services/` 只有三个角色的实现、协议和薄入口，`sdk/` 与 `protocols/` 分别为插件适配与共享契约。host 不链接具体角色或 codec。组合仓以正式组件提交做 gitlink；本仓独立维护锁。
