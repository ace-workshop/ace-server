# 受管节点与远端调用

一个节点是一个 `ace-server-host` 进程，有自己的 NodeId、独占身份目录、Tokio runtime 和服务表。`Cluster<C>` 是通常的 SDK Actor：它选择 seed、管理目录和重连，并处理 `Resolve(node, alias)`。**目录中有成员不等于已有业务路由。** 已就绪的 send/call 直接在两个 host 之间走 TCP，不逐条经过 Cluster 或 seed。没有多 seed、自动选主、Actor 迁移或公网节点认证。

## 复现

```sh
sh tools/smoke-abi.sh
# 只重跑集群部分（沿用上一步 release 产物，默认 target 目录）：
python3 -B tools/cluster-smoke.py target/release
```

脚本输出本次专属目录。`1/node.toml`、`2/node.toml` 等是完整清单，`N/run-K.log` 分别是第 N 个节点的第 K 次进程输出。A/B 先双向解析并调用，再加入 C；测试 caller 只监听本机临时 TCP 端口来触发普通 SDK 调用。`peer-selected` 的 `dialer-low`/`attempt` 可在两端核对，`link` 是**本机**代次；`peer-input` 的 source 和 target 来自远端 Actor 的信封，测试中的请求 27 加上样本向量得到 42。`heartbeat-repair` 可区分从 seed 补目录与非 seed 之间的直接业务。测试还实际暂停／恢复和终止自己创建的进程，使用第四个临时节点并发入会、制造未收到主动通知和首次加入失败；它不是第四个内置服务。测试专用 caller 不是可部署的管理端口。

手工在受管网络运行时，每个节点独立 `provision-new` 一次，用各自清单、受信库目录和持久目录运行 `run-manifest`；使用 SIGTERM 触发[配置顺序的有限关闭](architecture.md#部署与有界关闭)。旧清单默认保留输入字节或 EOF 时逆序回收。**不要用 provision-new 重置节点**。一个只运行默认成员控制器的最小 seed 清单如下。测试示例额外配置了用于验证调用的 Actor；仅有下面的控制器不会凭空生成 echo 服务。

```toml
revision = 1
node = 1
sequence_block = 256
libraries = ["ace_service_cluster"]
actions = [{kind="create",alias="members"}, {kind="prepare",alias="members"}, {kind="start",alias="members"}]
[budgets]
mailbox_entries = 128
payload_bytes = 1048576
task_slots = 32
polls_per_turn = 16
[peers]
advertised = "127.0.0.1:44001"
allow = [{ip="127.0.0.1",nodes=[1,16],ports=[44001,44016]}]
[[instances]]
implementation = "cluster"
alias = "members"
role = "cluster"
grants = {peer_control=true}
config = '''
seed = 1
seed_endpoint = "127.0.0.1:44001"
ports = [44001,44016]
'''
```

第二个节点可使用同样的部署域和种子；把 `node` 改为 2、`advertised` 改为 `127.0.0.1:44002`，使用**不同的**首次身份目录。端点只用数值 IP 和非零端口，不解析主机名。`allow` 是受管 IP、闭区间节点号和监听端口的许可，不是启动成员表；最多 256 条。入站核对固定握手中的节点与 socket **观测 IP**；出站还核对实际端口、预期节点和拨号方身份。默认 Cluster 对首次声明的监听端点用 `peer_connect` 反向建实际候选，从该端点核对同一 boot 和完整 Join 后才登记。普通节点首先向配置的 seed 端点确认本代身份，不将来自任意候选的快照当作权威。部署须隔离外部网络、保护身份目录和 native 库；这些核对不是 TLS、对恶意受信节点的密码学隔离或玩家登录。

## 加入、就绪与退出

- 固定 `PeerHello` 在任何角色解码之前完成。之后只有受权 owner 的匹配方法方向、codec 三元组、双摘要和阶段才可收发角色控制体。成功 `peer_admit` 只核准**这一代**链接；两端分别完成本地清单发布才发送 `PeerReady`。双方均准入且已就绪才有业务通路。本地启动只等加入结果，不等对方的业务 ready。
- 单 seed 维护 `(seed_boot, version)` 的完整快照。重复版本不重复执行；错误 epoch、来源、矛盾的同版本、重复节点／端点或超额快照不能作为新目录。不截断成员。默认 `max_members=32`（最多 256），`snapshot_bytes=16384`（256–65536，且不超过实例负载上限）；编码与解码有独立工作区和物化上限。目录变更时最多向 `notify_fanout=8` 条已就绪连接尽力通知，其余在心跳中报告版本并取得**最新完整快照**。无需保留历史更新队列。
- `heartbeat_ms=250`（25–10000）、`lease_ms=3000`（至少三次心跳，最多 120000）、`retry_max_ms=2000`（心跳间隔至 60000）。每个期望节点的常规拨号只保留一个进行中的尝试；失败后从心跳间隔倍增至上限。Hello 和候选准入各有 3 秒时限，第一次入会另受宿主 `timeout_ms` 的初始化期限约束。预留足够的监听、候选、控制槽和网络字节预算；全互连一共 O(N²) 条链接，16 位编号不是并发性能承诺。
- 低 NodeId 向高 NodeId 常规拨号。种子引导或端点确认可能临时有反向连接；同一活跃 boot 的合格候选在两端都优先低号发起方，再比较发起方的 boot 与递增 attempt，不依赖本机资源号或到达次序。新选定的链接可替换旧链接，旧 Closed 不撤新代。
- **链路断开**使该代路由立即不可用；该代尚未完成的调用得到一次 Unreachable，不能由旧响应命中新调用。局部断线不修改全局成员资格。**正常 Leave** 或 **seed 观测到心跳失活**才推进移除版本。可将 Cluster 的 `leave_with="业务别名"` 配合实例 `prepare_dependencies=["业务别名"]`，先创建该业务实例：Cluster 在 Prepare 订阅它；业务退役触发运行中的 Cluster 发 Leave 并撤直连。若需主动退会，应先停止所订阅的实例，再停止控制器。进程退出不保证送达 Leave；缺失时按失活处理，不以裸 FIN 冒充全局离开。
- 新握手可使失活但未重启的同一 boot 重新登记。活跃身份冲突不自动顶替，低 boot 不得复活。`peer-watermarks` 在首次身份配置时写入，同目录锁、校验和、同步临时文件、原子替换并同步目录。在有界管理 I/O 完成前 `peer_admit` 返回 Busy；同代 Writable 后重试，成功前 seed 不发布。存储结果不确定时拒绝再准入，默认 Cluster 进入 Faulted，宿主关停其所有链接；其它节点间的健康 TCP 仍可用。启用节点传输的目录缺少或损坏这个文件时拒绝启动，不能默认为从未准入。旧版仅本机身份仍可用于本机模式，不要通过手写空水位给旧身份开启成员能力。
- seed 丢失时保留已准入的健康直连，首次加入不发布，期限到达后清理。seed 恢复时保留高水位，用新的本机 boot 重建活跃目录；各节点重新登记，不恢复旧链接。身份目录丢失、克隆或完整旧备份回滚不能靠校验和识别；本机崩溃／同步失败试验不证明真实掉电行为。

## 业务数据与后压

业务方先向本地成员服务调用角色方法 `Resolve { node, alias }`。它对本机查询运行中的注册表，对指定远端向该节点的成员服务发独立的 typed RPC；不缓存业务别名，不替调用者自动重投新实例。取得 `ServiceAddress` 后照常用 `Context::send` / `call`。跨节点两方向始终是 Bytes，即使两份库声明兼容 LocalOwned 也不移动本地对象。

接纳共用本地调用的 pending、完成槽、请求期限与回复预算。每个 socket 有独立的条数／字节出口；被调端复核授权 peer、source 节点、真实本机目标与方法元数据，并独立取得 mailbox、入站 token 及响应预算。send 只承诺本地接纳；远端 send 拒收记日志。回复在原 socket 代次返回，必须匹配原 caller、target、session 和方法；成功还核对完整契约和本次预留，无体错误不经 decoder。临时成功出口满时经合并的 `ReplyWritable` 重试同一结果，不重跑 handler；错误槽在接纳时预留。旧链接终结时同时作废其入站回复端口，不把旧回复送到同节点的新连接。

每代与外部传输共享 `net_connections`、`net_listeners` 及 `net_buffer_bytes` 的总预算；节点帧含头不得超 `net_frame_bytes`（启用时至少 140）。每代预留八份帧长和有界错误槽的负载空间，普通发送出口至多八条、合计两份最大帧；一次就绪记录单独预留。I/O 不进入插件；控制输入的所有者只在排他 driver 的生命周期或运行回调里借用字节。控制器停止／故障复用[既有收尾顺序](plugin-abi.md)。外部玩家 TCP/KCP 不使用 peer 握手，受权源代转的内部玩家标志仍须目标 client-callable，不能借此取得 peer 控制权。完整的 Gate、Watchdog 业务流程不在这一层。
