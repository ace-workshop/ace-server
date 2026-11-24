# 插件和本地消息边界

首版插件 ABI 已冻结：下述表布局、消息所有权、生命周期及额度语义是宿主与服务共同遵守的契约。冻结不意味着兼容早期候选库，也不表示网络和调度行为已全部实现。表和消息见 `kernel/abi/src/native.rs`，SDK wrapper 见 `sdk/src/service.rs`，本地所有权见 `kernel/local/src/lib.rs`。

## 装载：版本在入口和表头，不在 Rust 类型名中

abi_stable 的常驻根模块提供唯一服务入口 `ace_service_v1`。宿主逐文件核验根的递归布局，再核对 `ServiceApi`、`Descriptor` 的 `abi_size/abi_version` 及帧修订、方法和角色声明；SDK 在 create 时核对 `HostApi` 表头。`ABI_VERSION = 1` 不是跳过布局检查的理由；**旧候选库不能与新表混装，宿主和所有插件须一起重建**。角色修订、codec 三元组、wire 摘要与本机表的版本分别核对。没有运行中装入、热替换或卸载。

`HostApi` 的 `startup_peer_check` 是一项独立的可选能力，不是 `HostOp` 新编号。`HostApi` 是根递归布局中的普通结构体，不是可任意追加字段的 prefix：缺少该字段的旧库／旧宿主不能与这版混装。布局相符后，SDK 在调用前再检查能力表存在且 `version == STARTUP_PEER_CHECK_V1`；缺席返回 NotReady，错版返回 Protocol，不调用不支持的入口。使用它的服务必须让本次启动失败，不能退回 `Select` 或普通订阅。`Select` 的目标阶段、返回表示以及普通消息权限不变。

`Context::check_startup_peer` 仅在本机清单发布前的 Prepare／Start 有效。Prepare 只查调用实例显式授权的已创建依赖，Start 只查 Running 实例。调用者给出已解析的精确地址、所需角色、双向方法键及必要条件。宿主对照调用者的 `requires`、该地址实际选定的角色和双方描述表，检查角色修订、codec、普通服务方法的方向与完整 wire 契约；可同时要求目标具备玩家入口授权、目标已订阅**当前实例**。只返回成功或状态错误，不产生地址、token、消息或授权，不提供订阅或成员查询接口。

`ServiceApi` 只有 `create`、`on_message`、`on_control`、`destroy` 四个同步回调。`CreateArgs` 交给 SDK 的有实例绑定上下文、专用普通唤醒令牌、服务地址、节点启动身份、通告端点、服务配置和 `SdkLimits`（入站任务数、受管负载字节）。宿主只持有不可克隆、可迁移、不可共享的排他句柄。Future/Waker 和普通 Rust 服务状态不跨 ABI。普通回调 panic 使实例 Faulted；取消或析构 panic 则 fail-stop。受信库仍可能阻塞、abort 或违反 unsafe 契约，宿主不是沙箱。

## 哪些回调可进实例

| 阶段 | 允许的推进 |
| --- | --- |
| Prepare / Start 返回 Pending | 宿主按所属动作、资源 owner 和代次投递 `LifecycleEvent`（如监听成功／失败、peer 控制、timer、订阅或完成通知）；SDK 可用 `LifecycleReady(action)` 通知专槽，再以同动作、非零预算 `LifecyclePoll` 取得结果。事件接纳成功本身不结束初始化。禁止普通消息、Data/Accepted 和普通 Poll。 |
| Start 成功，尚未发布 | 仍禁止业务。只有宿主管理方认领后才调用 `Instance::publish`。创建 ticket 的认领／放弃和 alias 登记是管理运行时的责任。 |
| Running | 普通消息、事件和有界 Poll。`Context::wake()` 只用签发的普通令牌通知，不栈上重入；它不是生命周期完成。 |
| Stopping | `begin_stop` 关闭普通回调。`Cleanup` 只向 SDK 的 `on_cleanup` 交付单条 Closed、ResourceGone 或完成终结记录，不调度普通 handler Future。首次清理返回 `Faulted`（包括 wrapper 捕获的 panic）即停止交付剩余事件，由原 driver 按 Faulted 收尾；节点关闭遇到该故障不会报告正常退出。 |
| Faulted / 已取消 | Faulted 不再投递清理事件或普通回调，只走专用 Cancel。正常 Stop 或专用 Cancel 均至多执行一次插件取消；此时宿主可释放残余完成槽等额度，最后 drop 排他句柄，在原库 destroy 一次。遗漏显式取消时 Drop 仍兜底。 |

本机运行时在同一账本中关闭准入与 wake，并由独立停止通知唤醒唯一 driver。取消并等待所属 socket 任务退出、撤销会话后，正常停止只对账已保留的有限清理事件，Faulted 跳过它们；随后终结 pending、取消 timer，专用 Cancel 丢弃任务和 Waker，释放宿主残留，最后在原库 destroy。属于实际持有者的字节或稳定对象凭证不因为发送方已停止而提前回收。插件取消/析构再次 panic 是进程 fail-stop，不是一个可恢复的 RPC 错误。节点的[配置顺序与进程硬期限](architecture.md#部署与有界关闭)围绕这条实例收尾路径执行，不改变 native 表。

### 控制事件何时算交付

宿主在登记表中选定事件，**不持宿主锁**调用唯一实例，再结算原记录。`ResourceGone` 在接纳或取消前仍占订阅槽；`CreateReady` 被接纳才标为已通知，不代替认领；`Completion` 被接纳仍占完成槽，直到 `CompletionConsumed`（可在回调中同步归还）。暂拒须在取走对象或接纳字节前返回 `Busy`。未转移的结果留在原槽，等待本实例推进或唤醒再尝试；不重跑处理请求的 handler，也不靠循环试到成功。事件的其它非成功返回同样不是消费凭据；Faulted 不重投。无论普通 `Completion` 还是 `Cleanup` 中的完成回调首次返回 Faulted，仍在投递槽中的回复都由原 driver 保留，专用 Cancel 返回后才在账本锁外释放，随后 destroy。已取走的对象按实际持有者的生命周期析构，不从原槽重复释放。若在途记录被显式取消、消费或认领，回调返回时不会复活它。接收者自己停止时只做有限清理，不等它最终确认。

## 两个方向的消息与权限

- `HostApi::invoke(context, HostOp, slot)` 的即时结果是本次操作的接纳或错误。`Request` 返回的 pending、`ServiceCreate` 返回的 ticket、`CreateClaim` 取得的地址、`CompletionConsumed` 归还的完成槽不能互换。只读装载检查用的空宿主表对合法操作返回 NotReady；本机运行时使用另一张真实表，实现通用 TCP/KCP 监听和收发，并按绑定实例授予[节点控制原语](cluster.md)。`peer_admit` 的 Busy（含等待持久化）不是已准入，DataReady 另按本代双端状态通知。
- 方法声明区分收／发、玩家可调用、节点控制准入阶段、codec、请求／响应摘要、可选本地布局和各表示的成功响应上限。`Select` 不转移负载。请求带非零相对期限与专用 reply token（由宿主投递给被调用者）；单向方法响应摘要为零，无期限、回复 token 或响应预留。
- `Reply::Success(Body)` 和 `Reply::Error(Status)` 共用同一 `HostOp::Reply` 和相应 `Event::Completion`。**空字节成功不等于无体失败**；错误不能带本地槽或字节额度凭证。可传错误为 Busy、TooSmall、Timeout、Stopped、Unreachable、UnknownMethod、Protocol、Panicked、Faulted、NotFound；`Pending`、`NotReady`、`Denied` 与未知码不能充作 RPC 回复。提交 `Error(Busy)` 与提交本身暂时返回 Busy 是两件事。响应上限取决于所选响应表示，而不是请求表示。
- 玩家入口只在受信的 native／内部消息中用 `Entry::Player { push_session }` 标记，请求和回复均为 Bytes。发送宿主按**绑定的调用上下文**核对 `player_entry` 授权并确定 source；目标在业务解码前拒绝未标为 client-callable 的方法。可选非零推送令牌由源 Gate 持有映射；目标可向该 source 调用角色推送方法，由 Gate 复核连接代次及当前绑定。它不是客户端自报的 session、RPC reply token，也不授予 `NetSend` 权限。公网帧不提供可信来源、入口标记或推送令牌。
- `IngressGrants` 按配置实例授权，不从角色名推导。同一节点只配置一个 `peer_control` owner。`PeerEvent::Control` 带宿主判定的连接阶段、实际方法和字节；loader 对照 owner 能力、方法方向、完整 wire 和准入阶段后才派发。`Connection` 携带 transport 和资源代次；`Context::net_listen/net_send/net_close/net_release_closed` 使用同一冻结表。Closed 以本代连接为清理凭据，未消费仍占连接槽；只许 owner 归还。实例/资源存活、owner、代次和权限仍须在宿主账本中裁决，不能信任插件自报或从网络 body 推导。

## 逐方向的所有权选择

1. 两端先比较方法、codec/修订/选项，以及**两个** wire 摘要。不兼容返回 Protocol，不试解码。
2. 仅同进程、本地方向声明存在、语义标识一致且真实布局双向兼容时，使用 LocalOwned。线路传输只能选 Bytes。请求与响应分别选择，raw 网关桥只接收字节响应。
3. Bytes 切片只在同步回调内有效。LocalOwned 可通过唯一可变槽交接；拒绝前不取走，取走后发生 panic 也不因错误码再释放一次。接收方只能在持有对象时按验证匹配的类型借用。不能对任意普通 Rust 类型使用此路径。
4. DTO 本体及递归拥有的容量按表示计费，固定队列/信封开销另算。额度凭证随对象移动，先在原模块析构值，再归还宿主凭证。成功字节负载若移交给任务，`charge_token` 随之持有，和完成槽分别归还。本地及受管端口的非空 Bytes 输入、向本地 caller 交付的非空 Bytes 回复，都在请求／发送接纳时备好独立凭证。不为本地对象、空字节或无体错误激活字节 loan；拒绝整组预留不消耗未签发的 RPC 编号。没有端到端零拷贝承诺；测试中的字符串与向量在构造和转换时可分配。

## 服务端编写：一个方法，两方向各自选择

`Service` 仍可直接处理四个 native 回调。普通异步服务实现 `ActorService` 并导出 `export::<Actor<S>>()`：实例状态用 `State<S>::with` 短同步访问，不返回 guard，不把 `&mut S` 留给 Future。`Incoming::source` / `entry` 是受信信封元数据；用 `Incoming::handle` 解包、await 业务并自动 reply，单向使用 `receive`。`Actor` 在取输入前检查任务槽；`Handler` 必须拥有所有权、`Send + 'static`。每个 SDK 任务只在自己的就绪记录出队后才 poll，重复 wake 可合并，预算计数实际 `Future::poll` 次数而非聚合流的一次 drain。

例如启用示例服务侧 `local` 表示后：

```rust,ignore
const ECHO: RpcMethod<Echo, EchoReply> = RpcMethod::call(echo::<AceCodec>(), Direction::Both)
    .local_input().local_output(64 * 1024);
// 在 S::DESCRIPTOR 的 methods 中使用 ECHO.descriptor()。接收：
msg.handle(ECHO, |q| async move { Ok(EchoReply { data: q.data.clone() }) });
// 发送：第一个 ? 是即时接纳，await? 是唯一终态。两次之间不占用 mailbox。
let reply = ctx.call(ECHO, destination, request, 1_000, 4_096)?.await?;
```

`Context::call`（同步接纳与返回的 Future）和 typed `send` 的 Rust 错误为 `TypedRpcError`：`Codec(CodecError)` 保留本库的具体原因，如预发送编码失败、成功响应的本机解码失败；`Status(Status)` 表示稳定状态，包括响应预留 `TooSmall`。`Incoming::handle` 的业务 Future 返回 `Result<R, TypedRpcError>`，可用 `?` 继续传播，也可用 `Err(Status::BUSY.into())` 返回合法业务终态。自动回复只在本节点记录 codec 错误枚举和方法键，对端收到无体 `Protocol`；不提交部分编码输出，也不让错误对象跨库。raw API 仍只返回稳定状态。

`RpcMethod::send` / `Context::send` 是 typed 单向路径。`Context::raw_call` / `raw_send` 转发已有字节，不把 raw 转发变成 codec 探测。`Managed<T>` 与 `ManagedBytes` 可跨 await 持有负载；框架凭据跟随值而不是完成槽，`FromLocal` 的普通 DTO 转换可能复制字段。drop 已提交调用不发远端 cancel；原期限、自然回复或停止仍负责唯一终结。`create_service(...)?` 返回的 Future 如果尚未向调用方交付地址，drop 会作废 ticket；不要把管理结果就绪当作已发布。有 `manage` 授权的调用者可用 `ctx.create_service("test.echo", b"", Some("child"))?.await?` 认领地址；`ctx.command(HostOp::ServiceStop(child.bits())).status` 是停止命令的接纳状态，不是已经 destroy 的证明。管理线程可用 `Node::stop` 等待终态。详见[实库使用示例](../tests/fixtures/model/src/runtime.rs)。

`sh tools/smoke-abi.sh` 重建并实际装载薄服务和测试策略库，检查错版本、受控事件推动 Pending、停止时不恢复普通 Future、取消与最终析构的分界。另用独立 caller/echo 库验证真实的逐方向 codec 次数、Busy/取走后 panic、发送者先停与对象额度。底层 ABI 试验的受控通知不等于网络验收。同一入口另外使用真实 runtime 及分别构建的 caller/echo 测试延迟 RPC、受管字节、额度与故障；有限入口测试不是 socket、成员准入或跨节点 RPC。同一脚本另起一个真实节点并在独立 owner 实库与纯客户端之间运行[双传输回环](transport.md)。
