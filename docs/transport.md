# 原生 TCP 与 KCP 传输

服务用 `Context::net_listen(transport, "IP:port")` 申请自己的监听。接纳只签发句柄；绑定成功或失败另由 `ListenReady` / `ListenFailed` 报告。端口 0 表示由系统选取，使用成功事件中的实际端点。Prepare/Start 可用所属监听结果推进同一动作；全部清单动作完成且 owner 已发布之前，不开放玩家传输握手或交付 `Accepted` / `Data`（TCP 内核可能已经接收连接到 backlog）。网络任务只做 I/O，不进入插件；在**同一个 Tokio runtime** 的唯一 Actor driver 上，事件的字节只借给这一次回调。公开表、帧头与方法摘要均没有因传输而另起版本。

- **TCP** 双方先交换固定公共 `Hello`。之后直接使用公共帧头的 `body_len`，没有第二层长度字：先读 104 字节，只有请求再读 12 字节扩展。先核对域、字段与总长再申请 body；半包保留，粘包逐帧交付。帧间 FIN 是正常关闭；已开始读取但不完整的帧遇到 FIN 是 Protocol，实际 I/O 失败是 Unreachable。Linux/macOS 上，即使业务读取暂停、写队列空闲，仍独立观察对端读方向终态。普通字节可读并不表示关闭；若字节留在内核，只观察半关闭通知，不为它们额外取帧。
- **KCP** 的 UDP 外层先做 `Hello(nonce) → Offer → Confirm → Ready`。nonce 和 token 用系统随机源，epoch 取节点启动身份；同源、同 nonce 重试沿用候选，不延长期限或替换现有会话。每监听最多 32 个未确认候选，3 秒到期；只有通过 Confirm 且资源足够才创建消息核心。每份 Data 都核对实际端点、epoch、token、conv；不支持 NAT 迁移。建立后公共 `Hello` 和每个完整应用帧各占**一条 KCP 消息**，并非一个 UDP 包就是一帧。公共 Hello 超时 3 秒。
- KCP 关闭撤销本地 token；没有 FIN/半关闭。只有通过本会话完整段预检的入站报文才更新 idle；本地输出、重传、重复握手和错域/越界报文不会续命。框架握手和应用方法身份都不是登录、认证或加密。

## 预算与回收

`[budgets]` 中 `net_listeners`（默认 16）和 `net_connections`（64）是节点总槽数；`net_buffer_bytes`（64 MiB）是网络会话受管负载的总预算，独立于 RPC/pending 和业务邮箱。`net_frame_bytes` 是**含头总长**，默认 149352，范围 116 B 至 4 MiB；KCP 的有效上限还不能超过 127 片。`net_idle_ms`（默认 30000）仅限 KCP，非零且不超过一天。固定段元数据、每监听的一份 UDP 读缓冲及内核 socket 缓冲不按应用 body 收费，不把预算解释为 RSS。

每次接纳同时预留连接、Accepted、Closed、初始读取/一份待交付帧和本地写容量；任一不足不产生 Accepted。出口每连接最多 8 条且总字节不超过该连接两份最大帧，含正在写的队首；额外的 KCP 收发窗与整包出口另算。`net_send` 只承诺**本地接纳**，临时无空间返回 Busy，后续 Writable 为合并提示，不保证下次一定成功。TCP 暂停读取；KCP 在事件槽或任务容量不足时不 `recv` 应用消息，继续处理有限输入、定时更新和其它会话。已经准入的一次读取仍可完成。每轮 UDP 至多接收 16 份，轮转各会话的有限输出；系统 WouldBlock 只丢本地整包，由协议重传。

存活 Running 实例的同一代连接必须先接纳 `Accepted`，再接纳 `Closed`。即使 socket 在首次通知前或回调中已经关闭，未消费的 `Accepted` 凭据仍占原槽，暂拒可以重试；物理关闭丢弃未分派的 Data，不滞留 socket 等待顺序。一旦 `Accepted` 已消费，`Closed` 不依赖普通任务容量或 Data 确认。停止 accept/关闭 socket、回收核心及字节缓冲不等待业务清理成功；锁外回调尚在借用的帧保留其额度至返回。owner 确认清理完成后调用 `net_release_closed(conn)`，此前连接槽不归还；原槽未换代时重复归还无额外作用。换代、错 transport 或非 owner 不能操作新连接。正常停止只进有限 Cleanup；Faulted 不进业务清理，两者均在 Cancel 后回收残余再 destroy。这份外部传输账本不负责 Gate 绑定、Watchdog 准入或成员协议；节点 TCP 的引导、授权与数据出口见[受管节点](cluster.md)。

## KCP 的受支持域

[共享适配器](../kernel/transport/src/session.rs) 固定使用发布的 `kcp=0.6.0` 消息模式，未 fork，不关闭溢出检查。KCP MTU 1200，段头 24，MSS 1176；完整非空帧最多 `127 × 1176 = 149352` B，下一字节拒绝。UDP payload 还含 28 B 外层，最多 **1228 B**。先验整包（最多 50 段）的长度、conv、命令、窗口、时间、SN/UNA、分片关系和配置尺寸，才允许 `input`；`send` 前按 `wait_snd + ceil(frame_len/MSS)` 保守预留最多 256 个 MSS 槽。发送窗 32、接收窗 128、远端通告不超过 128；`rcv_queue` 和 `rcv_buf` 各受一窗约束。只观察核心输出的 SN 高水位来限制确认域，不自行确认、重排或重传业务数据。

会话时钟由建会话起的单调毫秒给出，范围 `0..86400000`（右开）；双方不比较跨机绝对时间。20 ms interval，保留拥塞窗，快速重传阈值 2；最大 12 次发送。计算时包括第 12 次退避的 `rto += rto/2`（初值至多 60000 ms）、120000 ms 窗口探测及平滑 RTT 中的乘法。每方向本代最多 100000000 个发送序号、累计输入最多 200000000 段，保留窗口与重传计数余量。超出域、idle 或死链终结旧代，不能截断/归零旧核心继续用。`check` 返回的是到下次更新的**时长**。

最多累计 96 个输入段才需强制排空 ACK，不因一次 `update` 就清账。超额前 `flush_ack` 后再 `flush`，把核心内最后最多一个 MTU 也送至有界整包出口；成功才重新累计。同步输出最多 32 个完整报文，满即按整报丢失计数，不部分写或回滚。

**复验**：`cargo test -p ace-transport --locked` 及加 `--release` 运行同一生产实现的算术/消息/ACK 边界；`sh tools/check.sh` 另行构建真实 owner 动态库与独立客户端，在回环验证监听、帧、输出、有限损耗/乱序/重复、跨会话进展及正常/故障收尾。默认 Gate 另由两个独立 CLI 同时走 Watchdog 准入、自动回复与 Close；这部分可单独执行 `python3 -B tools/player-smoke.py target/release`。构建前置与手动入口见 [README](../README.md)。有限测试未承诺网络故障下无限可靠、任意客户端兼容或生产鉴权。
