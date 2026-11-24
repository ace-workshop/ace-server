# ace-server 开发约束

先读 [README](README.md)、[需求](docs/requirements.md)与[架构](docs/architecture.md)。README 明示当前可用范围；不能把规划中的接口当作已有功能。只改本仓；未经授权不改协议消费者、序列化仓或组合仓引用。

- host 执行通用路由、资源限额和生命周期，不依赖具体 codec、角色协议或服务实现。公开帧库不依赖 host、SDK、Tokio、Bevy 或 native 插件布局库。默认 codec 和角色装配位于调用方／服务侧。
- 新成员继承根 workspace 的 edition、版本和 lint；使用 rustfmt。导出 API 用中文文档说明用途及错误，库用结构化错误。不要在正常输入路径 panic 或用占位成功代替实现。
- ABI、wire、稳定布局、方法摘要、服务地址语义及资源收尾顺序是不同契约。变更先核对兼容边界与当次授权，关联更新文档和必要测试。unsafe 仅在需要的边界使用并逐处写 `// SAFETY:`；公开 unsafe 函数写 `# Safety`。不得让普通 Rust 对象、Future、Waker 或 panic 跨 ABI，不跨 await 持状态锁。
- 正式外部 Git 依赖固定完整公开提交，各自的锁在各自仓内维护。显式本地联调来源不是发布来源；不要提交个人路径、凭据、构建产物或内部材料。

完整验证在仓根运行 `sh tools/check.sh`：包含以下标准门、禁默认依赖边界、实际装载与回环。独立性能实测用 `python3 -B tools/measure.py --results <已有目录>`，结果不设 CI 阈值。标准 Cargo 门：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
cargo build --workspace --release --locked
```

只在有相应实现时计入功能验收：实际装载独立动态库、运行 TCP/KCP 和多节点测试，区分本机、目标 Linux 和 CI 的结果。提交用英文 Conventional Commits，遵守当次授权与适用协作审核规则。
