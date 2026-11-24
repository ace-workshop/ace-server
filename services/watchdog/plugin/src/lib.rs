//! Watchdog 默认 ace 装配；业务实现不在导出层。
#[abi_stable::export_root_module]
fn root() -> ace_server_sdk::native::ServiceModuleRef {
    ace_server_sdk::export::<ace_watchdog::Watchdog<ace_server_sdk::AceCodec>>()
}
