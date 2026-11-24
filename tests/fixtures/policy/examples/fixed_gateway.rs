//! 仅选择编译期 codec；与默认薄库使用同一份 Gateway 实现。
#[abi_stable::export_root_module]
fn root() -> ace_server_sdk::native::ServiceModuleRef {
    ace_server_sdk::export::<ace_gateway::Gateway<ace_fixture_codec::FixedCodec>>()
}
