//! 独立编译、装载的 caller 测试服务。
#[abi_stable::export_root_module]
fn root() -> ace_fixture_interface::FixtureRef {
    ace_fixture_model::module(env!("CARGO_PKG_NAME"))
}
