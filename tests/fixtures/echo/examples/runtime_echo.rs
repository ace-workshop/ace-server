//! 通过正式服务根表装载的 echo 夹具；与旧布局检查入口独立构建。
#[abi_stable::export_root_module]
fn root() -> ace_fixture_model::runtime::ServiceModuleRef {
    ace_fixture_model::runtime::export::<false>()
}
