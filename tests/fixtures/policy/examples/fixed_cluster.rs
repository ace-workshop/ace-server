//! 仍是同一个 Cluster 实现；这个薄入口只改变编译期 codec。
#[abi_stable::export_root_module]
fn root() -> ace_server_sdk::native::ServiceModuleRef {
    ace_server_sdk::export::<ace_cluster::Cluster<ace_fixture_codec::FixedCodec>>()
}
