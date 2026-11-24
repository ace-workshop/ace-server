//! 显式区分只读检查、首次身份配置与本地运行。
use ace_server_host::{config::Manifest, loader::Implementations};
use std::{
    env,
    error::Error,
    path::{Path, PathBuf},
};
fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    if let [mode, node, dir] = args.as_slice()
        && mode == "provision-new"
    {
        ace_server_host::identity::provision_new(
            Path::new(dir),
            node.to_str().ok_or("节点号不是 UTF-8")?.parse()?,
        )?;
        return Ok(());
    }
    if let [mode, file, dir, ids] = args.as_slice()
        && mode == "run-manifest"
    {
        let m = Manifest::read(Path::new(file))?;
        // SAFETY: 专用进程的主线程尚未创建其它线程，管理员明确选择遵守信号约束的受信库。
        unsafe { ace_server_host::runtime::run_manifest(&m, Path::new(dir), Path::new(ids)) }?;
        return Ok(());
    }
    let (config, paths) = match args.as_slice() {
        [mode, file, dir] if mode == "inspect-manifest" => {
            let cfg = Manifest::read(Path::new(file))?;
            let paths = cfg.library_paths(Path::new(dir));
            (Some(cfg), paths)
        }
        [mode, paths @ ..] if mode == "inspect" && !paths.is_empty() => {
            (None, paths.iter().map(PathBuf::from).collect())
        }
        _ => return Err(
            "用法：ace-server-host inspect <可信动态库>... 或 inspect-manifest <清单> <可信库目录>；provision-new <新节点号> <新身份目录>；run-manifest <清单> <可信库目录> <既有身份目录>"
                .into(),
        ),
    };
    // SAFETY: 操作者须显式提供受信配置与库目录，不能把网络输入作为装载路径。
    let registry = unsafe { Implementations::load_startup(&paths) }?;
    if let Some(c) = config {
        c.check(&registry)?;
        println!("已检查节点 {} 的静态启动配置", c.node);
    }
    for d in registry.descriptors() {
        println!(
            "{}: frame={} roles={} methods={}",
            d.implementation,
            d.frame_rev,
            d.roles.len(),
            d.methods.len()
        )
    }
    println!("库已装载；未启动实例或网络端口");
    Ok(())
}
