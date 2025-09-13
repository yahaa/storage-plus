use anyhow::Result;
use clap::Parser;
use log::info;
use storage_plus::{
    db::establish_pool, logging::init_logging, mounter::Mounter, repo::device_repo::DeviceRepo,
};
use std::{fs, path::PathBuf, sync::Arc};

#[derive(Debug, Parser)]
#[command(author, version, about = "Udev device manager with SQLite tracking", long_about = None)]
struct Args {
    #[arg(long, default_value = "/mnt/storage_pool")]
    storage_root: PathBuf,
    #[arg(long, default_value = "/var/lib/storage-plus/storage-plus.db")]
    db_path: PathBuf,
    #[arg(long, default_value_t = 5)]
    scan_interval_secs: u64,
    #[arg(
        long,
        default_value_t = false,
        help = "Run migrations and exit (for testing/deployment)"
    )]
    migrate_only: bool,
}

fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    fs::create_dir_all(&args.storage_root)?;
    if let Some(parent) = args.db_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pool = establish_pool(&args.db_path)?;
    let repo = DeviceRepo::new(pool.clone());
    info!(
        "starting udev monitor + scheduler storage_root={:?} db={:?}",
        args.storage_root, args.db_path
    );
    if args.migrate_only {
        info!("migrations applied, exiting due to --migrate-only flag");
        return Ok(());
    }
    let mounter = Arc::new(Mounter::new(
        repo,
        args.storage_root.clone(),
        args.scan_interval_secs,
    ));
    mounter.start_scheduler();
    mounter.run_udev_loop()
}
