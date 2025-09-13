use std::{fs, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use log::info;
use storage_plus::{
    db::establish_pool,
    logging::init_logging,
    repo::device_repo::new_device_repo,
    repo::file_repo::new_file_repo,
    server::{self, ServerConfig},
};

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Storage root directory for uploaded files
    #[arg(long, default_value = "/mnt/storage_pool")]
    storage_root: PathBuf,
    /// SQLite db file path
    #[arg(long, default_value = "/var/lib/storage-plus/storage-plus.db")]
    db_path: PathBuf,
    /// Bind address
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
    /// Device UUID cache TTL in seconds
    #[arg(long, default_value_t = 30)]
    device_cache_ttl_secs: u64,
    /// Run migrations and exit (for testing/deployment)
    #[arg(long, default_value_t = false)]
    migrate_only: bool,
}

#[actix_web::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    fs::create_dir_all(&args.storage_root)?;
    if let Some(parent) = args.db_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pool = establish_pool(&args.db_path)?;
    let file_repo = new_file_repo(pool.clone());
    let device_repo = new_device_repo(pool);

    if args.migrate_only {
        info!("migrations applied, exiting due to --migrate-only flag");
        return Ok(());
    }

    let cfg = ServerConfig {
        storage_root: args.storage_root.clone(),
        addr: args.addr.clone(),
        device_cache_ttl_secs: args.device_cache_ttl_secs,
    };
    server::run(cfg, file_repo, device_repo).await
}
