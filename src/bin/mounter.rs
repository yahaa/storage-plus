use anyhow::Result;
use clap::Parser;
use log::{debug, error, info, warn};
use nix::poll::{PollFd, PollFlags, poll};
use photo_plus::db::{Pool, establish_pool};
use photo_plus::repo::device_repo;
use photo_plus::logging::init_logging;
use std::fs;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use udev::{EventType, MonitorBuilder};

#[derive(Debug, Parser)]
#[command(author, version, about = "Udev device manager with SQLite tracking", long_about = None)]
struct Args {
    #[arg(long, default_value = "/mnt/storage_pool")]
    storage_root: PathBuf,
    #[arg(long, default_value = "/var/lib/photo-plus/devices.db")]
    db_path: PathBuf,
    #[arg(long, default_value_t = 5)]
    scan_interval_secs: u64,
    #[arg(long, default_value_t = false, help = "Run migrations and exit (for testing/deployment)")]
    migrate_only: bool,
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn fetch_uuid(devnode: &str) -> Option<String> {
    // Prefer udev / blkid call
    let out = Command::new("blkid")
        .arg("-s")
        .arg("UUID")
        .arg("-o")
        .arg("value")
        .arg(devnode)
        .output()
        .ok()?;
    if out.status.success() {
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !s.is_empty() {
            return Some(s);
        }
    }
    None
}

fn upsert_add(pool: &Pool, devnode: &str) -> Result<()> {
    if let Some(uuid) = fetch_uuid(devnode) {
        device_repo::upsert_device(pool, devnode, &uuid, now_epoch())?;
    }
    Ok(())
}

fn mark_removed(pool: &Pool, devnode: &str) -> Result<()> {
    // Attempt unmount if currently mounted & previously marked success.
    if is_mounted(devnode) {
        match Command::new("umount").arg(devnode).status() {
            Ok(st) if st.success() => info!("unmounted {}", devnode),
            Ok(_) => warn!("umount command failed for {}", devnode),
            Err(e) => error!("umount error for {}: {}", devnode, e),
        }
    }
    device_repo::mark_removed(pool, devnode, now_epoch())
}

fn is_mounted(devnode: &str) -> bool {
    if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
        return mounts.lines().any(|l| l.starts_with(devnode));
    }
    false
}

fn pick_mount_path(storage_root: &Path, uuid: Option<String>) -> Result<PathBuf> {
    if let Some(u) = uuid {
        return Ok(storage_root.join(u));
    }
    // Fallback: diskN based on current count
    fs::create_dir_all(storage_root)?;
    let mut n = 1u32;
    loop {
        let p = storage_root.join(format!("disk{}", n));
        if !p.exists() {
            return Ok(p);
        }
        n += 1;
    }
}

fn mount_device(devnode: &str, target: &Path) -> Result<bool> {
    fs::create_dir_all(target)?;
    let status = Command::new("mount").arg(devnode).arg(target).status()?;
    Ok(status.success())
}

fn process_pending(pool: &Pool, storage_root: &Path) -> Result<()> {
    let rows = device_repo::list_joined_active(pool)?;
    for row in rows {
        let uuid_val = match row.uuid {
            Some(u) if !u.is_empty() => u,
            _ => { warn!("device {} missing UUID, skipping", row.devnode); continue; }
        };
        if row.mount_success == 1 && is_mounted(&row.devnode) {
            info!("{} already mounted {}", row.devnode, row.mount_path.as_deref().unwrap_or("?"));
            continue;
        }
        let target = if let Some(mp) = row.mount_path.clone().map(PathBuf::from) { mp } else { pick_mount_path(storage_root, Some(uuid_val.clone()))? };
        if is_mounted(&row.devnode) {
            device_repo::mark_mounted_existing(pool, &row.devnode, &target.to_string_lossy(), &uuid_val)?;
            continue;
        }
        match mount_device(&row.devnode, &target) {
            Ok(true) => {
                info!("mounted {} at {:?}", row.devnode, target);
                device_repo::update_mount_result(pool, &row.devnode, &target.to_string_lossy(), &uuid_val)?;
            }
            Ok(false) => error!("mount command failed for {}", row.devnode),
            Err(e) => error!("error mounting {}: {}", row.devnode, e),
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    fs::create_dir_all(&args.storage_root)?;

    if let Some(parent) = args.db_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let pool = establish_pool(&args.db_path)?;
    info!(
        "starting udev monitor + scheduler storage_root={:?} db={:?}",
        args.storage_root, args.db_path
    );

    if args.migrate_only {
        info!("migrations applied, exiting due to --migrate-only flag");
        return Ok(())
    }

    // Spawn scheduler thread
    let pool_clone = pool.clone();
    let storage_root_clone = args.storage_root.clone();
    let interval = args.scan_interval_secs;
    thread::spawn(move || {
        loop {
            if let Err(e) = process_pending(&pool_clone, &storage_root_clone) {
                error!("process_pending error: {e}");
            } else {
                debug!("scheduled scan complete");
            }
            thread::sleep(Duration::from_secs(interval));
        }
    });

    // udev monitoring (blocking loop)
    let monitor = MonitorBuilder::new()?.match_subsystem("block")?.listen()?;
    let borrowed = monitor.as_fd();
    let mut fds = [PollFd::new(&borrowed, PollFlags::POLLIN)];
    loop {
        if let Err(e) = poll(&mut fds, -1) {
            error!("poll error: {:?}", e);
            continue;
        }
        for ev in monitor.iter() {
            if let Some(devnode) = ev.devnode() {
                let devpath = devnode.to_string_lossy().to_string();
                match ev.event_type() {
                    EventType::Add => {
                        if let Err(e) = upsert_add(&pool, &devpath) {
                            error!("db upsert error {}: {}", devpath, e);
                        } else {
                            info!("device add {} recorded", devpath);
                        }
                    }
                    EventType::Remove => {
                        if let Err(e) = mark_removed(&pool, &devpath) {
                            error!("db remove mark error {}: {}", devpath, e);
                        } else {
                            info!("device removed {}", devpath);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
