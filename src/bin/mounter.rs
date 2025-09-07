use anyhow::Result;
use clap::Parser;
use diesel::prelude::*;
use log::{debug, error, info, warn};
use nix::poll::{PollFd, PollFlags, poll};
use photo_plus::db::{Pool, establish_pool};
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
    let uuid = fetch_uuid(devnode);
    if uuid.is_none() {
        // Skip insert/update if we couldn't obtain a UUID yet.
        // Returning Ok so caller can retry later when the device settles.
        return Ok(());
    }

    let uuid_val = uuid.unwrap();
    use photo_plus::schema::devices;
    let mut conn = pool.get()?;
    // emulate upsert
    conn.immediate_transaction(|c| {
        // Try update by uuid; if no row updated insert new
        let updated = diesel::update(devices::table.filter(devices::uuid.eq(&uuid_val)))
            .set((
                devices::devnode.eq(devnode),
                devices::removed.eq(0),
                devices::last_seen.eq(now_epoch()),
            ))
            .execute(c)?;
        if updated == 0 {
            diesel::insert_into(devices::table)
                .values((
                    devices::devnode.eq(devnode),
                    devices::uuid.eq(Some(uuid_val.clone())),
                    devices::removed.eq(0),
                    devices::joined.eq(0),
                    devices::mount_success.eq(0),
                    devices::mount_path.eq::<Option<String>>(None),
                    devices::last_seen.eq(now_epoch()),
                ))
                .execute(c)?;
        }
        Ok::<(), diesel::result::Error>(())
    })?;
    Ok(())
}

fn mark_removed(pool: &Pool, devnode: &str) -> Result<()> {
    // Query existing state to decide if we should attempt umount first.
    use photo_plus::schema::devices;
    let mut conn = pool.get()?;
    if let Ok(existing) = devices::table
        .filter(devices::devnode.eq(devnode))
        .filter(devices::joined.eq(1))
        .select((devices::mount_path, devices::mount_success))
        .first::<(Option<String>, i32)>(&mut conn)
    {
        let (mount_path, mount_success) = existing;
        if mount_success == 1 && is_mounted(devnode) {
            if let Some(mp) = mount_path {
                match Command::new("umount").arg(&mp).status() {
                    Ok(st) if st.success() => info!("unmounted {} from {}", devnode, mp),
                    Ok(_) => warn!("umount command failed for {} (path {})", devnode, mp),
                    Err(e) => error!("umount error for {} (path {}): {}", devnode, mp, e),
                }
            } else {
                match Command::new("umount").arg(devnode).status() {
                    Ok(st) if st.success() => info!("unmounted {}", devnode),
                    Ok(_) => warn!("umount command failed for {}", devnode),
                    Err(e) => error!("umount error for {}: {}", devnode, e),
                }
            }
        }
    }
    diesel::update(devices::table.filter(devices::devnode.eq(devnode)))
        .set((
            devices::removed.eq(1),
            devices::mount_success.eq(0),
            devices::last_seen.eq(now_epoch()),
        ))
        .execute(&mut conn)?;
    Ok(())
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
    use photo_plus::schema::devices;
    let mut conn = pool.get()?;
    let rows = devices::table
        .filter(devices::removed.eq(0))
        .filter(devices::joined.eq(1))
        .select((
            devices::devnode,
            devices::uuid,
            devices::mount_success,
            devices::mount_path,
        ))
        .load::<(String, Option<String>, i32, Option<String>)>(&mut conn)?;
    for (devnode, uuid_opt, mount_success, mount_path_opt) in rows {
        let uuid_val = match uuid_opt {
            Some(u) if !u.is_empty() => u,
            _ => {
                // According to new contract UUID must exist; log and skip if not to avoid inconsistency.
                warn!("device {} missing UUID in DB, skipping", devnode);
                continue;
            }
        };
        if mount_success == 1 && is_mounted(&devnode) {
            info!(
                "{} already mounted {}, skipping",
                devnode,
                mount_path_opt.as_deref().unwrap_or("?")
            );
            continue;
        }

        let target = if let Some(mp) = mount_path_opt.map(PathBuf::from) {
            mp
        } else {
            // UUID guaranteed non-empty -> deterministic path.
            pick_mount_path(storage_root, Some(uuid_val.clone()))?
        };

        if is_mounted(&devnode) {
            diesel::update(devices::table.filter(devices::devnode.eq(&devnode)))
                .set((
                    devices::mount_success.eq(1),
                    devices::mount_path.eq(Some(target.to_string_lossy().to_string())),
                    devices::uuid.eq(Some(uuid_val.clone())),
                ))
                .execute(&mut conn)?;
            continue;
        }

        match mount_device(&devnode, &target) {
            Ok(true) => {
                info!("mounted {} at {:?}", devnode, target);
                diesel::update(devices::table.filter(devices::devnode.eq(&devnode)))
                    .set((
                        devices::mount_success.eq(1),
                        devices::mount_path.eq(Some(target.to_string_lossy().to_string())),
                        devices::uuid.eq(Some(uuid_val.clone())),
                    ))
                    .execute(&mut conn)?;
            }
            Ok(false) => error!("mount command failed for {}", devnode),
            Err(e) => error!("error mounting {}: {}", devnode, e),
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
