use anyhow::Result;
use clap::Parser;
use log::{debug, error, info, warn};
use nix::poll::{PollFd, PollFlags, poll};
use photo_plus::logging::init_logging;
use rusqlite::{Connection, params};
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
    #[arg(long, default_value_t = 30)]
    scan_interval_secs: u64,
    /// Danger: allow automatic filesystem creation if missing (mkfs.ext4). Leave false to only log.
    #[arg(long, default_value_t = false)]
    auto_format: bool,
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn ensure_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        devnode TEXT UNIQUE NOT NULL,
        uuid TEXT,
        removed INTEGER NOT NULL DEFAULT 0,
        joined INTEGER NOT NULL DEFAULT 0,
        mount_success INTEGER NOT NULL DEFAULT 0,
        mount_path TEXT,
        last_seen INTEGER NOT NULL
    );"#,
    )?;
    Ok(())
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

fn upsert_add(conn: &Connection, devnode: &str) -> Result<()> {
    let uuid = fetch_uuid(devnode);
    conn.execute(r#"INSERT INTO devices (devnode, uuid, removed, joined, mount_success, mount_path, last_seen)
        VALUES (?1, ?2, 0, 0, 0, NULL, ?3)
        ON CONFLICT(devnode) DO UPDATE SET removed=0, last_seen=excluded.last_seen, uuid=COALESCE(excluded.uuid, devices.uuid)"#,
        params![devnode, uuid, now_epoch()])?;
    Ok(())
}

fn mark_removed(conn: &Connection, devnode: &str) -> Result<()> {
    // Query existing state to decide if we should attempt umount first.
    if let Ok(mut stmt) =
        conn.prepare("SELECT mount_path, mount_success FROM devices WHERE devnode=?1")
    {
        if let Ok(mut rows) = stmt.query(params![devnode]) {
            if let Ok(Some(row)) = rows.next() {
                let mount_path: Option<String> = row.get(0)?;
                let mount_success: i64 = row.get(1)?;
                if mount_success == 1 && is_mounted(devnode) {
                    // Try to unmount using mount path if present; fallback to devnode.
                    if let Some(ref mp) = mount_path {
                        match Command::new("umount").arg(mp).status() {
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
        }
    }
    conn.execute(
        "UPDATE devices SET removed=1, mount_success=0, last_seen=?2 WHERE devnode=?1",
        params![devnode, now_epoch()],
    )?;
    Ok(())
}

fn is_mounted(devnode: &str) -> bool {
    if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
        return mounts.lines().any(|l| l.starts_with(devnode));
    }
    false
}

fn ensure_filesystem(devnode: &str, auto_format: bool) -> Result<bool> {
    // Check if blkid returns a TYPE
    let out = Command::new("blkid")
        .arg("-s")
        .arg("TYPE")
        .arg("-o")
        .arg("value")
        .arg(devnode)
        .output()?;
    if out.status.success() && !String::from_utf8_lossy(&out.stdout).trim().is_empty() {
        return Ok(false); // Already has filesystem
    }
    if !auto_format {
        warn!("device {} has no filesystem; auto_format disabled", devnode);
        return Ok(false);
    }
    info!("creating ext4 filesystem on {}", devnode);
    let status = Command::new("mkfs.ext4").arg("-F").arg(devnode).status()?;
    if status.success() {
        Ok(true)
    } else {
        Err(anyhow::anyhow!("mkfs.ext4 failed"))
    }
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

fn process_pending(conn: &Connection, storage_root: &Path, auto_format: bool) -> Result<()> {
    let mut stmt = conn
        .prepare("SELECT devnode, uuid, mount_success, mount_path FROM devices WHERE removed=0")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, Option<String>>(3)?,
        ))
    })?;
    for row in rows.flatten() {
        let (devnode, uuid_opt, mount_success, mount_path_opt) = row;
        if mount_success == 1 && is_mounted(&devnode) {
            continue;
        }
        // ensure filesystem
        let _formatted = ensure_filesystem(&devnode, auto_format).unwrap_or_else(|e| {
            error!("fs ensure failed {}: {}", devnode, e);
            false
        });
        // refresh uuid if missing
        let uuid_final = uuid_opt.or_else(|| fetch_uuid(&devnode));
        let target = if let Some(mp) = mount_path_opt.map(PathBuf::from) {
            mp
        } else {
            pick_mount_path(storage_root, uuid_final.clone()).unwrap()
        };
        if is_mounted(&devnode) {
            // update DB with path
            conn.execute("UPDATE devices SET mount_success=1, mount_path=?2, uuid=COALESCE(?3, uuid) WHERE devnode=?1", params![devnode, target.to_string_lossy(), uuid_final])?;
            continue;
        }
        match mount_device(&devnode, &target) {
            Ok(true) => {
                info!("mounted {} at {:?}", devnode, target);
                conn.execute("UPDATE devices SET mount_success=1, mount_path=?2, uuid=COALESCE(?3, uuid) WHERE devnode=?1", params![devnode, target.to_string_lossy(), uuid_final])?;
            }
            Ok(false) => {
                error!("mount command failed for {}", devnode);
            }
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
    let conn = Connection::open(&args.db_path)?;
    ensure_db(&conn)?;
    info!(
        "starting udev monitor + scheduler storage_root={:?} db={:?}",
        args.storage_root, args.db_path
    );

    // Spawn scheduler thread
    let db_path_clone = args.db_path.clone();
    let storage_root_clone = args.storage_root.clone();
    let interval = args.scan_interval_secs;
    let auto_format = args.auto_format;
    thread::spawn(move || {
        loop {
            match Connection::open(&db_path_clone) {
                Ok(c) => {
                    if let Err(e) = ensure_db(&c) {
                        error!("ensure_db error: {e}");
                    }
                    if let Err(e) = process_pending(&c, &storage_root_clone, auto_format) {
                        error!("process_pending error: {e}");
                    } else {
                        debug!("scheduled scan complete");
                    }
                }
                Err(e) => error!("open db error: {e}"),
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
                if !devpath.starts_with("/dev/sd") {
                    continue;
                }
                match ev.event_type() {
                    EventType::Add => {
                        let c = Connection::open(&args.db_path).expect("open db");
                        if let Err(e) = upsert_add(&c, &devpath) {
                            error!("db upsert error {}: {}", devpath, e);
                        } else {
                            info!("device add {} recorded", devpath);
                        }
                    }
                    EventType::Remove => {
                        let c = Connection::open(&args.db_path).expect("open db");
                        if let Err(e) = mark_removed(&c, &devpath) {
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
