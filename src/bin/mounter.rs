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
    #[arg(long, default_value_t = 5)]
    scan_interval_secs: u64,
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn ensure_db(conn: &Connection) -> Result<()> {
    // Initial table (older versions) had devnode TEXT UNIQUE. We now upsert by uuid instead,
    // allowing devnode to change (e.g., /dev/sdb -> /dev/sdc after reordering). We migrate if needed.
    conn.execute_batch(
        r#"CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        devnode TEXT NOT NULL,
        uuid TEXT,
        removed INTEGER NOT NULL DEFAULT 0,
        joined INTEGER NOT NULL DEFAULT 0,
        mount_success INTEGER NOT NULL DEFAULT 0,
        mount_path TEXT,
        last_seen INTEGER NOT NULL
    );"#,
    )?;

    // Detect legacy schema with UNIQUE on devnode and migrate.
    if let Ok(schema) = conn.query_row::<String, _, _>(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='devices'",
        [],
        |r| r.get(0),
    ) {
        if schema.contains("devnode TEXT UNIQUE") {
            // Perform migration: recreate table without UNIQUE on devnode.
            // We keep data; UNIQUE on devnode guaranteed no duplicates so safe copy.
            conn.execute_batch(
                r#"BEGIN IMMEDIATE TRANSACTION;
                CREATE TABLE devices_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    devnode TEXT NOT NULL,
                    uuid TEXT,
                    removed INTEGER NOT NULL DEFAULT 0,
                    joined INTEGER NOT NULL DEFAULT 0,
                    mount_success INTEGER NOT NULL DEFAULT 0,
                    mount_path TEXT,
                    last_seen INTEGER NOT NULL
                );
                INSERT INTO devices_new (id, devnode, uuid, removed, joined, mount_success, mount_path, last_seen)
                    SELECT id, devnode, uuid, removed, joined, mount_success, mount_path, last_seen FROM devices;
                DROP TABLE devices;
                ALTER TABLE devices_new RENAME TO devices;
                COMMIT;"#,
            )?;
        }
    }

    // Ensure a unique index on non-null UUIDs so ON CONFLICT(uuid) works (multiple NULLs allowed).
    // Previous version used a partial unique index (WHERE uuid IS NOT NULL) which SQLite
    // cannot target with ON CONFLICT(uuid). Replace it with a full unique index; SQLite
    // allows multiple NULLs in a UNIQUE index so behavior stays acceptable.
    conn.execute_batch(
        r#"DROP INDEX IF EXISTS devices_uuid_unique; CREATE UNIQUE INDEX IF NOT EXISTS devices_uuid_unique ON devices(uuid);"#,
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
    if uuid.is_none() {
        // Skip insert/update if we couldn't obtain a UUID yet.
        // Returning Ok so caller can retry later when the device settles.
        return Ok(());
    }
    let uuid_val = uuid.unwrap();
    // Insert new row or update existing by UUID. We do NOT reset mount_success/mount_path so they persist.
    conn.execute(r#"INSERT INTO devices (devnode, uuid, removed, joined, mount_success, mount_path, last_seen)
        VALUES (?1, ?2, 0, 0, 0, NULL, ?3)
        ON CONFLICT(uuid) DO UPDATE SET devnode=excluded.devnode, removed=0, last_seen=excluded.last_seen"#,
        params![devnode, uuid_val, now_epoch()])?;
    Ok(())
}

fn mark_removed(conn: &Connection, devnode: &str) -> Result<()> {
    // Query existing state to decide if we should attempt umount first.
    if let Ok(mut stmt) =
        conn.prepare("SELECT mount_path, mount_success FROM devices WHERE devnode=?1 and joined=1")
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

fn process_pending(conn: &Connection, storage_root: &Path) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT devnode, uuid, mount_success, mount_path FROM devices WHERE removed=0 and joined=1",
    )?;
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
            // Ensure DB reflects path/uuid.
            conn.execute(
                "UPDATE devices SET mount_success=1, mount_path=?2, uuid=?3 WHERE devnode=?1",
                params![devnode, target.to_string_lossy(), uuid_val],
            )?;
            continue;
        }

        match mount_device(&devnode, &target) {
            Ok(true) => {
                info!("mounted {} at {:?}", devnode, target);
                conn.execute(
                    "UPDATE devices SET mount_success=1, mount_path=?2, uuid=?3 WHERE devnode=?1",
                    params![devnode, target.to_string_lossy(), uuid_val],
                )?;
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
    thread::spawn(move || {
        loop {
            match Connection::open(&db_path_clone) {
                Ok(c) => {
                    if let Err(e) = ensure_db(&c) {
                        error!("ensure_db error: {e}");
                    }
                    if let Err(e) = process_pending(&c, &storage_root_clone) {
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
