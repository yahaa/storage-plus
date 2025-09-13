use std::{
    fs,
    os::fd::AsFd,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use log::{debug, error, info, warn};
use nix::poll::{PollFd, PollFlags, poll};
use udev::{EventType, MonitorBuilder};

use crate::repo::device_repo::DeviceRepo;

/// Orchestrates device tracking, mounting, and periodic reconciliation.
pub struct Mounter {
    repo: Arc<dyn DeviceRepo>,
    storage_root: PathBuf,
    scan_interval: Duration,
}

impl Mounter {
    pub fn new<R>(repo: R, storage_root: PathBuf, scan_interval_secs: u64) -> Self
    where
        R: DeviceRepo + 'static,
    {
        Self {
            repo: Arc::new(repo),
            storage_root,
            scan_interval: Duration::from_secs(scan_interval_secs),
        }
    }

    fn now_epoch() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    fn fetch_uuid(&self, devnode: &str) -> Option<String> {
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

    fn is_mounted(&self, devnode: &str) -> bool {
        if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
            mounts.lines().any(|l| l.starts_with(devnode))
        } else {
            false
        }
    }

    fn pick_mount_path(&self, uuid: Option<String>) -> Result<PathBuf> {
        if let Some(u) = uuid {
            return Ok(self.storage_root.join(u));
        }
        fs::create_dir_all(&self.storage_root)?;
        let mut n = 1u32;
        loop {
            let p = self.storage_root.join(format!("disk{}", n));
            if !p.exists() {
                return Ok(p);
            }
            n += 1;
        }
    }

    fn mount_device(&self, devnode: &str, target: &Path) -> Result<bool> {
        fs::create_dir_all(target)?;
        Ok(Command::new("mount")
            .arg(devnode)
            .arg(target)
            .status()?
            .success())
    }

    fn upsert_device(&self, devnode: &str) -> Result<()> {
        if let Some(uuid) = self.fetch_uuid(devnode) {
            self.repo.upsert_device(devnode, &uuid, Self::now_epoch())?;
        }
        Ok(())
    }

    fn mark_removed(&self, devnode: &str) -> Result<()> {
        if self.is_mounted(devnode) {
            match Command::new("umount").arg(devnode).status() {
                Ok(st) if st.success() => info!("unmounted {}", devnode),
                Ok(_) => warn!("umount command failed for {}", devnode),
                Err(e) => error!("umount error for {}: {}", devnode, e),
            }
        }
        self.repo.mark_removed(devnode, Self::now_epoch())
    }

    fn process_pending(&self) -> Result<()> {
        let rows = self.repo.list_joined_active()?;
        for row in rows {
            let uuid_val = match row.uuid {
                Some(u) if !u.is_empty() => u,
                _ => {
                    warn!("device {} missing UUID, skipping", row.devnode);
                    continue;
                }
            };
            if row.mount_success == 1 && self.is_mounted(&row.devnode) {
                info!(
                    "{} already mounted {}",
                    row.devnode,
                    row.mount_path.as_deref().unwrap_or("?")
                );
                continue;
            }
            let target = if let Some(mp) = row.mount_path.clone().map(PathBuf::from) {
                mp
            } else {
                self.pick_mount_path(Some(uuid_val.clone()))?
            };
            if self.is_mounted(&row.devnode) {
                self.repo.mark_mounted_existing(
                    &row.devnode,
                    &target.to_string_lossy(),
                    &uuid_val,
                )?;
                continue;
            }
            match self.mount_device(&row.devnode, &target) {
                Ok(true) => {
                    info!("mounted {} at {:?}", row.devnode, target);
                    self.repo.update_mount_result(
                        &row.devnode,
                        &target.to_string_lossy(),
                        &uuid_val,
                    )?;
                }
                Ok(false) => error!("mount command failed for {}", row.devnode),
                Err(e) => error!("error mounting {}: {}", row.devnode, e),
            }
        }
        Ok(())
    }

    /// Spawn background thread for periodic reconciliation.
    pub fn start_scheduler(self: &Arc<Self>) {
        let this = Arc::clone(self);
        thread::spawn(move || {
            loop {
                if let Err(e) = this.process_pending() {
                    error!("process_pending error: {e}");
                } else {
                    debug!("scheduled scan complete");
                }
                thread::sleep(this.scan_interval);
            }
        });
    }

    /// Blocking udev event loop. Never returns unless there is a fatal error.
    pub fn run_udev_loop(self: &Arc<Self>) -> Result<()> {
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
                            if let Err(e) = self.upsert_device(&devpath) {
                                error!("db upsert error {}: {}", devpath, e);
                            } else {
                                info!("device add {} recorded", devpath);
                            }
                        }
                        EventType::Remove => {
                            if let Err(e) = self.mark_removed(&devpath) {
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
}
