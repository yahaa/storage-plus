use diesel::prelude::*;
use anyhow::Result;
use crate::db::Pool;
use crate::schema::devices;

/// Row subset used during mount scheduling.
#[derive(Debug, Queryable)]
pub struct DeviceMountRow {
    pub devnode: String,
    pub uuid: Option<String>,
    pub mount_success: i32,
    pub mount_path: Option<String>,
}

/// Upsert (emulated) a device row when we see a device add/join event.
pub fn upsert_device(pool: &Pool, devnode: &str, uuid: &str, ts: i64) -> Result<()> {
    let mut conn = pool.get()?;
    conn.immediate_transaction(|c| {
        // Try update by uuid (unique logical identity) else insert.
        let updated = diesel::update(devices::table.filter(devices::uuid.eq(uuid)))
            .set((
                devices::devnode.eq(devnode),
                devices::removed.eq(0),
                devices::last_seen.eq(ts),
            ))
            .execute(c)?;
        if updated == 0 {
            diesel::insert_into(devices::table)
                .values((
                    devices::devnode.eq(devnode),
                    devices::uuid.eq(Some(uuid.to_string())),
                    devices::removed.eq(0),
                    devices::joined.eq(0),
                    devices::mount_success.eq(0),
                    devices::mount_path.eq::<Option<String>>(None),
                    devices::last_seen.eq(ts),
                ))
                .execute(c)?;
        }
        Ok::<(), diesel::result::Error>(())
    })?;
    Ok(())
}

pub fn mark_removed(pool: &Pool, devnode: &str, ts: i64) -> Result<()> {
    let mut conn = pool.get()?;
    diesel::update(devices::table.filter(devices::devnode.eq(devnode)))
        .set((
            devices::removed.eq(1),
            devices::mount_success.eq(0),
            devices::last_seen.eq(ts),
        ))
        .execute(&mut conn)?;
    Ok(())
}

pub fn list_joined_active(pool: &Pool) -> Result<Vec<DeviceMountRow>> {
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
        .load::<DeviceMountRow>(&mut conn)?;
    Ok(rows)
}

pub fn update_mount_result(pool: &Pool, devnode: &str, mount_path: &str, uuid: &str) -> Result<()> {
    let mut conn = pool.get()?;
    diesel::update(devices::table.filter(devices::devnode.eq(devnode)))
        .set((
            devices::mount_success.eq(1),
            devices::mount_path.eq(Some(mount_path.to_string())),
            devices::uuid.eq(Some(uuid.to_string())),
        ))
        .execute(&mut conn)?;
    Ok(())
}

pub fn mark_mounted_existing(pool: &Pool, devnode: &str, mount_path: &str, uuid: &str) -> Result<()> {
    update_mount_result(pool, devnode, mount_path, uuid)
}
