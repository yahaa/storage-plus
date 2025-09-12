use crate::{db::Pool, schema::devices};
use anyhow::Result;
use diesel::prelude::*;

/// Row subset used during mount scheduling.
#[derive(Debug, Queryable)]
pub struct DeviceMountRow {
    pub devnode: String,
    pub uuid: Option<String>,
    pub mount_success: i32,
    pub mount_path: Option<String>,
}

#[derive(Clone)]
pub struct DeviceRepo {
    pool: Pool,
}

impl DeviceRepo {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn conn(
        &self,
    ) -> Result<r2d2::PooledConnection<diesel::r2d2::ConnectionManager<SqliteConnection>>> {
        Ok(self.pool.get()?)
    }

    pub fn upsert_device(&self, devnode: &str, uuid: &str, ts: i64) -> Result<()> {
        let mut conn = self.conn()?;
        conn.immediate_transaction(|c| {
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

    pub fn mark_removed(&self, devnode: &str, ts: i64) -> Result<()> {
        let mut conn = self.conn()?;
        diesel::update(devices::table.filter(devices::devnode.eq(devnode)))
            .set((
                devices::removed.eq(1),
                devices::mount_success.eq(0),
                devices::last_seen.eq(ts),
            ))
            .execute(&mut conn)?;
        Ok(())
    }

    pub fn list_joined_active(&self) -> Result<Vec<DeviceMountRow>> {
        let mut conn = self.conn()?;
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

    pub fn update_mount_result(&self, devnode: &str, mount_path: &str, uuid: &str) -> Result<()> {
        let mut conn = self.conn()?;
        diesel::update(
            devices::table
                .filter(devices::devnode.eq(devnode))
                .filter(devices::uuid.eq(uuid)),
        )
        .set((
            devices::mount_success.eq(1),
            devices::mount_path.eq(Some(mount_path.to_string())),
        ))
        .execute(&mut conn)?;
        Ok(())
    }

    pub fn mark_mounted_existing(&self, devnode: &str, mount_path: &str, uuid: &str) -> Result<()> {
        self.update_mount_result(devnode, mount_path, uuid)
    }
}
