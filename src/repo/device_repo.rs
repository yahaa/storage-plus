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
struct DeviceRepoImpl {
    pool: Pool,
}

impl DeviceRepoImpl {
    fn new(pool: Pool) -> Self {
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

    /// Returns the UUID of an active device if available.
    /// Policy: removed=0 AND joined=1 AND mount_success=1 AND uuid IS NOT NULL; pick first.
    pub fn get_active_uuid(&self) -> Result<Option<String>> {
        let mut conn = self.conn()?;
        use crate::schema::devices::dsl as d;
        let res = d::devices
            .filter(d::removed.eq(0))
            .filter(d::joined.eq(1))
            .filter(d::mount_success.eq(1))
            .select(d::uuid)
            .first::<Option<String>>(&mut conn)
            .optional()?;
        Ok(res.flatten())
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

/// Repository interface for device-related queries and mutations.
pub trait DeviceRepo: Send + Sync + 'static {
    fn upsert_device(&self, devnode: &str, uuid: &str, ts: i64) -> Result<()>;
    fn mark_removed(&self, devnode: &str, ts: i64) -> Result<()>;
    fn list_joined_active(&self) -> Result<Vec<DeviceMountRow>>;
    fn get_active_uuid(&self) -> Result<Option<String>>;
    fn update_mount_result(&self, devnode: &str, mount_path: &str, uuid: &str) -> Result<()>;
    fn mark_mounted_existing(&self, devnode: &str, mount_path: &str, uuid: &str) -> Result<()>;
}

impl DeviceRepo for DeviceRepoImpl {
    fn upsert_device(&self, devnode: &str, uuid: &str, ts: i64) -> Result<()> {
        DeviceRepoImpl::upsert_device(self, devnode, uuid, ts)
    }

    fn mark_removed(&self, devnode: &str, ts: i64) -> Result<()> {
        DeviceRepoImpl::mark_removed(self, devnode, ts)
    }

    fn list_joined_active(&self) -> Result<Vec<DeviceMountRow>> {
        DeviceRepoImpl::list_joined_active(self)
    }

    fn get_active_uuid(&self) -> Result<Option<String>> {
        DeviceRepoImpl::get_active_uuid(self)
    }

    fn update_mount_result(&self, devnode: &str, mount_path: &str, uuid: &str) -> Result<()> {
        DeviceRepoImpl::update_mount_result(self, devnode, mount_path, uuid)
    }

    fn mark_mounted_existing(&self, devnode: &str, mount_path: &str, uuid: &str) -> Result<()> {
        DeviceRepoImpl::mark_mounted_existing(self, devnode, mount_path, uuid)
    }
}

/// Create a new device repository instance. The concrete type is hidden; callers only see the trait.
pub fn new_device_repo(pool: Pool) -> impl DeviceRepo {
    DeviceRepoImpl::new(pool)
}
