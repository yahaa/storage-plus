use anyhow::Result;
use diesel::prelude::*;

use crate::{
    db::Pool,
    entity::file_meta::{FileMeta, NewFileMeta},
    schema::files,
};

struct FileRepoImpl {
    pool: Pool,
}

impl FileRepoImpl {
    fn new(pool: Pool) -> Self {
        Self { pool }
    }

    fn conn(
        &self,
    ) -> Result<r2d2::PooledConnection<diesel::r2d2::ConnectionManager<SqliteConnection>>> {
        Ok(self.pool.get()?)
    }

    pub fn insert_file(
        &self,
        key: &str,
        filename: &str,
        content_type: Option<&str>,
        size: i64,
        path: &str,
        created_at: i64,
    ) -> Result<usize> {
        let mut conn = self.conn()?;
        let new_row = NewFileMeta {
            key,
            filename,
            content_type,
            size,
            path,
            created_at,
            deleted: 0,
        };
        Ok(diesel::insert_into(files::table)
            .values(&new_row)
            .execute(&mut conn)?)
    }

    pub fn get_by_key(&self, key: &str) -> Result<Option<FileMeta>> {
        let mut conn = self.conn()?;
        let res = files::table
            .filter(files::key.eq(key))
            .filter(files::deleted.eq(0))
            .first::<FileMeta>(&mut conn)
            .optional()?;
        Ok(res)
    }

    pub fn soft_delete(&self, key: &str) -> Result<usize> {
        let mut conn = self.conn()?;
        Ok(diesel::update(files::table.filter(files::key.eq(key)))
            .set(files::deleted.eq(1))
            .execute(&mut conn)?)
    }
}

/// Repository interface for file metadata operations.
/// Public trait; concrete implementation is private to this module.
pub trait FileRepo: Send + Sync + 'static {
    fn insert_file(
        &self,
        key: &str,
        filename: &str,
        content_type: Option<&str>,
        size: i64,
        path: &str,
        created_at: i64,
    ) -> Result<usize>;

    fn get_by_key(&self, key: &str) -> Result<Option<FileMeta>>;

    fn soft_delete(&self, key: &str) -> Result<usize>;
}

impl FileRepo for FileRepoImpl {
    fn insert_file(
        &self,
        key: &str,
        filename: &str,
        content_type: Option<&str>,
        size: i64,
        path: &str,
        created_at: i64,
    ) -> Result<usize> {
        Self::insert_file(self, key, filename, content_type, size, path, created_at)
    }

    fn get_by_key(&self, key: &str) -> Result<Option<FileMeta>> {
        Self::get_by_key(self, key)
    }

    fn soft_delete(&self, key: &str) -> Result<usize> {
        Self::soft_delete(self, key)
    }
}

/// Create a new file repository instance. The concrete type is hidden; callers only see the trait.
pub fn new_file_repo(pool: Pool) -> impl FileRepo {
    FileRepoImpl::new(pool)
}
