use anyhow::Result;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use std::path::Path;

// 使用嵌入式 migrations，避免运行时查找当前工作目录导致的找不到 migrations 目录问题。
// 如果之前遇到 rust-analyzer 对 proc-macro 的问题，现在可以再尝试；若仍有 IDE 报错，可在构建/运行时不受影响。

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub type Pool = r2d2::Pool<ConnectionManager<SqliteConnection>>;

pub fn establish_pool(db_path: &Path) -> Result<Pool> {
    let db_path_str = db_path.to_string_lossy().to_string();
    let database_url = format!("sqlite://{}", db_path_str);
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = r2d2::Pool::builder().max_size(4).build(manager)?;
    {
        let mut conn = pool.get()?;
        run_migrations(&mut conn)?;
    }
    Ok(pool)
}

fn run_migrations(conn: &mut SqliteConnection) -> Result<()> {
    conn.run_pending_migrations(MIGRATIONS)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("migration error: {e}"))
}
