use crate::schema::files;
use diesel::prelude::*;

#[derive(Debug, Queryable, Identifiable)]
#[diesel(table_name = files)]
pub struct FileMeta {
    pub id: i32,
    pub key: String,
    pub filename: String,
    pub content_type: Option<String>,
    pub size: i64,
    pub path: String,
    pub created_at: i64,
    pub deleted: i32,
}

#[derive(Insertable)]
#[diesel(table_name = files)]
pub struct NewFileMeta<'a> {
    pub key: &'a str,
    pub filename: &'a str,
    pub content_type: Option<&'a str>,
    pub size: i64,
    pub path: &'a str,
    pub created_at: i64,
    pub deleted: i32,
}
