use diesel::prelude::*;
use crate::schema::devices;

#[derive(Debug, Queryable, Identifiable)]
#[diesel(table_name = devices)]
pub struct Device {
    pub id: i32,
    pub devnode: String,
    pub uuid: Option<String>,
    pub removed: i32,
    pub joined: i32,
    pub mount_success: i32,
    pub mount_path: Option<String>,
    pub last_seen: i64,
}

#[derive(Insertable)]
#[diesel(table_name = devices)]
pub struct NewDevice<'a> {
    pub devnode: &'a str,
    pub uuid: Option<&'a str>,
    pub removed: i32,
    pub joined: i32,
    pub mount_success: i32,
    pub mount_path: Option<&'a str>,
    pub last_seen: i64,
}

impl<'a> NewDevice<'a> {
    pub fn new(devnode: &'a str, uuid: Option<&'a str>, ts: i64) -> Self {
        Self { devnode, uuid, removed: 0, joined: 0, mount_success: 0, mount_path: None, last_seen: ts }
    }
}
