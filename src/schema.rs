// @generated automatically by Diesel CLI (placeholder created by assistant)
// This file will be overwritten by `diesel print-schema` after running migrations.
// For now we declare the table manually to allow compilation.

// See migrations in `migrations/` for authoritative schema.

diesel::table! {
    devices (id) {
        id -> Integer,
        devnode -> Text,
        uuid -> Nullable<Text>,
        removed -> Integer,
        joined -> Integer,
        mount_success -> Integer,
        mount_path -> Nullable<Text>,
        last_seen -> BigInt,
    }
}
