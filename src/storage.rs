use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use log::{debug, error};
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::{fs, fs::File};
use uuid::Uuid;

/// Storage layout helper: {root}/{device_uuid}/{object_key}
#[async_trait]
pub trait Storage: Send + Sync {
    /// Resolve the absolute path for a (device_uuid, object_key)
    fn resolve_path(&self, device_uuid: &str, object_key: &str) -> Result<PathBuf>;

    /// Write from a stream into the resolved path. Returns (final_path, total_bytes)
    async fn write_stream<R>(
        &self,
        device_uuid: &str,
        object_key: &str,
        reader: &mut R,
    ) -> Result<(PathBuf, i64)>
    where
        R: AsyncRead + Unpin + Send,
        Self: Sized;

    /// Read entire file to bytes
    async fn read_all(&self, device_uuid: &str, object_key: &str) -> Result<Vec<u8>>;

    /// Open a reader for the file (seek to start) returning tokio File
    async fn open_reader(&self, device_uuid: &str, object_key: &str) -> Result<File>;

    /// Delete the resolved path if it exists; Ok if missing
    async fn delete(&self, device_uuid: &str, object_key: &str) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct StorageImpl {
    root: PathBuf,
}

impl StorageImpl {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn ensure_segment(segment: &str, label: &str) -> Result<()> {
        if segment.is_empty() {
            bail!("{} must not be empty", label);
        }
        if segment.contains('/') || segment.contains('\\') {
            bail!(
                "{} must be a single path segment without separators: {}",
                label,
                segment
            );
        }
        if segment == "." || segment == ".." {
            bail!("{} must not be '.' or '..'", label);
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for StorageImpl {
    fn resolve_path(&self, device_uuid: &str, object_key: &str) -> Result<PathBuf> {
        Self::ensure_segment(device_uuid, "device_uuid")?;
        Self::ensure_segment(object_key, "object_key")?;
        Ok(self.root.join(device_uuid).join(object_key))
    }

    async fn write_stream<R>(
        &self,
        device_uuid: &str,
        object_key: &str,
        reader: &mut R,
    ) -> Result<(PathBuf, i64)>
    where
        R: AsyncRead + Unpin + Send,
    {
        let final_path = self.resolve_path(device_uuid, object_key)?;
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create_dir_all {:?}", parent))?;
        }
        // write to a temp file under the same directory, then atomic rename
        let tmp_name = format!("{}.{}.part", object_key, Uuid::new_v4());
        let tmp_path = final_path
            .parent()
            .map(|p| p.join(tmp_name))
            .unwrap_or_else(|| PathBuf::from(format!("{}.{}.part", object_key, Uuid::new_v4())));

        let mut file = File::create(&tmp_path)
            .await
            .with_context(|| format!("create temp file {:?}", tmp_path))?;
        let mut total: i64 = 0;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n]).await?;
            total += n as i64;
        }
        file.flush().await.ok();
        drop(file);

        // Atomic rename to final target
        fs::rename(&tmp_path, &final_path)
            .await
            .with_context(|| format!("rename {:?} -> {:?}", tmp_path, final_path))?;
        debug!("wrote {} bytes to {:?}", total, final_path);
        Ok((final_path, total))
    }

    async fn read_all(&self, device_uuid: &str, object_key: &str) -> Result<Vec<u8>> {
        let path = self.resolve_path(device_uuid, object_key)?;
        let mut f = File::open(&path)
            .await
            .with_context(|| format!("open {:?}", path))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    async fn open_reader(&self, device_uuid: &str, object_key: &str) -> Result<File> {
        let path = self.resolve_path(device_uuid, object_key)?;
        let mut f = File::open(&path)
            .await
            .with_context(|| format!("open {:?}", path))?;
        f.rewind().await.ok();
        Ok(f)
    }

    async fn delete(&self, device_uuid: &str, object_key: &str) -> Result<()> {
        let path = self.resolve_path(device_uuid, object_key)?;
        match fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => {
                error!("remove_file {:?} error: {}", path, e);
                Err(e.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, duplex};

    #[tokio::test]
    async fn write_and_delete_roundtrip() -> Result<()> {
        let tmp_dir = std::env::temp_dir().join(format!("storage-plus-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&tmp_dir).await?;
        let storage = StorageImpl::new(&tmp_dir);
        let device_uuid = "dev-123";
        let object_key = "obj-456";

        let data = b"hello world".to_vec();
        let (mut rd, mut wr) = duplex(1024);
        let data_clone = data.clone();
        tokio::spawn(async move {
            let _ = wr.write_all(&data_clone).await;
            let _ = wr.shutdown().await; // signal EOF
        });
        let (path, sz) = storage
            .write_stream(device_uuid, object_key, &mut rd)
            .await?;
        assert_eq!(sz, data.len() as i64);
        assert!(path.exists());
        assert!(path.starts_with(&tmp_dir.join(device_uuid)));

        // read back
        let bytes = storage.read_all(device_uuid, object_key).await?;
        assert_eq!(bytes, data);

        // delete
        storage.delete(device_uuid, object_key).await?;
        assert!(!path.exists());
        Ok(())
    }
}
