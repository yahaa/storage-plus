use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use actix_files::NamedFile;
use actix_multipart::Multipart;
use actix_web::{App, HttpResponse, HttpServer, Responder, delete, get, post, web};
use anyhow::Result;
use futures_util::StreamExt;
use log::{error, info};
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::{fs as tokio_fs, io::AsyncWriteExt};
use uuid::Uuid;

use crate::repo::device_repo::DeviceRepo;
use crate::repo::file_repo::FileRepo;
use crate::storage::{Storage, StorageImpl};

#[derive(Clone)]
struct AppState {
    storage: Arc<dyn Storage>,
    file_repo: Arc<dyn FileRepo>,
    device_repo: Arc<dyn DeviceRepo>,
    device_cache: Arc<DeviceUuidCache>,
}

#[derive(Debug)]
struct DeviceUuidCache {
    inner: RwLock<Option<(Vec<String>, Instant)>>,
    ttl: Duration,
}

impl DeviceUuidCache {
    fn new(ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(None),
            ttl,
        }
    }

    // Get cached device UUID if fresh; otherwise fetch via repo and update cache
    async fn get_or_fetch(&self, repo: Arc<dyn DeviceRepo>) -> actix_web::Result<String> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as usize;
        // Read cache snapshot
        if let Some((uuids, ts)) = { self.inner.read().await.clone() } {
            if ts.elapsed() < self.ttl {
                if uuids.is_empty() {
                    return Err(actix_web::error::ErrorServiceUnavailable(
                        "no active device uuid",
                    ));
                }

                let idx = nanos % uuids.len();
                return Ok(uuids[idx].clone());
            }
        }

        // Fetch from DB (blocking). Consider multiple devices: pick one at random among mounted.
        let rows = web::block(move || repo.list_joined_active())
            .await
            .map_err(|e| {
                actix_web::error::ErrorServiceUnavailable(format!("device uuid error: {e}"))
            })?
            .map_err(|e| {
                actix_web::error::ErrorServiceUnavailable(format!("device uuid error: {e}"))
            })?;
        let candidates: Vec<String> = rows
            .into_iter()
            .filter(|r| r.mount_success == 1)
            .filter_map(|r| r.uuid)
            .collect();
        {
            let mut w = self.inner.write().await;
            *w = Some((candidates.clone(), Instant::now()));
        }
        if candidates.is_empty() {
            return Err(actix_web::error::ErrorServiceUnavailable(
                "no active device uuid",
            ));
        }
        // Pseudo-random selection using current time nanos to avoid extra deps
        let idx = nanos % candidates.len();
        Ok(candidates[idx].clone())
    }
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[post("/upload")]
async fn upload(
    mut payload: Multipart,
    data: web::Data<AppState>,
) -> actix_web::Result<impl Responder> {
    while let Some(item) = payload.next().await {
        let mut field = item.map_err(|e| actix_web::error::ErrorBadRequest(e.to_string()))?;
        let orig_name = field
            .content_disposition()
            .get_filename()
            .unwrap_or("file")
            .to_string();
        let content_type = field.content_type().map(|ct| ct.to_string());
        let key = Uuid::new_v4().to_string();
        // device uuid: prefer cached value; if absent, query once and cache
        info!("uploading file: {}", orig_name);
        let device_uuid = data
            .device_cache
            .get_or_fetch(data.device_repo.clone())
            .await?;
        // log the device uuid being used
        info!("Using device UUID: {}", device_uuid);

        // stream write using storage
        let mut total: i64 = 0;
        // write via temp file using StorageImpl by feeding chunks
        let temp_path = data
            .storage
            .resolve_path(&device_uuid, &format!("{}.part", key))
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        if let Some(parent) = temp_path.parent() {
            tokio_fs::create_dir_all(parent)
                .await
                .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        }
        let mut f = tokio_fs::File::create(&temp_path)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        while let Some(chunk) = field.next().await {
            let bytes = chunk.map_err(|e| actix_web::error::ErrorBadRequest(e.to_string()))?;
            total += bytes.len() as i64;
            f.write_all(&bytes)
                .await
                .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        }
        f.flush().await.ok();
        drop(f);
        let final_path = data
            .storage
            .resolve_path(&device_uuid, &key)
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        tokio_fs::rename(&temp_path, &final_path)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        let size = total;
        let repo = data.file_repo.clone();
        let fp = final_path.clone();
        let fkey = key.clone();
        let fname = orig_name.clone();
        let _inserted: usize = web::block(move || {
            repo.insert_file(
                &fkey,
                &fname,
                content_type.as_deref(),
                size,
                fp.to_string_lossy().as_ref(),
                now_epoch(),
            )
        })
        .await
        .map_err(|e| {
            error!("insert_file error: {e:?}");
            actix_web::error::ErrorInternalServerError("db error")
        })?
        .map_err(|e| {
            error!("insert_file inner error: {e}");
            actix_web::error::ErrorInternalServerError("db error")
        })?;
        let resp = serde_json::json!({"key": key, "filename": orig_name, "size": size, "device_uuid": device_uuid});
        return Ok(HttpResponse::Ok().json(resp));
    }
    // add some logging here
    error!("upload called but no file part found in the request");
    Ok(HttpResponse::BadRequest().body("no file part"))
}

#[get("/files/{key}")]
async fn download(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> actix_web::Result<NamedFile> {
    let key = path.into_inner();
    let key_db = key.clone();
    let repo = data.file_repo.clone();
    let meta_res = web::block(move || repo.get_by_key(&key_db))
        .await
        .map_err(|e| {
            error!("get_by_key error: {e:?}");
            actix_web::error::ErrorInternalServerError("db error")
        })?;
    let meta = meta_res.map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
    let meta = meta.ok_or_else(|| actix_web::error::ErrorNotFound("not found"))?;
    let file = NamedFile::open(Path::new(&meta.path))?;
    Ok(
        file.set_content_disposition(actix_web::http::header::ContentDisposition {
            disposition: actix_web::http::header::DispositionType::Attachment,
            parameters: vec![actix_web::http::header::DispositionParam::Filename(
                String::from(meta.filename.clone()),
            )],
        }),
    )
}

#[delete("/files/{key}")]
async fn delete_file(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> actix_web::Result<impl Responder> {
    let key = path.into_inner();
    let repo = data.file_repo.clone();
    let key_db = key.clone();
    let meta_res = web::block(move || repo.get_by_key(&key_db))
        .await
        .map_err(|e| {
            error!("get_by_key error: {e:?}");
            actix_web::error::ErrorInternalServerError("db error")
        })?;
    let meta = meta_res.map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
    if let Some(m) = meta {
        // delete by path directly
        if let Err(e) = tokio_fs::remove_file(&m.path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                error!("remove_file error: {}", e);
            }
        }
        let repo2 = data.file_repo.clone();
        let key_del = key.clone();
        let _affected: usize = web::block(move || repo2.soft_delete(&key_del))
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub storage_root: PathBuf,
    pub addr: String,
    pub device_cache_ttl_secs: u64,
}

pub async fn run<R, D>(config: ServerConfig, repo: R, device_repo: D) -> Result<()>
where
    R: FileRepo + 'static,
    D: DeviceRepo + 'static,
{
    let state = AppState {
        storage: Arc::new(StorageImpl::new(config.storage_root.clone())) as Arc<dyn Storage>,
        file_repo: Arc::new(repo) as Arc<dyn FileRepo>,
        device_repo: Arc::new(device_repo) as Arc<dyn DeviceRepo>,
        device_cache: Arc::new(DeviceUuidCache::new(Duration::from_secs(
            config.device_cache_ttl_secs.max(1),
        ))),
    };
    let bind_addr = config.addr.clone();
    info!("Starting api-server at http://{}", &bind_addr);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .service(upload)
            .service(download)
            .service(delete_file)
    })
    .bind(&bind_addr)?
    .run()
    .await?;
    Ok(())
}
