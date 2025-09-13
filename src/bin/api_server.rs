use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use actix_files::NamedFile;
use actix_multipart::Multipart;
use actix_web::{App, HttpResponse, HttpServer, Responder, delete, get, post, web};
use anyhow::Result;
use clap::Parser;
use futures_util::StreamExt;
use log::{error, info};
use tokio::{fs as tokio_fs, io::AsyncWriteExt};
use uuid::Uuid;

use storage_plus::{db::establish_pool, logging::init_logging, repo::file_repo::FileRepo};

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Storage root directory for uploaded files
    #[arg(long, default_value = "/mnt/storage_pool")]
    storage_root: PathBuf,
    /// SQLite db file path
    #[arg(long, default_value = "/var/lib/storage-plus/storage-plus.db")]
    db_path: PathBuf,
    /// Bind address
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
    /// Run migrations and exit (for testing/deployment)
    #[arg(long, default_value_t = false)]
    migrate_only: bool,
}

#[derive(Clone)]
struct AppState {
    storage_root: PathBuf,
    repo: Arc<FileRepo>,
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
    // Accept the first file field
    while let Some(item) = payload.next().await {
        let mut field = item.map_err(|e| actix_web::error::ErrorBadRequest(e.to_string()))?;
        let orig_name = field
            .content_disposition()
            .get_filename()
            .unwrap_or("file")
            .to_string();
        let content_type = field.content_type().map(|ct| ct.to_string());
        let key = Uuid::new_v4().to_string();
        let file_path = data.storage_root.join(&key);
        // ensure storage dir
        tokio_fs::create_dir_all(&data.storage_root)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        let mut f = tokio_fs::File::create(&file_path)
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        let mut size: i64 = 0;
        while let Some(chunk) = field.next().await {
            let bytes = chunk.map_err(|e| actix_web::error::ErrorBadRequest(e.to_string()))?;
            size += bytes.len() as i64;
            f.write_all(&bytes)
                .await
                .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        }
        let created_at = now_epoch();
        // insert metadata (blocking Diesel) on separate thread
        let repo = data.repo.clone();
        let fp = file_path.clone();
        let key_for_db = key.clone();
        let name_for_db = orig_name.clone();
        let ct_for_db = content_type.clone();
        let _inserted: usize = web::block(move || {
            repo.insert_file(
                &key_for_db,
                &name_for_db,
                ct_for_db.as_deref(),
                size,
                fp.to_string_lossy().as_ref(),
                created_at,
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
        let resp = serde_json::json!({"key": key, "filename": orig_name, "size": size});
        return Ok(HttpResponse::Ok().json(resp));
    }
    Ok(HttpResponse::BadRequest().body("no file part"))
}

#[get("/files/{key}")]
async fn download(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> actix_web::Result<NamedFile> {
    let key = path.into_inner();
    let key_db = key.clone();
    let repo = data.repo.clone();
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
    // Look up and delete file on disk; mark deleted in DB
    let repo = data.repo.clone();
    let key_db = key.clone();
    let meta_res = web::block(move || repo.get_by_key(&key_db))
        .await
        .map_err(|e| {
            error!("get_by_key error: {e:?}");
            actix_web::error::ErrorInternalServerError("db error")
        })?;
    let meta = meta_res.map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
    if let Some(m) = meta {
        if let Err(e) = tokio_fs::remove_file(&m.path).await {
            error!("remove_file error: {}", e);
        }
        let repo2 = data.repo.clone();
        let key_del = key.clone();
        let _affected: usize = web::block(move || repo2.mark_deleted(&key_del))
            .await
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    // Ensure directories like mounter
    fs::create_dir_all(&args.storage_root)?;
    if let Some(parent) = args.db_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pool = establish_pool(&args.db_path)?;
    let repo = Arc::new(FileRepo::new(pool));
    if args.migrate_only {
        info!("migrations applied, exiting due to --migrate-only flag");
        return Ok(());
    }

    let state = AppState {
        storage_root: args.storage_root.clone(),
        repo,
    };
    let bind_addr = args.addr.clone();
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
