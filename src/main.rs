mod api;
mod app_state;
mod column_key;
mod env;
mod error;
mod identifier;
mod manifest;
mod metrics;
mod response;
mod table;

use crate::env::{data_folder, get_port};
use actix_web::{
    http::header::ContentType, middleware::Logger, web, App, HttpResponse, HttpServer,
};
use app_state::AppState;
use base64::{engine::general_purpose, Engine};
use column_key::ColumnKey;
use error::CustomRouteResult;
use manifest::ManifestTable;
use metrics::MetricsTable;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use sysinfo::SystemExt;
use table::{
    writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter},
    SmolTable,
};

/* #[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc; */

fn recover_user_tables(
    manifest_table: &ManifestTable,
) -> lsm_tree::Result<HashMap<String, SmolTable>> {
    log::info!("Recovering user tables");

    let mut user_tables = HashMap::default();

    for table_name in manifest_table.get_user_table_names()? {
        log::debug!("Recovering user table {table_name}");

        let recovered_table = SmolTable::new(
            data_folder().join("user_tables").join(&table_name),
            manifest_table.config().block_cache.clone(),
        )?;
        user_tables.insert(table_name, recovered_table);
    }

    log::info!("Recovered {} user tables", user_tables.len());

    Ok(user_tables)
}

const INDEX_HTML: &str = include_str!("../dist/index.html");

async fn catch_all(data: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    // Render metrics into html
    let system_metrics = data.metrics_table.query_timeseries("sys")?;
    let disk_usage = data.metrics_table.query_timeseries("t#")?;

    let html = INDEX_HTML
        .replace(
            "{{system_metrics}}",
            &serde_json::to_string(&system_metrics).expect("should serialize"),
        )
        .replace(
            "{{disk_usage}}",
            &serde_json::to_string(&disk_usage).expect("should serialize"),
        );

    Ok(HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(html))
}

#[actix_web::main]
async fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::from_default_env().init();

    log::info!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"));
    let port = get_port();

    let block_cache = Arc::new(lsm_tree::BlockCache::with_capacity_blocks(
        /* MiB */ 64 * 1_024 * 1_024,
    ));

    let manifest_table = ManifestTable::open(block_cache.clone())?;
    let user_tables = Arc::new(RwLock::new(recover_user_tables(&manifest_table)?));
    let metrics_table = MetricsTable::new(block_cache)?;
    let manifest_table = Arc::new(manifest_table);

    {
        let metrics_table = metrics_table.clone();
        let user_tables = user_tables.clone();

        log::debug!("Starting system metrics worker");

        // Start metrics worker
        std::thread::spawn(move || loop {
            log::trace!("Saving system metrics");

            let sysinfo = sysinfo::System::new_all();

            let user_tables = user_tables.read().expect("lock is poisoned");

            for (table_name, table) in user_tables.iter() {
                let folder_size = table.disk_space_usage().expect("should get size");

                TableWriter::write_raw(
                    &metrics_table.0,
                    &RowWriteItem {
                        row_key: format!("t#{table_name}"),
                        cells: vec![
                            ColumnWriteItem {
                                column_key: ColumnKey::try_from("stats:du")
                                    .expect("should be column key"),
                                timestamp: None,
                                value: general_purpose::STANDARD.encode(folder_size.to_string()),
                            },
                            ColumnWriteItem {
                                column_key: ColumnKey::try_from("stats:mem_cache")
                                    .expect("should be column key"),
                                timestamp: None,
                                value: general_purpose::STANDARD
                                    .encode(table.cache_memory_usage().to_string()),
                            },
                            ColumnWriteItem {
                                column_key: ColumnKey::try_from("stats:cache_blocks")
                                    .expect("should be column key"),
                                timestamp: None,
                                value: general_purpose::STANDARD
                                    .encode(table.cached_block_count().to_string()),
                            },
                        ],
                    },
                )
                .expect("should write");
            }
            drop(user_tables);

            TableWriter::write_raw(
                &metrics_table.0,
                &RowWriteItem {
                    row_key: "sys".to_string(),
                    cells: vec![
                        ColumnWriteItem {
                            column_key: ColumnKey::try_from("stats:cpu")
                                .expect("should be column key"),
                            timestamp: None,
                            value: general_purpose::STANDARD
                                .encode(sysinfo.load_average().one.to_string()),
                        },
                        ColumnWriteItem {
                            column_key: ColumnKey::try_from("stats:mem")
                                .expect("should be column key"),
                            timestamp: None,
                            value: general_purpose::STANDARD
                                .encode(sysinfo.used_memory().to_string()),
                        },
                    ],
                },
            )
            .expect("should write");

            metrics_table.0.tree.flush().unwrap();

            std::thread::sleep(Duration::from_secs(60));
        });
    }

    let app_state = web::Data::new(AppState {
        manifest_table,
        metrics_table,
        user_tables,
    });

    log::info!("Starting on port {port}");
    log::info!("Visit http://localhost:{port}");

    HttpServer::new(move || {
        let cors = actix_cors::Cors::default()
            .send_wildcard()
            .allowed_methods(vec!["*"])
            .allowed_headers(vec!["*"])
            .allowed_header("*")
            .max_age(3600);

        App::new()
            .wrap(cors)
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(app_state.clone())
            .route("/", web::get().to(catch_all))
            .route("/index.html", web::get().to(catch_all))
            .service(api::list_tables::handler)
            .service(api::system::handler)
            .service(api::create_table::handler)
            .service(api::write::handler)
            .service(api::get_row::handler)
            .service(api::delete_row::handler)
            .service(api::prefix::handler)
            .service(api::create_column_family::handler)
            .service(actix_files::Files::new("/", "./dist"))
            .default_service(web::route().to(catch_all))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
