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
use column_key::ColumnKey;
use error::CustomRouteResult;
use manifest::ColumnFamilyDefinition;
use manifest::ManifestTable;
use metrics::MetricsTable;
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};
use sysinfo::SystemExt;
use table::{
    writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter},
    Smoltable,
};
use tokio::sync::RwLock;

/* #[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc; */

fn recover_tables(manifest_table: &ManifestTable) -> lsm_tree::Result<HashMap<String, Smoltable>> {
    log::info!("Recovering user tables");

    let mut tables = HashMap::default();

    for table_name in manifest_table
        .get_user_table_names()?
        .into_iter()
        .filter(|x| !x.starts_with('_'))
    {
        log::debug!("Recovering user table {table_name}");

        let recovered_table = Smoltable::new(
            data_folder().join("tables").join(&table_name),
            manifest_table.tree.config().block_cache.clone(),
        )?;
        tables.insert(table_name, recovered_table);
    }

    log::info!("Recovered {} tables", tables.len());

    Ok(tables)
}

async fn catch_all(data: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    // Render metrics into html
    let system_metrics = data.metrics_table.query_timeseries("sys", None)?;
    let disk_usage = data.metrics_table.query_timeseries(
        "t#",
        Some(ColumnKey::try_from("stats:").expect("should be valid column key")),
    )?;
    let latency = data.metrics_table.query_timeseries(
        "t#",
        Some(ColumnKey::try_from("lat:").expect("should be valid column key")),
    )?;

    let html = if cfg!(debug_assertions) {
        // NOTE: Enable hot reload in debug mode
        std::fs::read_to_string("dist/index.html")?
    } else {
        include_str!("../dist/index.html").to_owned()
    };

    let html = html
        .replace(
            "{{system_metrics}}",
            &serde_json::to_string(&system_metrics.get(0).unwrap()).expect("should serialize"),
        )
        .replace(
            "{{disk_usage}}",
            &serde_json::to_string(&disk_usage).expect("should serialize"),
        )
        .replace(
            "{{latency}}",
            &serde_json::to_string(&latency).expect("should serialize"),
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
    let mut tables = recover_tables(&manifest_table)?;

    let metrics_table = {
        let existed_before = !data_folder().join("tables").join("_metrics").exists();

        let table = MetricsTable::create_new(block_cache)?;
        tables.insert("_metrics".into(), table.deref().clone());

        if !existed_before {
            manifest_table.persist_user_table("_metrics")?;
            manifest_table.persist_column_family(
                "_metrics",
                &ColumnFamilyDefinition {
                    name: "stats".into(),
                    version_limit: None,
                },
            )?;
        }

        table
    };

    let manifest_table = Arc::new(manifest_table);
    let tables = Arc::new(RwLock::new(tables));

    {
        let metrics_table = metrics_table.clone();
        let tables = tables.clone();

        log::debug!("Starting table size counting worker");

        // Start counting worker
        tokio::spawn(async move {
            loop {
                let tables = tables.read().await;

                for (table_name, table) in tables.iter() {
                    log::debug!("Counting {table_name}");

                    if let Ok(count) = table.cell_count() {
                        TableWriter::write_raw(
                            &metrics_table,
                            &RowWriteItem {
                                row_key: format!("t#{table_name}"),
                                cells: vec![ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:cell_cnt")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(count as f64),
                                }],
                            },
                        )
                        .ok();
                    }

                    log::debug!("Counted {table_name}");
                }

                drop(tables);

                metrics_table.tree.flush().unwrap();

                tokio::time::sleep(Duration::from_secs(3_600)).await;
            }
        });
    }

    {
        let metrics_table = metrics_table.clone();
        let tables = tables.clone();

        log::debug!("Starting system metrics worker");

        // Start metrics worker
        tokio::spawn(async move {
            loop {
                log::debug!("Saving system metrics");

                let sysinfo = sysinfo::System::new_all();

                let tables = tables.read().await;

                for (table_name, table) in tables.iter() {
                    let folder_size = table.disk_space_usage().unwrap_or(0);

                    TableWriter::write_raw(
                        &metrics_table,
                        &RowWriteItem {
                            row_key: format!("t#{table_name}"),
                            cells: vec![
                                ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:du")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(folder_size as f64),
                                },
                                ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:mem_cache")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(
                                        table.cache_memory_usage() as f64
                                    ),
                                },
                                ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:cache_blocks")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(
                                        table.cached_block_count() as f64
                                    ),
                                },
                            ],
                        },
                    )
                    .ok();
                }
                drop(tables);

                TableWriter::write_raw(
                    &metrics_table,
                    &RowWriteItem {
                        row_key: "sys".to_string(),
                        cells: vec![
                            ColumnWriteItem {
                                column_key: ColumnKey::try_from("stats:cpu")
                                    .expect("should be column key"),
                                timestamp: None,
                                value: table::cell::Value::F64(sysinfo.load_average().one),
                            },
                            ColumnWriteItem {
                                column_key: ColumnKey::try_from("stats:mem")
                                    .expect("should be column key"),
                                timestamp: None,
                                value: table::cell::Value::F64(sysinfo.used_memory() as f64),
                            },
                        ],
                    },
                )
                .ok();

                metrics_table.tree.flush().unwrap();

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    let app_state = web::Data::new(AppState {
        manifest_table,
        metrics_table,
        tables,
    });

    log::info!("Starting on port {port}");
    log::info!("Visit http://localhost:{port}");

    HttpServer::new(move || {
        let cors = actix_cors::Cors::default()
            .send_wildcard()
            .allow_any_origin()
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
            .service(api::metrics::handler)
            .service(api::delete_table::handler)
            .service(actix_files::Files::new("/", "./dist"))
            .default_service(web::route().to(catch_all))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
