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
use manifest::ManifestTable;
use metrics::MetricsTable;
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};
use sysinfo::SystemExt;
use table::{
    single_row_reader::{QueryRowInput, QueryRowInputColumnOptions, QueryRowInputRowOptions},
    writer::{ColumnWriteItem, RowWriteItem, Writer as TableWriter},
    ColumnFamilyDefinition, CreateColumnFamilyInput, Smoltable,
};
use tokio::sync::RwLock;

async fn recover_tables(
    manifest_table: &ManifestTable,
) -> fjall::Result<HashMap<String, Smoltable>> {
    log::info!("Recovering user tables");

    let mut tables = HashMap::default();

    for table_name in manifest_table
        .get_user_table_names()?
        .into_iter()
        .filter(|x| !x.starts_with('_'))
    {
        log::debug!("Recovering user table {table_name}");

        let recovered_table = Smoltable::open(&table_name, manifest_table.keyspace.clone())?;
        tables.insert(table_name, recovered_table);
    }

    log::info!("Recovered {} tables", tables.len());

    Ok(tables)
}

// TODO: metrics of user tables should be stored in separate metrics tables

async fn catch_all(data: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    let start = std::time::Instant::now();

    // TODO: 1 row per timeseries
    // TODO: change how metrics are stored, 1 metric table per user table etc...

    let system_metrics = data.metrics_table.multi_get(vec![QueryRowInput {
        row: QueryRowInputRowOptions { key: "sys".into() },
        // cell: None,
        column: Some(QueryRowInputColumnOptions {
            filter: Some(table::ColumnFilter::Key(
                ColumnKey::try_from("stats:").expect("should be valid column key"),
            )),
            cell_limit: Some(1_440 / 2),
        }),
    }])?;

    let table_names = data
        .tables
        .read()
        .await
        .keys()
        .map(|x| format!("t#{x}"))
        .collect::<Vec<_>>();

    let disk_usage = data.metrics_table.multi_get(
        table_names
            .iter()
            .map(|name| {
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: name.to_owned(),
                    },
                    // cell: None,
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(table::ColumnFilter::Key(
                            ColumnKey::try_from("stats:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                }
            })
            .collect::<Vec<_>>(),
    )?;

    let latency = data.metrics_table.multi_get(
        table_names
            .iter()
            .map(|name| {
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: name.to_owned(),
                    },
                    // cell: NOne
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(table::ColumnFilter::Key(
                            ColumnKey::try_from("lat:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                }
            })
            .collect::<Vec<_>>(),
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
            &serde_json::to_string(&system_metrics.rows.first().unwrap())
                .expect("should serialize"),
        )
        .replace(
            "{{disk_usage}}",
            &serde_json::to_string(&disk_usage.rows).expect("should serialize"),
        )
        .replace(
            "{{latency}}",
            &serde_json::to_string(&latency.rows).expect("should serialize"),
        )
        .replace(
            "{{render_time_ms}}",
            &start.elapsed().as_millis().to_string(),
        );

    Ok(HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(html))
}

#[actix_web::main]
async fn main() -> fjall::Result<()> {
    env_logger::Builder::from_default_env().init();

    log::info!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"));
    let port = get_port();

    let block_cache = Arc::new(fjall::BlockCache::with_capacity_bytes(
        /* 16 MiB */ 16 * 1_024 * 1_024,
    ));

    let keyspace = fjall::Config::new(data_folder())
        .block_cache(block_cache.clone())
        // TODO: write buffer setting
        .open()?;

    let manifest_table = ManifestTable::open(keyspace.clone())?;
    let mut tables = recover_tables(&manifest_table).await?;

    let metrics_table = {
        let existed_before = keyspace.partition_exists("_metrics");

        let table = MetricsTable::create_new(keyspace.clone()).await?;

        tables.insert("_metrics".into(), table.deref().clone());

        if !existed_before {
            manifest_table.persist_user_table("_metrics")?;

            table.create_column_families(&CreateColumnFamilyInput {
                column_families: vec![
                    ColumnFamilyDefinition {
                        name: "stats".into(),
                        version_limit: None,
                    },
                    ColumnFamilyDefinition {
                        name: "lat".into(),
                        version_limit: None,
                    },
                ],
                locality_group: None,
            })?;
        }

        table
    };

    let manifest_table = Arc::new(manifest_table);
    let tables = Arc::new(RwLock::new(tables));

    {
        let keyspace = keyspace.clone();
        let metrics_table = metrics_table.clone();
        let tables = tables.clone();

        log::debug!("Starting row counting worker");

        // Start row counting worker
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;

            loop {
                let tables = tables.read().await;

                let before = std::time::Instant::now();

                for (table_name, table) in tables.iter() {
                    log::debug!("Counting {table_name}");

                    if let Ok((row_count, cell_count)) = table.count() {
                        TableWriter::write_raw(
                            metrics_table.deref().clone(),
                            &RowWriteItem {
                                row_key: format!("t#{table_name}"),
                                cells: vec![ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:row_cnt")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(row_count as f64),
                                }],
                            },
                        )
                        .ok();

                        TableWriter::write_raw(
                            metrics_table.deref().clone(),
                            &RowWriteItem {
                                row_key: format!("t#{table_name}"),
                                cells: vec![ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:cell_cnt")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(cell_count as f64),
                                }],
                            },
                        )
                        .ok();
                    }

                    log::debug!("Counted {table_name}");
                }

                drop(tables);

                keyspace.persist().unwrap();

                let time_s = before.elapsed().as_secs();

                let sleep_time = match time_s {
                    _ if time_s < 5 => 60,
                    _ if time_s < 60 => 3_600,
                    _ => 21_600, // 6 hours
                };

                tokio::time::sleep(Duration::from_secs(sleep_time)).await;
            }
        });
    }

    {
        let keyspace = keyspace.clone();
        let metrics_table = metrics_table.clone();
        let tables = tables.clone();

        log::debug!("Starting system metrics worker");

        // Start metrics worker
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;

            loop {
                log::debug!("Saving system metrics");

                let sysinfo = sysinfo::System::new_all();

                let tables = tables.read().await;

                for (table_name, table) in tables.iter() {
                    let folder_size = table.disk_space_usage();
                    let segment_count = table.segment_count();

                    TableWriter::write_raw(
                        metrics_table.deref().clone(),
                        &RowWriteItem {
                            row_key: format!("t#{table_name}"),
                            cells: vec![
                                ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:seg_cnt")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(segment_count as f64),
                                },
                                ColumnWriteItem {
                                    column_key: ColumnKey::try_from("stats:du")
                                        .expect("should be column key"),
                                    timestamp: None,
                                    value: table::cell::Value::F64(folder_size as f64),
                                },
                            ],
                        },
                    )
                    .ok();
                }
                drop(tables);

                let journal_count = keyspace.journal_count();

                TableWriter::write_raw(
                    metrics_table.deref().clone(),
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
                            ColumnWriteItem {
                                column_key: ColumnKey::try_from("stats:wal_cnt")
                                    .expect("should be column key"),
                                timestamp: None,
                                value: table::cell::Value::Byte(journal_count as u8),
                            },
                        ],
                    },
                )
                .ok();

                keyspace.persist().unwrap();

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    let app_state = web::Data::new(AppState {
        keyspace,
        manifest_table,
        metrics_table,
        tables,
        block_cache,
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
            .service(api::create_table::handler)
            .service(api::write::handler)
            .service(api::get_rows::handler)
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
