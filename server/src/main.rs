mod api;
mod app_state;
mod env;
mod error;
mod identifier;
mod manifest;
mod metrics;
mod response;

use crate::env::{data_folder, get_port};
use actix_web::{
    http::header::ContentType, middleware::Logger, web, App, HttpResponse, HttpServer,
};
use app_state::{AppState, MonitoredSmoltable};
use error::CustomRouteResult;
use manifest::ManifestTable;
use metrics::MetricsTable;
use smoltable::{
    ColumnFamilyDefinition, ColumnFilter, ColumnKey, CreateColumnFamilyInput,
    GarbageCollectionOptions, Smoltable, TableWriter,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use sysinfo::SystemExt;
use tokio::sync::RwLock;

async fn recover_tables(
    manifest_table: &ManifestTable,
) -> fjall::Result<HashMap<String, MonitoredSmoltable>> {
    log::info!("Recovering user tables");

    let mut tables = HashMap::default();

    for table_name in manifest_table
        .get_user_table_names()?
        .into_iter()
        .filter(|x| !x.starts_with('_'))
    {
        log::debug!("Recovering user table {table_name}");

        let recovered_table = Smoltable::open(&table_name, manifest_table.keyspace.clone())?;
        let metrics_table = MetricsTable::open(
            manifest_table.keyspace.clone(),
            &format!("_mtx_{table_name}"),
        )
        .await?;

        tables.insert(
            table_name,
            MonitoredSmoltable {
                inner: recovered_table,
                metrics: metrics_table,
            },
        );
    }

    log::info!("Recovered {} tables", tables.len());

    Ok(tables)
}

async fn catch_all(data: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    use smoltable::{QueryRowInput, QueryRowInputColumnOptions, QueryRowInputRowOptions};

    let start = std::time::Instant::now();

    let system_metrics = data.system_metrics_table.multi_get(vec![
        QueryRowInput {
            row: QueryRowInputRowOptions {
                key: "sys#cpu".into(),
            },
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ColumnKey::try_from("value:").expect("should be valid column key"),
                )),
                cell_limit: Some(1_440 / 2),
            }),
        },
        QueryRowInput {
            row: QueryRowInputRowOptions {
                key: "sys#mem".into(),
            },
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ColumnKey::try_from("value:").expect("should be valid column key"),
                )),
                cell_limit: Some(1_440 / 2),
            }),
        },
        QueryRowInput {
            row: QueryRowInputRowOptions {
                key: "wal#len".into(),
            },
            column: Some(QueryRowInputColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ColumnKey::try_from("value:").expect("should be valid column key"),
                )),
                cell_limit: Some(1_440 / 2),
            }),
        },
    ])?;

    let user_tables_lock = data.tables.read().await;
    let user_tables = user_tables_lock
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    drop(user_tables_lock);

    let table_stats = user_tables
        .iter()
        .map(|(table_name, table)| {
            let result = table.metrics.multi_get(vec![
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "lat#write#batch".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "lat#read#pfx".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "lat#read#row".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "lat#del#row".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "stats#du".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "stats#seg_cnt".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "stats#row_cnt".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "stats#cell_cnt".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                QueryRowInput {
                    row: QueryRowInputRowOptions {
                        key: "gc#del_cnt".into(),
                    },
                    column: Some(QueryRowInputColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
            ])?;

            Ok((table_name.clone(), result.rows))
        })
        .collect::<fjall::Result<HashMap<_, _>>>()?;

    let html = if cfg!(debug_assertions) {
        // NOTE: Enable hot reload in debug mode
        std::fs::read_to_string("dist/index.html")?
    } else {
        include_str!("../../dist/index.html").to_owned()
    };

    let html = html
        .replace(
            "{{system_metrics}}",
            &serde_json::to_string(&system_metrics.rows).expect("should serialize"),
        )
        .replace(
            "{{table_stats}}",
            &serde_json::to_string(&table_stats).expect("should serialize"),
        );

    let html = html.replace(
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
    let tables = recover_tables(&manifest_table).await?;

    let system_metrics_table = {
        let existed_before = keyspace.partition_exists("_man__metrics");

        let table = MetricsTable::open(keyspace.clone(), "_metrics").await?;

        if !existed_before {
            manifest_table.persist_user_table("_metrics")?;

            table.create_column_families(&CreateColumnFamilyInput {
                column_families: vec![
                    ColumnFamilyDefinition {
                        name: "value".into(),
                        gc_settings: GarbageCollectionOptions {
                            ttl_secs: None,
                            version_limit: None,
                        },
                    },
                    ColumnFamilyDefinition {
                        name: "stats".into(),
                        gc_settings: GarbageCollectionOptions {
                            ttl_secs: None,
                            version_limit: None,
                        },
                    },
                    ColumnFamilyDefinition {
                        name: "lat".into(),
                        gc_settings: GarbageCollectionOptions {
                            ttl_secs: None,
                            version_limit: None,
                        },
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
        let tables = tables.clone();

        log::info!("Starting TTL worker");

        // Start TTL worker
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;

            loop {
                let tables_lock = tables.read().await;
                let tables = tables_lock.clone();
                drop(tables_lock);

                for (table_name, table) in tables {
                    log::debug!("Running TTL worker on {table_name:?}");

                    match table.run_version_gc() {
                        Ok(deleted_count) => {
                            log::info!("Cell GC deleted {deleted_count} cells in {table_name:?}");

                            TableWriter::write_batch(
                                table.metrics.clone(),
                                &[smoltable::row!(
                                    "gc#del_cnt",
                                    vec![data_point!(deleted_count as f64)]
                                )],
                            )
                            .ok();
                        }
                        Err(e) => {
                            log::error!("Error during cell GC: {e:?}");
                        }
                    };
                }

                log::info!("TTL worker done");
                tokio::time::sleep(Duration::from_secs(/* 24 hours*/ 21_600 * 4)).await;
            }
        });
    }

    {
        let tables = tables.clone();

        log::info!("Starting row counting worker");

        // Start row counting worker
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;

            loop {
                let tables_lock = tables.read().await;
                let tables = tables_lock.clone();
                drop(tables_lock);

                let before = std::time::Instant::now();

                for (table_name, table) in tables {
                    log::debug!("Counting {table_name}");

                    if let Ok((row_count, cell_count)) = table.count() {
                        TableWriter::write_batch(
                            table.metrics.clone(),
                            &[
                                smoltable::row!(
                                    "stats#row_cnt",
                                    vec![data_point!(row_count as f64)]
                                ),
                                smoltable::row!(
                                    "stats#cell_cnt",
                                    vec![data_point!(cell_count as f64)]
                                ),
                            ],
                        )
                        .ok();
                    }

                    log::debug!("Counted {table_name}");
                }

                let time_s = before.elapsed().as_secs();

                log::info!("Counting worker done");

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
        let system_metrics_table = system_metrics_table.clone();
        let tables = tables.clone();

        log::info!("Starting system metrics worker");

        // Start metrics worker
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;

            loop {
                log::debug!("Saving system metrics");

                let sysinfo = sysinfo::System::new_all();

                let tables_lock = tables.read().await;
                let tables = tables_lock.clone();
                drop(tables_lock);

                for (_, table) in tables {
                    let folder_size = table.disk_space_usage();
                    let segment_count = table.segment_count();

                    TableWriter::write_batch(
                        table.metrics.clone(),
                        &[
                            smoltable::row!(
                                "stats#seg_cnt",
                                vec![data_point!(segment_count as f64)]
                            ),
                            smoltable::row!("stats#du", vec![data_point!(folder_size as f64)]),
                        ],
                    )
                    .ok();
                }

                let journal_count = keyspace.journal_count();

                TableWriter::write_batch(
                    system_metrics_table.clone(),
                    &[
                        smoltable::row!("sys#cpu", vec![data_point!(sysinfo.load_average().one)]),
                        smoltable::row!("sys#mem", vec![data_point!(sysinfo.used_memory() as f64)]),
                        smoltable::row!("wal#len", vec![data_point!(journal_count as f64)]),
                    ],
                )
                .ok();

                log::info!("System metrics worker done");
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    let app_state = web::Data::new(AppState {
        keyspace,
        manifest_table,
        system_metrics_table,
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
