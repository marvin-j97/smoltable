mod api;
mod app_state;
mod env;
mod error;
mod html;
mod identifier;
mod manifest;
mod metrics;
mod recovery;
mod response;
mod worker;

use actix_web::{middleware::Logger, web, App, HttpServer};
use app_state::AppState;
use env::{data_folder, get_port, write_buffer_size};
use html::render_dashboard;
use manifest::ManifestTable;
use metrics::MetricsTable;
use recovery::recover_tables;
use smoltable::{ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions};
use std::sync::Arc;
use tokio::sync::RwLock;

fn print_banner() {
    eprintln!();
    eprintln!("                     | | |      | |   | |     ");
    eprintln!("  ___ _ __ ___   ___ | | |_ __ _| |__ | | ___ ");
    eprintln!(" / __| '_ ` _ \\ / _ \\| | __/ _` | '_ \\| |/ _ \\");
    eprintln!(" \\__ \\ | | | | | (_) | | || (_| | |_) | |  __/");
    eprintln!(" |___/_| |_| |_|\\___/|_|\\__\\__,_|_.__/|_|\\___|");
    eprintln!();
}

#[actix_web::main]
async fn main() -> smoltable::Result<()> {
    print_banner();

    env_logger::Builder::from_default_env().init();

    log::info!("smoltable server {}", env!("CARGO_PKG_VERSION"));
    let port = get_port();

    // NOTE: Block cache should be pretty small, because it will be mostly used for
    // metrics & manifest, because if the user really wants more cache, it should be
    // defined on a per-table/locality-group basis
    let block_cache = Arc::new(fjall::BlockCache::with_capacity_bytes(
        /* 8 MiB */ 8 * 1_024 * 1_024,
    ));

    let keyspace = fjall::Config::new(data_folder())
        .block_cache(block_cache.clone())
        .max_write_buffer_size(u64::from(write_buffer_size()) * 1_024 * 1_024)
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

    worker::start_all(&keyspace, &system_metrics_table, &tables);

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

        // custom `Json` extractor configuration
        let json_cfg = web::JsonConfig::default()
            // limit request payload size
            .limit(10 * 1_024 * 1_024);

        App::new()
            .wrap(cors)
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(json_cfg)
            .app_data(app_state.clone())
            .route("/", web::get().to(render_dashboard))
            .route("/index.html", web::get().to(render_dashboard))
            .service(api::list_tables::handler)
            .service(api::create_table::handler)
            .service(api::write::handler)
            .service(api::get_rows::handler)
            .service(api::delete_row::handler)
            .service(api::scan::handler)
            .service(api::create_column_family::handler)
            .service(api::metrics::handler)
            .service(api::delete_table::handler)
            .service(actix_files::Files::new("/", "./dist"))
            .default_service(web::route().to(render_dashboard))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
