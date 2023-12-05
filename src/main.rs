mod api;
mod app_state;
mod column_key;
mod error;
mod identifier;
mod manifest;
mod response;
mod table;

use actix_web::{middleware::Logger, web, App, HttpServer, Responder};
use app_state::AppState;
use manifest::ManifestTable;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};
use table::SmolTable;

/* #[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc; */

pub fn data_folder() -> PathBuf {
    let data_folder = std::env::var("SMOLTABLE_DATA").unwrap_or(".smoltable_data".into());
    PathBuf::from(&data_folder)
}

fn recover_user_tables(
    manifest_table: &ManifestTable,
) -> lsm_tree::Result<HashMap<String, SmolTable>> {
    log::info!("Recovering user tables");

    let mut user_tables = HashMap::default();

    for table_name in manifest_table.get_user_table_names()? {
        log::debug!("Recovering user table {table_name}");

        let recovered_table = SmolTable::new(data_folder().join("user_tables").join(&table_name))?;
        user_tables.insert(table_name, recovered_table);
    }

    log::info!("Recovered {} user tables", user_tables.len());

    Ok(user_tables)
}

fn get_port() -> u16 {
    let port = std::env::var("PORT")
        .or_else(|_| std::env::var("SMOLTABLE_PORT"))
        .unwrap_or("9876".into());

    port.parse::<u16>().expect("invalid port")
}

async fn catch_all() -> impl Responder {
    format!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"))
    //actix_files::NamedFile::open_async("./dist/index.html").await
}

#[actix_web::main]
async fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    log::info!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"));
    let port = get_port();

    let manifest_table = ManifestTable::open()?;
    let user_tables = RwLock::new(recover_user_tables(&manifest_table)?);

    let app_state = web::Data::new(AppState {
        manifest_table: Arc::new(manifest_table),
        user_tables,
    });

    log::info!("Starting on port {port}");
    log::info!("Visit http://localhost:{port}");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(app_state.clone())
            .service(api::list_tables::handler)
            .service(api::system::handler)
            .service(api::create_table::handler)
            .service(api::write::handler)
            .service(api::get_row::handler)
            .service(api::prefix::handler)
            .service(api::create_column_family::handler)
            .service(actix_files::Files::new("/", "./dist").index_file("index.html"))
            .default_service(web::route().to(catch_all))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
