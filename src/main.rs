mod api;
mod app_state;
mod error;
mod manifest;
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

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// Define the allowed characters
const ALLOWED_CHARS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_#$";

pub fn is_valid_identifier(s: &str) -> bool {
    // Check if all characters in the string are allowed
    let all_allowed = s.chars().all(|c| ALLOWED_CHARS.contains(c));

    !s.is_empty() && s.len() < 512 && all_allowed
}

pub fn data_folder() -> PathBuf {
    let data_folder = std::env::var("SMOLTABLE_DATA_FOLDER").unwrap_or(".smoltable_data".into());
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

async fn catch_all() -> impl Responder {
    actix_files::NamedFile::open_async("./dist/index.html").await
}

#[actix_web::main]
async fn main() -> lsm_tree::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    log::info!("{} {}", env!("CARGO_CRATE_NAME"), env!("CARGO_PKG_VERSION"));

    let port = std::env::var("PORT")
        .or_else(|_| std::env::var("SMOLTABLE_PORT"))
        .unwrap_or("9876".into());

    let port = port.parse::<u16>().expect("invalid port");

    let manifest_table = ManifestTable::open()?;
    log::info!("Recovered manifest table");

    let user_tables = RwLock::new(recover_user_tables(&manifest_table)?);

    log::info!("Starting on port {port}");
    log::info!("Visit http://localhost:{port}");

    let app_state = web::Data::new(AppState {
        manifest_table: Arc::new(manifest_table),
        user_tables,
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::new("%r %s - %{User-Agent}i"))
            .app_data(app_state.clone())
            .service(api::list_tables::handler)
            .service(api::system::handler)
            .service(api::create_table::handler)
            .service(api::ingest::handler)
            .service(api::create_column_family::handler)
            .service(actix_files::Files::new("/", "./dist").index_file("index.html"))
            .default_service(web::route().to(catch_all))
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await?;

    Ok(())
}
