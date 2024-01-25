use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::identifier::is_valid_table_identifier;
use crate::response::build_response;
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smoltable::{ColumnFamilyDefinition, CreateColumnFamilyInput, GarbageCollectionOptions};

#[derive(Debug, Deserialize, Serialize)]
struct ColumnFamilyDefinitionInput {
    pub name: String,
    pub gc_settings: Option<GarbageCollectionOptions>,
}

#[derive(Debug, Deserialize)]
struct Input {
    pub column_families: Vec<ColumnFamilyDefinitionInput>,
    pub locality_group: Option<bool>,
}

impl From<ColumnFamilyDefinitionInput> for ColumnFamilyDefinition {
    fn from(val: ColumnFamilyDefinitionInput) -> Self {
        ColumnFamilyDefinition {
            name: val.name,
            gc_settings: val.gc_settings.unwrap_or_default(),
        }
    }
}

#[post("/v1/table/{name}/column-family")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let table_name = path.into_inner();

    if table_name.starts_with('_') {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::FORBIDDEN,
            "Invalid table name",
            &json!(null),
        ));
    }

    if !is_valid_table_identifier(&table_name) {
        return Ok(build_response(
            before.elapsed(),
            StatusCode::BAD_REQUEST,
            "Invalid table name",
            &json!(null),
        ));
    }

    let tables = app_state.tables.write().await;

    if let Some(table) = tables.get(&table_name) {
        let existing_families = table
            .list_column_families()?
            .into_iter()
            .map(|x| x.name)
            .collect::<Vec<_>>();

        for family in req_body.column_families.iter().map(|x| x.name.clone()) {
            if existing_families.contains(&family) {
                return Ok(build_response(
                    before.elapsed(),
                    StatusCode::CONFLICT,
                    &format!("Column family {family} already exists"),
                    &json!(null),
                ));
            }
        }

        let column_families = req_body
            .0
            .column_families
            .into_iter()
            .map(Into::into)
            .collect();

        table.create_column_families(&CreateColumnFamilyInput {
            column_families,
            locality_group: req_body.0.locality_group,
        })?;

        Ok(build_response(
            before.elapsed(),
            StatusCode::CREATED,
            "Column families created successfully",
            &json!(null),
        ))
    } else {
        Ok(build_response(
            before.elapsed(),
            StatusCode::NOT_FOUND,
            "Table not found",
            &json!(null),
        ))
    }
}
