use crate::error::CustomRouteResult;
use crate::identifier::is_valid_identifier;
use crate::response::build_response;
use crate::{app_state::AppState, manifest::ColumnFamilyDefinition};
use actix_web::http::StatusCode;
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct Input {
    row_limit: Option<u64>,
}

#[post("/table/{name}/column-family/{cf_name}")]
pub async fn handler(
    path: Path<(String, String)>,
    app_state: web::Data<AppState>,
    req_body: web::Json<Input>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let (table_name, cf_name) = path.into_inner();

    if !is_valid_identifier(&cf_name) {
        return Ok(build_response(
            before,
            StatusCode::BAD_REQUEST,
            "Invalid column family name",
            &json!(null),
        ));
    }

    if app_state
        .manifest_table
        .column_family_exists(&table_name, &cf_name)?
    {
        return Ok(build_response(
            before,
            StatusCode::CONFLICT,
            "Conflict",
            &json!(null),
        ));
    }

    app_state.manifest_table.persist_column_family(
        &table_name,
        &ColumnFamilyDefinition {
            name: cf_name,
            row_limit: req_body.row_limit,
        },
    )?;

    // TODO: spawn GC thread... spawn on recover as well
    // stop on delete table

    Ok(build_response(
        before,
        StatusCode::CREATED,
        "Column family created successfully",
        &json!(null),
    ))
}
