use super::format_server_header;
use crate::error::CustomRouteResult;
use crate::is_valid_identifier;
use crate::{app_state::AppState, manifest::ColumnFamilyDefinition};
use actix_web::{
    post,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

// TODO: TTL options etc JSON body
#[post("/table/{name}/column-family/{cf_name}")]
pub async fn handler(
    path: Path<(String, String)>,
    app_state: web::Data<AppState>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let (table_name, cf_name) = path.into_inner();

    if !is_valid_identifier(&cf_name) {
        let body = json!({
            "status": 400,
            "message": "Invalid column family name",
            "result": null
        });

        let body = serde_json::to_string(&body).expect("should serialize");

        return Ok(HttpResponse::BadRequest()
            .append_header(("x-server", format_server_header()))
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(body));
    }

    if app_state
        .manifest_table
        .column_family_exists(&table_name, &cf_name)?
    {
        let body = json!({
            "status": 409,
            "message": "Conflict",
            "result": null
        });
        let body = serde_json::to_string(&body).expect("should serialize");

        return Ok(HttpResponse::Conflict()
            .append_header(("x-server", format_server_header()))
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(body));
    }

    app_state
        .manifest_table
        .persist_column_family(&table_name, &ColumnFamilyDefinition { name: cf_name })?;

    let body = json!({
        "status": 201,
        "message": "Column family created successfully",
        "result": null
    });
    let body = serde_json::to_string(&body).expect("should serialize");

    Ok(HttpResponse::Created()
        .append_header(("x-server", format_server_header()))
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .content_type("application/json; utf-8")
        .body(body))
}
