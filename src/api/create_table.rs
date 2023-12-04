use super::format_server_header;
use crate::app_state::AppState;
use crate::error::CustomRouteResult;
use crate::is_valid_identifier;
use actix_web::{
    put,
    web::{self, Path},
    HttpResponse,
};
use serde_json::json;

#[put("/table/{name}")]
pub async fn handler(
    path: Path<String>,
    app_state: web::Data<AppState>,
) -> CustomRouteResult<HttpResponse> {
    let before = std::time::Instant::now();

    let table_name = path.into_inner();

    if !is_valid_identifier(&table_name) {
        let body = json!({
            "status": 400,
            "message": "Invalid table name",
            "result": null
        });

        let body = serde_json::to_string(&body).expect("should serialize");

        return Ok(HttpResponse::BadRequest()
            .append_header(("x-server", format_server_header()))
            .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
            .content_type("application/json; utf-8")
            .body(body));
    }

    let tables = app_state.user_tables.read().expect("lock is poisoned");
    if tables.contains_key(&table_name) {
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
    drop(tables);

    app_state.create_table(&table_name)?;

    let body = json!({
        "status": 201,
        "message": "Table created successfully",
        "result": null
    });
    let body = serde_json::to_string(&body).expect("should serialize");

    Ok(HttpResponse::Created()
        .append_header(("x-server", format_server_header()))
        .append_header(("x-took-ms", before.elapsed().as_millis().to_string()))
        .content_type("application/json; utf-8")
        .body(body))
}
