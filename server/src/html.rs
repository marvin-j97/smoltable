use crate::{app_state::AppState, error::CustomRouteResult};
use actix_web::{http::header::ContentType, web, HttpResponse};
use smoltable::{ColumnFilter, ColumnKey};
use std::collections::HashMap;

pub async fn render_dashboard(data: web::Data<AppState>) -> CustomRouteResult<HttpResponse> {
    use smoltable::query::row::{ColumnOptions, Input, RowOptions};

    let start = std::time::Instant::now();

    let system_metrics = data.system_metrics_table.multi_get(vec![
        Input {
            row: RowOptions {
                key: "sys#cpu".into(),
                cell_limit: None,
            },
            column: Some(ColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ColumnKey::try_from("value:").expect("should be valid column key"),
                )),
                cell_limit: Some(1_440 / 2),
            }),
        },
        Input {
            row: RowOptions {
                key: "sys#mem".into(),
                cell_limit: None,
            },
            column: Some(ColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ColumnKey::try_from("value:").expect("should be valid column key"),
                )),
                cell_limit: Some(1_440 / 2),
            }),
        },
        Input {
            row: RowOptions {
                key: "wal#len".into(),
                cell_limit: None,
            },
            column: Some(ColumnOptions {
                filter: Some(ColumnFilter::Key(
                    ColumnKey::try_from("value:").expect("should be valid column key"),
                )),
                cell_limit: Some(1_440 / 2),
            }),
        },
        Input {
            row: RowOptions {
                key: "wbuf#size".into(),
                cell_limit: None,
            },
            column: Some(ColumnOptions {
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
                Input {
                    row: RowOptions {
                        key: "lat#write#cell".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "lat#write#batch".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "lat#read#pfx".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "lat#read#row".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "lat#del#row".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "stats#du".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "stats#seg_cnt".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "stats#row_cnt".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "stats#cell_cnt".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
                Input {
                    row: RowOptions {
                        key: "gc#del_cnt".into(),
                        cell_limit: None,
                    },
                    column: Some(ColumnOptions {
                        filter: Some(ColumnFilter::Key(
                            ColumnKey::try_from("value:").expect("should be valid column key"),
                        )),
                        cell_limit: Some(1_440 / 2),
                    }),
                },
            ])?;

            Ok((table_name.clone(), result.rows))
        })
        .collect::<smoltable::Result<HashMap<_, _>>>()?;

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
