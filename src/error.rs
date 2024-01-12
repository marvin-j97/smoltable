use actix_web::{HttpResponse, ResponseError};

#[derive(Debug)]
pub struct CustomHttpError(fjall::Error);

impl std::fmt::Display for CustomHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl ResponseError for CustomHttpError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().body("Internal Server Error")
    }
}

impl From<fjall::Error> for CustomHttpError {
    fn from(value: fjall::Error) -> Self {
        Self(value)
    }
}

impl From<std::io::Error> for CustomHttpError {
    fn from(value: std::io::Error) -> Self {
        Self(fjall::Error::from(value))
    }
}

pub type CustomRouteResult<T> = Result<T, CustomHttpError>;
