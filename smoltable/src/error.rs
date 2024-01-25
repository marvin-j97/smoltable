#[derive(Debug)]
pub enum Error {
    Storage(fjall::Error),
    Tree(fjall::LsmError),
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SmoltableError")
    }
}

impl std::error::Error for Error {}

impl From<fjall::Error> for Error {
    fn from(value: fjall::Error) -> Self {
        Error::Storage(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<fjall::LsmError> for Error {
    fn from(value: fjall::LsmError) -> Self {
        Error::Tree(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
