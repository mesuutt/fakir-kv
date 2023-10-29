use thiserror::Error;

#[derive(Error, Debug)]
pub enum BitcaskError {
    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("not found")]
    NotFound
}