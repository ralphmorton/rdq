
#[derive(Debug)]
pub enum Error {
    R2d2Error(r2d2::Error),
    RedisError(redis::RedisError),
    ParseError(redis::streams::StreamId)
}

impl From<r2d2::Error> for Error {
    fn from(value: r2d2::Error) -> Self {
        Self::R2d2Error(value)
    }
}

impl From<redis::RedisError> for Error {
    fn from(value: redis::RedisError) -> Self {
        Self::RedisError(value)
    }
}
