use core::fmt;

//pub mod zfs;
pub mod zpool;

pub async fn pools() -> Result<Vec<PoolName>, Error> {
    zpool::ZpoolCmd::default().list_pools().await
}

pub async fn pool_list(pool: &str) -> Result<zpool::Zpool, Error> {
    zpool::ZpoolCmd::default().list_pool(pool).await
}

#[derive(Debug)]
pub struct PoolName(pub String);

impl fmt::Display for PoolName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for PoolName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

pub struct Error {
    inner: eyre::Report,
}

impl From<eyre::Report> for Error {
    fn from(inner: eyre::Report) -> Self {
        Error { inner }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl std::error::Error for Error {}
