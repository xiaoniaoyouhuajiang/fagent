use octocrab::Octocrab;
use crate::error::{Result, FetcherError};

/// The `Fetcher` client, responsible for handling authentication and interaction with the GitHub API.
pub struct Fetcher {
    pub octocrab: Octocrab,
}

impl Fetcher {
    /// Creates a new `Fetcher` client instance.
    ///
    /// Optionally uses a personal access token for authentication to increase rate limits.
    pub fn new(token: Option<String>) -> Result<Self> {
        let mut builder = Octocrab::builder();
        if let Some(token) = token {
            builder = builder.personal_token(token);
        }
        let octocrab = builder.build().map_err(FetcherError::GitHub)?;
        Ok(Self { octocrab })
    }
}
