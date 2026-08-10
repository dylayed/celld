//! Celld's daemon-wide object-storage policy.

use anyhow::Context;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::{ClientOptions, ObjectMeta, ObjectStore, PutResult, RetryConfig};
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StaticCredentials {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) session_token: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ObjectStorageConfig {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    credentials: Option<StaticCredentials>,
    force_path_style: bool,
}

impl ObjectStorageConfig {
    pub(crate) fn from_bucket_uri(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
    ) -> anyhow::Result<Self> {
        Self::s3(bucket, endpoint, region, None)
    }

    pub(crate) fn s3(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<StaticCredentials>,
    ) -> anyhow::Result<Self> {
        let bucket = bucket.trim_start_matches("s3://");
        Ok(Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: endpoint.map(Into::into),
            credentials,
            force_path_style: endpoint.is_some(),
        })
    }

    pub(crate) fn managed(
        bucket: &str,
        region: String,
        endpoint: String,
        credentials: StaticCredentials,
    ) -> anyhow::Result<Self> {
        Self::s3(bucket, Some(&endpoint), &region, Some(credentials))
    }

    #[cfg(test)]
    pub(crate) fn with_credentials(mut self, credentials: StaticCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    fn runtime_builder(&self) -> AmazonS3Builder {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_virtual_hosted_style_request(!self.force_path_style);
        if let Some(endpoint) = self.endpoint.as_deref() {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(StaticCredentials {
            access_key_id,
            secret_access_key,
            session_token,
        }) = &self.credentials
        {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
            if let Some(token) = session_token.as_deref() {
                builder = builder.with_token(token);
            }
        }
        builder
    }

    pub(crate) fn build_ltx_store(&self) -> anyhow::Result<Arc<dyn ObjectStore>> {
        self.replica_config(String::new())
            .build_store()
            .map_err(anyhow::Error::from)
    }

    pub(crate) fn build_bucket_stores(
        &self,
        options: ClientOptions,
        ordinary: RetryConfig,
        cas: RetryConfig,
    ) -> anyhow::Result<(Arc<dyn ObjectStore>, Arc<dyn ObjectStore>)> {
        let builder = self
            .runtime_builder()
            .with_client_options(options)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        let store = builder
            .clone()
            .with_retry(ordinary)
            .build()
            .context("build s3 client")?;
        let cas_store = builder
            .with_retry(cas)
            .build()
            .context("build s3 cas client")?;
        Ok((Arc::new(store), Arc::new(cas_store)))
    }

    pub(crate) fn object_version(&self, meta: &ObjectMeta) -> String {
        meta.e_tag.clone().unwrap_or_default()
    }
    pub(crate) fn put_result_version(&self, result: PutResult) -> String {
        result.e_tag.unwrap_or_default()
    }
    pub(crate) fn update_version(&self, version: &str) -> object_store::UpdateVersion {
        object_store::UpdateVersion {
            e_tag: Some(version.into()),
            version: None,
        }
    }

    pub(crate) fn replica_config(&self, path: String) -> celld_ltx::ObjectStoreConfig {
        let env = |name| std::env::var(name).ok().filter(|value| !value.is_empty());
        let (access_key_id, secret_access_key, session_token) = match &self.credentials {
            None => (
                env("AWS_ACCESS_KEY_ID").unwrap_or_default(),
                env("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                env("AWS_SESSION_TOKEN").unwrap_or_default(),
            ),
            Some(StaticCredentials {
                access_key_id,
                secret_access_key,
                session_token,
            }) => (
                (!access_key_id.is_empty())
                    .then(|| access_key_id.clone())
                    .or_else(|| env("AWS_ACCESS_KEY_ID"))
                    .unwrap_or_default(),
                (!secret_access_key.is_empty())
                    .then(|| secret_access_key.clone())
                    .or_else(|| env("AWS_SECRET_ACCESS_KEY"))
                    .unwrap_or_default(),
                session_token
                    .clone()
                    .filter(|value| !value.is_empty())
                    .or_else(|| env("AWS_SESSION_TOKEN"))
                    .unwrap_or_default(),
            ),
        };
        celld_ltx::ObjectStoreConfig {
            bucket: self.bucket.clone(),
            path,
            region: self.region.clone(),
            endpoint: self.endpoint.clone().unwrap_or_default(),
            access_key_id,
            secret_access_key,
            session_token,
            force_path_style: self
                .endpoint
                .as_deref()
                .is_some_and(|value| !value.is_empty()),
            skip_verify: false,
            part_size: 0,
            concurrency: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_s3_and_bare_identically() {
        assert!(
            ObjectStorageConfig::from_bucket_uri("bucket", None, "r").unwrap()
                == ObjectStorageConfig::from_bucket_uri("s3://bucket", None, "r").unwrap()
        );
    }
    #[test]
    fn maps_etag_version() {
        let storage = ObjectStorageConfig::from_bucket_uri("b", None, "r").unwrap();
        let update = storage.update_version("tag");
        assert_eq!(update.e_tag.as_deref(), Some("tag"));
        assert!(update.version.is_none());
    }
}
