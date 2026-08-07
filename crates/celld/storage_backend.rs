//! Celld's daemon-wide object-storage policy.

use anyhow::{ensure, Context};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::gcp::GoogleCloudStorageBuilder;
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
    provider: Provider,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    credentials: Option<StaticCredentials>,
    force_path_style: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Provider {
    S3,
    Gcs,
}

impl ObjectStorageConfig {
    pub(crate) fn from_bucket_uri(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
    ) -> anyhow::Result<Self> {
        if let Some(bucket) = bucket.strip_prefix("gs://") {
            ensure!(
                endpoint.is_none(),
                "--endpoint is S3-only and conflicts with gs:// storage"
            );
            ensure!(
                !bucket.is_empty() && !bucket.contains(['/', '?', '#']),
                "gs:// storage target must contain only a bucket name"
            );
            return Ok(Self {
                provider: Provider::Gcs,
                bucket: bucket.into(),
                region: String::new(),
                endpoint: None,
                credentials: None,
                force_path_style: false,
            });
        }
        if let Some((scheme, _)) = bucket.split_once("://") {
            ensure!(scheme == "s3", "unsupported storage scheme {scheme}://");
        }
        Self::s3(bucket, endpoint, region, None)
    }

    pub(crate) fn s3(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<StaticCredentials>,
    ) -> anyhow::Result<Self> {
        let bucket = bucket.trim_start_matches("s3://");
        ensure!(!bucket.is_empty(), "s3: bucket name is required");
        Ok(Self {
            provider: Provider::S3,
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

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn scheme(&self) -> &'static str {
        match self.provider {
            Provider::S3 => "s3",
            Provider::Gcs => "gs",
        }
    }

    pub(crate) fn uri(&self) -> String {
        format!("{}://{}", self.scheme(), self.bucket)
    }

    pub(crate) fn enrollment_bucket(&self) -> String {
        match self.provider {
            Provider::S3 => self.bucket.clone(),
            Provider::Gcs => self.uri(),
        }
    }

    pub(crate) fn object_uri(&self, path: &str) -> String {
        format!(
            "{}://{}/{}",
            self.scheme(),
            self.bucket,
            path.trim_start_matches('/')
        )
    }

    fn runtime_s3_builder(&self) -> AmazonS3Builder {
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

    fn runtime_gcs_builder(&self) -> GoogleCloudStorageBuilder {
        // Honor the standard ADC file override without importing unrelated
        // object_store-specific environment configuration.
        let mut builder = GoogleCloudStorageBuilder::new();
        if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            if !path.is_empty() {
                builder = builder.with_application_credentials(path);
            }
        }
        builder.with_bucket_name(&self.bucket)
    }

    pub(crate) fn build_ltx_store(&self) -> anyhow::Result<Arc<dyn ObjectStore>> {
        match self.provider {
            Provider::S3 => self
                .replica_config(String::new())
                .build_store()
                .map_err(anyhow::Error::from),
            Provider::Gcs => self
                .runtime_gcs_builder()
                .with_retry(RetryConfig::default())
                .build()
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                .context("build shared GCS object store"),
        }
    }

    pub(crate) fn build_bucket_stores(
        &self,
        options: ClientOptions,
        ordinary: RetryConfig,
        cas: RetryConfig,
    ) -> anyhow::Result<(Arc<dyn ObjectStore>, Arc<dyn ObjectStore>)> {
        match self.provider {
            Provider::S3 => {
                let builder = self
                    .runtime_s3_builder()
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
            Provider::Gcs => {
                let builder = self.runtime_gcs_builder().with_client_options(options);
                let store = builder
                    .clone()
                    .with_retry(ordinary)
                    .build()
                    .context("build gcs client")?;
                let cas_store = builder
                    .with_retry(cas)
                    .build()
                    .context("build gcs cas client")?;
                Ok((Arc::new(store), Arc::new(cas_store)))
            }
        }
    }

    pub(crate) fn object_version(&self, meta: &ObjectMeta) -> String {
        match self.provider {
            Provider::S3 => meta.e_tag.clone(),
            Provider::Gcs => meta.version.clone(),
        }
        .unwrap_or_default()
    }

    pub(crate) fn put_result_version(&self, result: PutResult) -> String {
        match self.provider {
            Provider::S3 => result.e_tag,
            Provider::Gcs => result.version,
        }
        .unwrap_or_default()
    }

    pub(crate) fn update_version(&self, version: &str) -> object_store::UpdateVersion {
        match self.provider {
            Provider::S3 => object_store::UpdateVersion {
                e_tag: Some(version.into()),
                version: None,
            },
            Provider::Gcs => object_store::UpdateVersion {
                e_tag: None,
                version: Some(version.into()),
            },
        }
    }

    fn s3_replica_credentials(&self) -> (String, String, String) {
        let env = |name| std::env::var(name).ok().filter(|value| !value.is_empty());
        match &self.credentials {
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
        }
    }

    pub(crate) fn replica_config(&self, path: String) -> celld_ltx::ObjectStoreConfig {
        let (access_key_id, secret_access_key, session_token) = match self.provider {
            Provider::S3 => self.s3_replica_credentials(),
            Provider::Gcs => (String::new(), String::new(), String::new()),
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
    use std::time::SystemTime;

    fn meta(e_tag: Option<&str>, version: Option<&str>) -> ObjectMeta {
        ObjectMeta {
            location: "test".into(),
            last_modified: SystemTime::UNIX_EPOCH.into(),
            size: 0,
            e_tag: e_tag.map(Into::into),
            version: version.map(Into::into),
        }
    }

    #[test]
    fn parses_s3_and_bare_identically() {
        let bare = ObjectStorageConfig::from_bucket_uri("bucket", None, "r").unwrap();
        let prefixed =
            ObjectStorageConfig::from_bucket_uri("s3://bucket", None, "r").unwrap();
        let repeated =
            ObjectStorageConfig::from_bucket_uri("s3://s3://bucket", None, "r").unwrap();
        assert!(bare == prefixed);
        assert!(bare == repeated);
        assert_eq!(bare.enrollment_bucket(), "bucket");
    }

    #[test]
    fn rejects_empty_bucket() {
        assert!(ObjectStorageConfig::from_bucket_uri("s3://", None, "r").is_err());
    }

    #[test]
    fn parses_strict_gcs_uri_and_conflicts() {
        let gcs = ObjectStorageConfig::from_bucket_uri("gs://bucket", None, "ignored").unwrap();
        assert_eq!(gcs.scheme(), "gs");
        assert_eq!(gcs.enrollment_bucket(), "gs://bucket");
        for uri in [
            "gs://bucket/path",
            "gs://bucket/",
            "gs://bucket?x",
            "gs://bucket#x",
        ] {
            assert!(ObjectStorageConfig::from_bucket_uri(uri, None, "r").is_err());
        }
        let error =
            ObjectStorageConfig::from_bucket_uri("gs://bucket", Some("https://example"), "r")
                .err()
                .unwrap()
                .to_string();
        assert!(error.contains("--endpoint is S3-only"));
        assert!(ObjectStorageConfig::from_bucket_uri("azure://bucket", None, "r").is_err());
    }

    #[test]
    fn maps_etag_version() {
        let storage = ObjectStorageConfig::from_bucket_uri("b", None, "r").unwrap();
        let update = storage.update_version("tag");
        assert_eq!(update.e_tag.as_deref(), Some("tag"));
        assert!(update.version.is_none());
    }

    #[test]
    fn maps_gcs_generation_version() {
        let gcs = ObjectStorageConfig::from_bucket_uri("gs://b", None, "r").unwrap();
        assert_eq!(gcs.object_version(&meta(Some("ignored"), Some("42"))), "42");
        assert_eq!(
            gcs.put_result_version(PutResult {
                e_tag: Some("ignored".into()),
                version: Some("43".into()),
            }),
            "43"
        );
        let update = gcs.update_version("44");
        assert_eq!(update.version.as_deref(), Some("44"));
        assert!(update.e_tag.is_none());
    }
}
