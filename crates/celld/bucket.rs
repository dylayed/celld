// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The engine's object-store client: the `object_store` crate `celld-ltx`
//! already links, bound to one bucket (wiki/designs/s3-client-dedup.md).
//! Replaces aws-sdk-s3. No call site streamed a body, so everything is
//! in-memory `Bytes`.
//!
//! Error contract, relied on by the self-fence: `put_cas` answers
//! `Ok(None)` only for a definite precondition rejection; every other failure
//! is ambiguous — the write may have committed — and surfaces as `Err`.

use crate::storage_backend::ObjectStorageConfig;
use anyhow::anyhow;
use anyhow::Context;
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::Attribute;
use object_store::Attributes;
use object_store::ClientOptions;
use object_store::Error;
use object_store::GetOptions;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMode;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::RetryConfig;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

/// One configured object-storage bucket. Cheap to clone; each `open` builds
/// its own HTTP transport, so a dedicated instance also isolates its traffic.
#[derive(Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    /// Conditional writes only, built with retries OFF: a retried CAS put
    /// can observe the first attempt's object-version change and report a
    /// definite precondition rejection — converting "may have committed" into
    /// a false rejection. The ambiguity must surface as `Err` so the caller
    /// reconciles.
    cas_store: Arc<dyn ObjectStore>,
    /// Bucket name, for messages — the store is already bound to it.
    pub name: String,
    storage_config: ObjectStorageConfig,
}

impl Bucket {
    /// `app` labels this client's traffic in the User-Agent (the aws
    /// AppName format, `app/<name>`), keeping e.g. the lease safety lane
    /// observable in black-box storage traces.
    pub fn open(
        storage_config: ObjectStorageConfig,
        app: Option<&str>,
    ) -> anyhow::Result<Bucket> {
        // These bounds mirror the aws-sdk TimeoutConfig they replace
        // (connect 3 s / attempt 15 s / operation 30 s) — a correctness
        // condition for the node self-fence, not tuning. The read-timeout
        // knob collapses into the per-request bound.
        let mut options = ClientOptions::new()
            .with_timeout(Duration::from_secs(15))
            .with_connect_timeout(Duration::from_secs(3))
            .with_allow_http(true);
        if let Some(app) = app {
            options = options.with_user_agent(
                hyper::header::HeaderValue::from_str(&format!("celld app/{app}"))
                    .context("app user agent")?,
            );
        }
        let retry = |max_retries| RetryConfig {
            max_retries,
            retry_timeout: Duration::from_secs(30),
            ..RetryConfig::default()
        };
        let ordinary_retry = retry(2);
        let cas_retry = retry(0);
        let (store, cas_store) = storage_config
            .build_bucket_stores(options, ordinary_retry, cas_retry)
            .map_err(|e| anyhow!(e))?;
        let clean_name = storage_config.bucket().to_string();
        Ok(Bucket {
            store,
            cas_store,
            name: clean_name,
            storage_config,
        })
    }

    pub(crate) fn object_uri(&self, key: &str) -> String {
        self.storage_config.object_uri(key)
    }

    pub(crate) fn uri(&self) -> String {
        self.storage_config.uri()
    }

    pub(crate) fn scheme(&self) -> &'static str {
        self.storage_config.scheme()
    }

    /// Body and object version, or `None` when the key does not exist.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>> {
        match self.store.get(&Path::from(key)).await {
            Ok(result) => {
                let version = self.storage_config.object_version(&result.meta);
                let bytes = result
                    .bytes()
                    .await
                    .with_context(|| format!("read body {}", self.object_uri(key)))?;
                Ok(Some((bytes, version)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!("read {}", self.object_uri(key)))),
        }
    }

    /// Size and object version, or `None` when the key does not exist.
    pub async fn head(&self, key: &str) -> anyhow::Result<Option<(u64, String)>> {
        match self.store.head(&Path::from(key)).await {
            Ok(meta) => {
                let version = self.storage_config.object_version(&meta);
                Ok(Some((meta.size as u64, version)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!("head {}", self.object_uri(key)))),
        }
    }

    pub async fn put(&self, key: &str, body: impl Into<PutPayload>) -> anyhow::Result<()> {
        self.store
            .put(&Path::from(key), body.into())
            .await
            .with_context(|| format!("write {}", self.object_uri(key)))?;
        Ok(())
    }

    /// Size plus one user-metadata value, or `None` when the key does not
    /// exist. A plain `head` cannot see user metadata; this one can.
    pub async fn head_with_meta(
        &self,
        key: &str,
        name: &str,
    ) -> anyhow::Result<Option<(u64, Option<String>)>> {
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self.store.get_opts(&Path::from(key), options).await {
            Ok(result) => {
                let value = result
                    .attributes
                    .get(&Attribute::Metadata(name.to_string().into()))
                    .map(|value| value.as_ref().to_string());
                Ok(Some((result.meta.size as u64, value)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!("head {}", self.object_uri(key)))),
        }
    }

    /// Plain write carrying user metadata.
    pub async fn put_with_meta(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        meta: &[(&'static str, &str)],
    ) -> anyhow::Result<()> {
        let mut attributes = Attributes::new();
        for (name, value) in meta {
            attributes.insert(
                Attribute::Metadata(Cow::Borrowed(name)),
                value.to_string().into(),
            );
        }
        let options = PutOptions {
            attributes,
            ..PutOptions::default()
        };
        self.store
            .put_opts(&Path::from(key), body.into(), options)
            .await
            .with_context(|| format!("write {}", self.object_uri(key)))?;
        Ok(())
    }

    /// Conditional write. `version: None` requires the key to be absent;
    /// `Some` requires the current provider object version.
    /// `Ok(Some(new_version))` applied, `Ok(None)` cleanly rejected; any other
    /// failure is ambiguous and stays an error.
    pub async fn put_cas(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        version: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let mode = match version {
            None => PutMode::Create,
            Some(version) => PutMode::Update(self.storage_config.update_version(version)),
        };
        match self
            .cas_store
            .put_opts(&Path::from(key), body.into(), PutOptions::from(mode))
            .await
        {
            Ok(result) => {
                let new_version = self.storage_config.put_result_version(result);
                Ok(Some(new_version))
            }
            Err(Error::Precondition { .. } | Error::AlreadyExists { .. }) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!(
                "conditional write {} may have committed",
                self.object_uri(key)
            ))),
        }
    }

    /// Idempotent: deleting an absent key succeeds.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self.store.delete(&Path::from(key)).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(anyhow!(error).context(format!("delete {}", self.object_uri(key)))),
        }
    }

    /// Every object under `prefix/`; the client paginates internally.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<ObjectMeta>> {
        let path = Path::from(prefix.trim_end_matches('/'));
        let mut stream = self.store.list(Some(&path));
        let mut objects = Vec::new();
        while let Some(meta) = stream.next().await {
            objects.push(meta.with_context(|| format!("list {}", self.object_uri(prefix)))?);
        }
        Ok(objects)
    }

    /// Does anything exist under `prefix/`? One page at most.
    pub async fn list_any(&self, prefix: &str) -> anyhow::Result<bool> {
        let path = Path::from(prefix.trim_end_matches('/'));
        match self.store.list(Some(&path)).next().await {
            None => Ok(false),
            Some(Ok(_)) => Ok(true),
            Some(Err(error)) => {
                Err(anyhow!(error).context(format!("list {}", self.object_uri(prefix))))
            }
        }
    }

    /// Immediate child "directories" under `prefix/` (delimiter listing),
    /// as full prefixes with the trailing slash stripped.
    pub async fn common_prefixes(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let path = Path::from(prefix.trim_end_matches('/'));
        let result = self
            .store
            .list_with_delimiter(Some(&path))
            .await
            .with_context(|| format!("list {}", self.object_uri(prefix)))?;
        Ok(result
            .common_prefixes
            .into_iter()
            .map(|p| p.as_ref().to_string())
            .collect())
    }

    /// The head_bucket replacement: prove the bucket is reachable and the
    /// credential is accepted with one list page.
    pub async fn validate(&self) -> anyhow::Result<()> {
        match self.store.list(None).next().await {
            None | Some(Ok(_)) => Ok(()),
            Some(Err(error)) => Err(anyhow!(error).context(format!("validate {}", self.uri()))),
        }
    }
}

/// Was this a 401/403 — the credential itself rejected? Used by the managed
/// path to report a revoked credential rather than a flaky bucket.
pub fn is_unauthorized(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if matches!(
            cause.downcast_ref::<Error>(),
            Some(Error::PermissionDenied { .. } | Error::Unauthenticated { .. })
        ) {
            return true;
        }
        // The list path wraps HTTP errors as Generic; the status only
        // survives in the retry error's message.
        let text = cause.to_string();
        text.contains("status 403") || text.contains("status 401")
    })
}

#[cfg(test)]
mod live_cas {
    use super::Bucket;

    // Live CAS contract against the selected provider. Gated on
    // CELLD_CAS_LIVE=1 so it never reaches a provider in CI; a mock cannot
    // answer whether object_store maps that provider's precondition failures
    // to Ok(None) (the fencing contract) rather than Err.
    // S3-compatible example:
    //   CELLD_CAS_LIVE=1 CELLD_CAS_BUCKET=<b> CELLD_CAS_ENDPOINT=<ep> AWS_*=... \
    //     cargo test -p celld put_cas_contract -- --nocapture
    #[tokio::test]
    async fn put_cas_contract_against_real_bucket() {
        if std::env::var("CELLD_CAS_LIVE").as_deref() != Ok("1") {
            return;
        }
        let name = std::env::var("CELLD_CAS_BUCKET").expect("CELLD_CAS_BUCKET");
        let endpoint = std::env::var("CELLD_CAS_ENDPOINT").ok();
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".into());
        let storage = crate::fleet::normalize_storage(&name, endpoint.as_deref(), &region, None)
            .expect("normalize storage");
        let bucket = Bucket::open(storage, Some("cas-test")).expect("open bucket");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = format!("cas-probe/{nanos}");

        // 1. Create on an absent key applies.
        let e1 = bucket
            .put_cas(&key, b"v1".to_vec(), None)
            .await
            .expect("create must not error")
            .expect("fresh create must apply (Ok(Some))");
        // 2. Create over an existing key is cleanly rejected.
        assert!(
            bucket
                .put_cas(&key, b"v1b".to_vec(), None)
                .await
                .expect("create-again must not error")
                .is_none(),
            "create over an existing key must be Ok(None)"
        );
        // 3. Update with the current object version applies.
        bucket
            .put_cas(&key, b"v3".to_vec(), Some(&e1))
            .await
            .expect("update must not error")
            .expect("update with current object version must apply (Ok(Some))");
        // 4. Update with the now-stale version is cleanly rejected — the fencing case.
        assert!(
            bucket
                .put_cas(&key, b"v4".to_vec(), Some(&e1))
                .await
                .expect("stale update must not error")
                .is_none(),
            "update with a stale object version must be Ok(None) — the fencing contract"
        );
        bucket.delete(&key).await.expect("cleanup delete");
        eprintln!("CAS verified on {name}: create / reject-create / update / reject-stale");
    }
}
