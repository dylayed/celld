//! Runs the generic `run_client_suite` against `ObjectStoreClient` backed by a
//! real Google Cloud Storage bucket.
//!
//! GCS is opt-in. Set `CELLD_GCS_LIVE=1` and `CELLD_GCS_BUCKET=gs://BUCKET` to
//! run the test; otherwise it prints a SKIP note and returns without accessing
//! a provider.

#![cfg(all(feature = "s3", feature = "gcs"))]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use celld_ltx::client::object_store::{ObjectStoreClient, ObjectStoreConfig};
use celld_ltx::client::{run_client_suite, ReplicaClient};
use celld_ltx::object_store::gcp::GoogleCloudStorageBuilder;

fn unique_path() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("celld-live/ltx/{nanos}")
}

#[tokio::test]
async fn object_store_passes_conformance_suite_vs_gcs() {
    if std::env::var("CELLD_GCS_LIVE").as_deref() != Ok("1") {
        eprintln!(
            "SKIP object_store_passes_conformance_suite_vs_gcs: \
             set CELLD_GCS_LIVE=1 and CELLD_GCS_BUCKET=gs://BUCKET"
        );
        return;
    }

    let bucket_uri = std::env::var("CELLD_GCS_BUCKET").expect("CELLD_GCS_BUCKET");
    let bucket = bucket_uri
        .strip_prefix("gs://")
        .filter(|bucket| !bucket.is_empty() && !bucket.contains(['/', '?', '#']))
        .expect("CELLD_GCS_BUCKET must be gs://BUCKET");
    let mut builder = GoogleCloudStorageBuilder::new();
    if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        if !path.is_empty() {
            builder = builder.with_application_credentials(path);
        }
    }
    let store = builder
        .with_bucket_name(bucket)
        .build()
        .expect("build GCS object store");
    let client = ObjectStoreClient::with_store(
        ObjectStoreConfig {
            bucket: bucket.into(),
            path: unique_path(),
            ..Default::default()
        },
        Arc::new(store),
    );

    client
        .init()
        .await
        .expect("init ObjectStoreClient against GCS");
    run_client_suite(&client).await;
    client.delete_all().await.expect("final cleanup");
}
