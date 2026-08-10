// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Production bucket deployment adapters reused by the clean-sheet host.

use crate::bucket::Bucket;
use crate::deploy;
use crate::js::WorkerConfigOptions;
use crate::storage_backend::{ObjectStorageConfig, StaticCredentials};
use crate::protocol::{DeployPointer, Manifest};
use anyhow::{bail, Context};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::info;

pub fn normalize_storage(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
) -> anyhow::Result<ObjectStorageConfig> {
    if let Some(managed) = managed {
        if let Some((scheme, _)) = bucket.split_once("://") {
            anyhow::ensure!(
                scheme == "s3",
                "managed R2 storage is S3 and does not support {scheme}://"
            );
        }
        let name = bucket.trim_start_matches("s3://");
        return ObjectStorageConfig::managed(
            name,
            managed.region.clone(),
            managed.endpoint.clone(),
            StaticCredentials {
                access_key_id: managed.access_key_id.clone(),
                secret_access_key: managed.secret_access_key.clone(),
                session_token: managed.session_token.clone(),
            },
        );
    }

    ObjectStorageConfig::from_bucket_uri(bucket, endpoint, region)
}

pub fn normalize_byo_storage(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
) -> anyhow::Result<(ObjectStorageConfig, crate::control_plane::ByoStorageConfig)> {
    let storage = normalize_storage(bucket, endpoint, region, None)?;
    let (endpoint, region) = if storage.scheme() == "gs" {
        (None, String::new())
    } else {
        (endpoint.map(Into::into), region.into())
    };
    let config = crate::control_plane::ByoStorageConfig {
        bucket: storage.enrollment_bucket(),
        endpoint,
        region,
    };
    Ok((storage, config))
}

pub fn storage_client(backend: &ObjectStorageConfig) -> anyhow::Result<Bucket> {
    Bucket::open(backend.clone(), None)
}

/// Build the authority-heartbeat client on its own HTTP connection pool.
///
/// Node lease traffic must not queue behind ordinary ownership, deployment,
/// or replica requests. Every `Bucket::open` builds its own transport, so a
/// dedicated instance keeps the safety lane isolated, and the `celld-lease`
/// app tag labels it in black-box storage traces.
pub fn lease_storage_client(backend: &ObjectStorageConfig) -> anyhow::Result<Bucket> {
    Bucket::open(backend.clone(), Some("celld-lease"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_storage_reaches_ltx_config() {
        let managed = crate::control_plane::ManagedStorageConfig {
            bucket: "managed-bucket".into(),
            endpoint: "https://managed.example".into(),
            region: "managed-region".into(),
            access_key_id: "managed-access-key".into(),
            secret_access_key: "managed-secret-key".into(),
            session_token: Some("managed-session-token".into()),
        };
        let storage = normalize_storage(&managed.bucket, None, "ignored", Some(&managed)).unwrap();
        let replica = storage.replica_config("replicas/epoch".into());

        assert_eq!(replica.bucket, "managed-bucket");
        assert_eq!(replica.path, "replicas/epoch");
        assert_eq!(replica.endpoint, "https://managed.example");
        assert_eq!(replica.region, "managed-region");
        assert_eq!(replica.access_key_id, "managed-access-key");
        assert_eq!(replica.secret_access_key, "managed-secret-key");
        assert_eq!(replica.session_token, "managed-session-token");
        assert!(replica.force_path_style);
    }

    #[test]
    fn canonicalizes_byo_storage_before_enrollment() {
        let s3_endpoint = "https://s3.example";
        let s3_region = "us-east-1";
        for bucket in ["bucket", "s3://bucket", "s3://s3://bucket"] {
            let (_, config) = normalize_byo_storage(bucket, Some(s3_endpoint), s3_region).unwrap();
            assert_eq!(config.bucket, "bucket");
            assert_eq!(config.endpoint.as_deref(), Some(s3_endpoint));
            assert_eq!(config.region, s3_region);
        }

        let irrelevant_aws_region = "us-west-2";
        let (gcs_storage, gcs_config) =
            normalize_byo_storage("gs://bucket", None, irrelevant_aws_region).unwrap();
        let canonical_gcs_storage = normalize_storage("gs://bucket", None, "", None).unwrap();
        assert!(gcs_storage == canonical_gcs_storage);
        assert_eq!(gcs_config.bucket, "gs://bucket");
        assert!(gcs_config.endpoint.is_none());
        assert!(gcs_config.region.is_empty());
        assert!(normalize_byo_storage("gs://bucket/path", None, "ignored").is_err());
        assert!(normalize_byo_storage("gs://bucket", Some(""), "ignored").is_err());
        assert!(normalize_byo_storage("azure://bucket", None, "ignored").is_err());
    }
}

pub async fn validate_bucket(bucket: &Bucket) -> anyhow::Result<()> {
    bucket
        .validate()
        .await
        .with_context(|| format!("bucket unavailable or inaccessible: {}", bucket.uri()))
}

/// Validate storage issued by the Managed Control Plane and preserve the
/// operator-visible failure vocabulary. Newly issued provider credentials can
/// take a moment to propagate, so only the final rejection is authoritative.
pub async fn validate_managed_bucket(bucket: &Bucket) -> anyhow::Result<()> {
    const RETRIES: u32 = 5;
    for attempt in 1..=RETRIES {
        match validate_managed_bucket_once(bucket, attempt == RETRIES).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt == RETRIES => return Err(error),
            Err(_) => {
                info!(
                    bucket = %bucket.name,
                    attempt,
                    "storage credential not accepted yet; retrying"
                );
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
        }
    }
    unreachable!("loop returns on the final attempt")
}

async fn validate_managed_bucket_once(bucket: &Bucket, report: bool) -> anyhow::Result<()> {
    match bucket.validate().await {
        Ok(()) => Ok(()),
        Err(error) if crate::bucket::is_unauthorized(&error) => {
            if report {
                crate::control_plane::report_managed_runtime_state(
                    crate::control_plane::ManagedRuntimeState::CredentialRevoked,
                );
                bail!(
                    "managed storage credential was rejected or revoked for {}",
                    bucket.uri()
                );
            }
            bail!(
                "managed storage credential was not accepted yet for {}",
                bucket.uri()
            );
        }
        Err(error) => {
            if report {
                crate::control_plane::report_managed_runtime_state(
                    crate::control_plane::ManagedRuntimeState::BucketUnavailable,
                );
            }
            Err(error)
                .with_context(|| format!("bucket unavailable or inaccessible: {}", bucket.uri()))
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct DiagnosticNode {
    pub(crate) node: String,
    pub(crate) expires_ms: u64,
    pub(crate) addr: String,
    #[serde(default)]
    pub(crate) probe_public_key: String,
    #[serde(default)]
    pub(crate) peer_protocol: u16,
    #[serde(default)]
    pub(crate) load: crate::ownership_store::NodeLoadWire,
}

pub async fn diagnose(
    bucket: &Bucket,
    peers: Vec<String>,
    unsafe_public_advertise: bool,
) -> anyhow::Result<()> {
    validate_bucket(bucket).await?;
    println!("ok bucket {}", bucket.uri());

    let enumerated = peers.is_empty();
    let peers = if enumerated {
        let peers = diagnostic_node_ids(bucket).await?;
        println!("ok fleet {} node lease(s) enumerated", peers.len());
        peers
    } else {
        peers
    };
    if peers.is_empty() {
        return Ok(());
    }
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build peer diagnostic client")?;
    let auth = crate::peer_auth::PeerAuth::new(
        crate::peer_auth::load_existing(bucket).await?,
        "diagnostic",
    )?;
    let mut failures = 0_usize;
    let mut expired = 0_usize;
    for peer in peers {
        let node = match diagnostic_node(bucket, &peer).await {
            Ok(Some(node)) => node,
            Ok(None) if enumerated => {
                expired += 1;
                println!("skip peer {peer}: lease is expired");
                continue;
            }
            Ok(None) => {
                failures += 1;
                eprintln!("fail peer {peer}: node {peer} lease is expired");
                continue;
            }
            Err(error) => {
                failures += 1;
                eprintln!("fail peer {peer}: {error}");
                continue;
            }
        };
        let advertise = match crate::startup::parse_advertise(&node.addr) {
            Ok(advertise) => advertise,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "fail peer {peer}: malformed advertise address {:?}: {error}",
                    node.addr
                );
                continue;
            }
        };
        if advertise.is_public_ip() && !unsafe_public_advertise {
            failures += 1;
            eprintln!(
                "fail peer {peer}: unsafe public advertise address {}; use a private overlay or --unsafe-public-advertise",
                node.addr
            );
            continue;
        }
        if let Err(error) = crate::peer_probe::probe(&http, &node, &auth).await {
            failures += 1;
            eprintln!("fail peer {peer} at {}: {error}", node.addr);
            continue;
        }
        let load_age_ms = if node.load.sampled_ms == 0 {
            "unknown".to_string()
        } else {
            crate::ownership_store::now_ms()
                .saturating_sub(node.load.sampled_ms)
                .to_string()
        };
        // A 1-byte RSS is the sentinel a platform without /proc leaves behind,
        // not a measurement. Report it the way the load age already reports a
        // missing sample, so no operator reads it as a real number.
        let rss_bytes = if node.load.rss_bytes <= 1 {
            "unknown".to_string()
        } else {
            node.load.rss_bytes.to_string()
        };
        println!(
            "ok peer {} at {} (signed direct probe) protocol={} resident_cells={} \
             websockets={} rss_bytes={} cpu_percent={:.2} fds={}/{} pressured={} \
             shed_cells={} load_age_ms={}",
            node.node,
            node.addr,
            node.peer_protocol,
            node.load.resident_cells,
            node.load.host_websockets,
            rss_bytes,
            node.load.cpu_percent_x100 as f64 / 100.0,
            node.load.open_fds,
            node.load.fd_limit,
            node.load.pressured,
            node.load.shed_cells,
            load_age_ms,
        );
    }
    if expired > 0 {
        println!("ok fleet skipped {expired} expired node lease(s)");
    }
    if failures > 0 {
        bail!("fleet diagnostics failed for {failures} peer(s)");
    }
    Ok(())
}

async fn diagnostic_node(bucket: &Bucket, peer: &str) -> anyhow::Result<Option<DiagnosticNode>> {
    let key = format!("nodes/{peer}.json");
    let node: DiagnosticNode = serde_json::from_str(&get_string(bucket, &key).await?)
        .with_context(|| format!("decode {}", bucket.object_uri(&key)))?;
    if node.node != peer {
        bail!(
            "node lease {key} identifies unexpected node {:?}",
            node.node
        );
    }
    if node.expires_ms <= crate::ownership_store::now_ms() {
        return Ok(None);
    }
    if node.addr.is_empty() {
        bail!("node {peer} lease has no advertised address");
    }
    Ok(Some(node))
}

async fn diagnostic_node_ids(bucket: &Bucket) -> anyhow::Result<Vec<String>> {
    let mut nodes = Vec::new();
    for object in bucket
        .list("nodes/")
        .await
        .context("enumerate node leases")?
    {
        let Some(node) = object
            .location
            .as_ref()
            .strip_prefix("nodes/")
            .and_then(|key| key.strip_suffix(".json"))
        else {
            continue;
        };
        if !node.is_empty() {
            nodes.push(node.to_string());
        }
    }
    nodes.sort();
    nodes.dedup();
    Ok(nodes)
}

pub async fn run_deploy(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(mut options) = deploy::options_from_arguments(arguments)? else {
        deploy::print_help();
        return Ok(());
    };
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    if options.bucket.is_none() {
        options.bucket = env("CELLD_BUCKET");
    }
    if options.endpoint.is_none() {
        options.endpoint = env("S3_ENDPOINT");
    }
    if !options.dry_run && options.bucket.is_none() {
        bail!("celld deploy requires --bucket [s3://|gs://]NAME (or CELLD_BUCKET)");
    }
    let built = deploy::build(&options)?;
    built.report();
    if options.dry_run {
        println!(
            "Current Version ID: {} (dry run; nothing written)",
            built.version
        );
        return Ok(());
    }

    let bucket = options.bucket.expect("validated deployment bucket");
    let region = options
        .region
        .or_else(|| env("AWS_REGION"))
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_string());
    let storage = normalize_storage(&bucket, options.endpoint.as_deref(), &region, None)?;
    let store = storage_client(&storage)?;
    validate_bucket(&store).await?;
    let started = std::time::Instant::now();
    deploy::write(&store, &built).await?;
    println!(
        "Uploaded {} ({:.2} sec)",
        built.script_name,
        started.elapsed().as_secs_f64()
    );
    println!("  {}", storage.object_uri(&built.prefix));
    println!("Current Version ID: {}", built.version);
    println!("Nodes load a deployment at startup; restart them to serve this version.");
    Ok(())
}

async fn get_string(bucket: &Bucket, key: &str) -> anyhow::Result<String> {
    let (bytes, _) = bucket
        .get(key)
        .await?
        .with_context(|| format!("read {}: no such key", bucket.object_uri(key)))?;
    String::from_utf8(bytes.to_vec()).context("deployment module is not UTF-8")
}

pub async fn load_current_worker(
    bucket: &Bucket,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    load_worker_from_pointer(bucket, "deploy/current.json", node).await
}

pub async fn load_named_worker(
    bucket: &Bucket,
    script: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    load_worker_from_pointer(bucket, &format!("deploy/{script}/current.json"), node).await
}

async fn load_worker_from_pointer(
    bucket: &Bucket,
    pointer_key: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    let pointer: DeployPointer = serde_json::from_str(&get_string(bucket, pointer_key).await?)
        .with_context(|| format!("decode {pointer_key}"))?;
    let manifest: Manifest = serde_json::from_str(
        &get_string(bucket, &format!("{}/manifest.json", pointer.prefix)).await?,
    )
    .context("decode deployment manifest")?;
    let src = match manifest.main_module.as_deref() {
        Some(main) => get_string(bucket, &format!("{}/{main}", pointer.prefix)).await?,
        None if manifest.assets.is_some() => {
            // Ingress is handled by the immutable asset resolver. Keeping a
            // synthetic Worker makes the runtime construction path uniform
            // and is a fail-closed guard if an asset-only request escapes it.
            "export default { fetch() { return new Response('Not found', { status: 404 }); } };"
                .to_string()
        }
        None => bail!("deployment has neither a main module nor assets"),
    };
    let mut text = Vec::new();
    for module in &manifest.modules {
        if manifest.main_module.as_deref() == Some(module.name.as_str()) {
            continue;
        }
        let source = get_string(bucket, &format!("{}/{}", pointer.prefix, module.name)).await?;
        text.push((format!("./{}", module.name), source));
    }
    let do_bindings = bindings(&manifest, "durable_object_namespace")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("class_name")?.as_str()?.to_string(),
            ))
        })
        .collect();
    let r2_bindings = bindings(&manifest, "r2_bucket")
        .filter_map(|binding| binding.get("name")?.as_str().map(str::to_string))
        .collect();
    let ai_binding = configured_ai_binding(
        bindings(&manifest, "ai")
            .find_map(|binding| binding.get("name")?.as_str().map(str::to_string)),
    );
    let services = service_bindings(&manifest);
    let vars = worker_vars(&manifest)?;
    let compat = crate::worker_compat(&manifest.raw_metadata);
    let assets = match &manifest.assets {
        Some(reference) => Some(
            crate::assets::AssetResolver::load(
                bucket,
                &pointer.prefix,
                reference,
                manifest.main_module.is_none(),
            )
            .await?,
        ),
        None => None,
    };
    let asset_binding = assets
        .as_ref()
        .and_then(crate::assets::AssetResolver::binding_name)
        .map(str::to_string);
    let script_name = manifest.script_name.clone();
    Ok(LoadedDeployment {
        options: WorkerConfigOptions {
            src,
            script_name: script_name.clone(),
            do_classes: manifest.do_classes,
            bindings: do_bindings,
            r2_bindings,
            ai_binding,
            vars,
            node,
            text,
            compat,
        },
        script_name,
        asset_binding,
        assets,
        services,
    })
}

/// Apply celld's manifest-first precedence for the optional AI binding.
pub fn configured_ai_binding(manifest_binding: Option<String>) -> Option<String> {
    manifest_binding
        .or_else(|| std::env::var("CELLD_AI_BINDING").ok())
        .or_else(|| std::env::var_os("CELLD_AI_URL").map(|_| "AI".to_string()))
}

pub struct LoadedDeployment {
    pub options: WorkerConfigOptions,
    pub script_name: String,
    pub asset_binding: Option<String>,
    pub assets: Option<crate::assets::AssetResolver>,
    pub services: Vec<(String, String, Option<String>)>,
}

fn bindings<'a>(
    manifest: &'a Manifest,
    kind: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> {
    manifest
        .raw_metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(move |binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(kind)
        })
}

fn service_bindings(manifest: &Manifest) -> Vec<(String, String, Option<String>)> {
    bindings(manifest, "service")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("service")?.as_str()?.to_string(),
                binding
                    .get("entrypoint")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ))
        })
        .collect()
}

fn worker_vars(manifest: &Manifest) -> anyhow::Result<Vec<(String, String)>> {
    let mut vars = BTreeMap::new();
    for binding in bindings(manifest, "plain_text") {
        if let (Some(name), Some(value)) = (
            binding.get("name").and_then(serde_json::Value::as_str),
            binding.get("text").and_then(serde_json::Value::as_str),
        ) {
            vars.insert(name.to_string(), value.to_string());
        }
    }
    if let Ok(path) = std::env::var("CELLD_VARS_FILE") {
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("read Worker vars file {path}"))?;
        for line in contents.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((name, raw)) = line.split_once('=') else {
                continue;
            };
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let raw = raw.trim();
            let value = raw
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    raw.strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
                .unwrap_or(raw);
            vars.insert(name.to_string(), value.to_string());
        }
    }
    for (name, value) in std::env::vars() {
        if let Some(name) = name
            .strip_prefix("CELLD_VAR_")
            .filter(|name| !name.is_empty())
        {
            vars.insert(name.to_string(), value);
        }
    }
    Ok(vars.into_iter().collect())
}
