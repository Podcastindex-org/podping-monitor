mod swarm;
mod topology;
mod web;

use crate::swarm::PeerAnnounce;
use crate::swarm::PeerRegistry;
use ed25519_dalek::{Signer, SigningKey};
use futures_lite::StreamExt;
use iroh::protocol::Router;
use iroh::SecretKey;
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::net::Gossip;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PeerSuggest {
    #[serde(rename = "type")]
    msg_type: String,
    sender: String,
    target_node_id: String,
    suggested_peers: Vec<String>,
    reason: String,
    timestamp: u64,
    signature: String,
}

#[derive(serde::Serialize)]
struct CanonicalPeerSuggest<'a> {
    reason: &'a str,
    sender: &'a str,
    suggested_peers: &'a Vec<String>,
    target_node_id: &'a str,
    timestamp: u64,
    #[serde(rename = "type")]
    msg_type: &'a str,
}

const TOPIC_STRING: &str = "gossipping/v1/all";
// First 32 bytes of SHA-512(TOPIC_STRING) — the same derivation dtt's
// RecordTopic used. Must never change: this is the wire-level gossip topic
// the deployed fleet is subscribed to.
const TOPIC_ID_BYTES: [u8; 32] = [
    0xd2, 0x50, 0x50, 0x12, 0x44, 0x19, 0xf4, 0xc5, 0x71, 0x18, 0xc5, 0x99,
    0xdc, 0xb8, 0x98, 0x53, 0x22, 0x42, 0xa2, 0x30, 0x62, 0xab, 0xc4, 0x24,
    0x80, 0x17, 0x01, 0x37, 0x7f, 0x36, 0xc8, 0xb5,
];
const DEFAULT_NODE_KEY_FILE: &str = "gossip_monitor_node.key";
const DEFAULT_KNOWN_PEERS_FILE: &str = "gossip_monitor_known_peers.txt";
const MAX_KNOWN_PEERS: usize = 15;
// Stable podping.cloud writer nodes, used when BOOTSTRAP_PEER_IDS is unset.
// Set BOOTSTRAP_PEER_IDS to override, or to an empty string to rely on
// inbound connections and the known-peers file only.
const DEFAULT_BOOTSTRAP_PEER_IDS: &str = concat!(
    "85db0701f52fddbe251734cff653aeed22e1b7f2dce4a190981afcb74df84b0e,",
    "8fd0624d8373fa42bbebe9dca14dcb36c2973aa78bac542e595660d68a594c8b,",
    "8f96a85f84a72dfea7a8730c165f797fe30427670560c05d43103b43deb30062,",
    "6cbec939629fde22956ff9c24b77388c9130e0419f878ac2d11c6d32a159d4b3,",
    "156a91eaa4266d0819c12c7ef80501d117932ebad9e069c1d77fe9012c77e52d"
);
const DEFAULT_WEB_BIND_ADDR: &str = "0.0.0.0:8090";
const REBOOTSTRAP_TIMEOUT: u64 = 180;
const REJOIN_INTERVAL_SECS: u64 = 1800;
const RECONNECT_AFTER_FAILURES: u64 = 5;
const RECONNECT_SHUTDOWN_TIMEOUT_SECS: u64 = 10; // Cap on old gossip actor shutdown during reconnect
const RECONNECT_JOIN_TIMEOUT_SECS: u64 = 60;     // Cap on gossip re-join during reconnect; a hung join must not wedge the reconnect task
const BROADCAST_TIMEOUT_SECS: u64 = 10;
const JOIN_PEERS_TIMEOUT_SECS: u64 = 10;
const ISOLATION_CHECK_INTERVAL_SECS: u64 = 300;
const ISOLATION_MIN_UNIQUE_PEERS: usize = 3;
const ENDPOINT_RESET_AFTER_RECONNECTS: u32 = 3;
const PERIODIC_RESET_INTERVAL_SECS: u64 = 12 * 3600; // Recycle iroh endpoint every 12h to bound memory growth
const RSS_CEILING_BYTES: u64 = 1024 * 1024 * 1024;   // 1 GB RSS ceiling — safety valve for endpoint recycle

/// Read process resident set size in bytes. Returns 0 on non-Linux or read failure.
fn read_rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()) {
                return pages * 4096;
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    { 0 }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Non-blocking tracing to stderr
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stderr());
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    println!("gossip-monitor v{}", env!("CARGO_PKG_VERSION"));

    // Load config from env vars
    let node_key_file =
        env::var("IROH_NODE_KEY_FILE").unwrap_or_else(|_| DEFAULT_NODE_KEY_FILE.to_string());
    let peers_file =
        env::var("KNOWN_PEERS_FILE").unwrap_or_else(|_| DEFAULT_KNOWN_PEERS_FILE.to_string());
    let web_bind_addr: SocketAddr = env::var("WEB_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_WEB_BIND_ADDR.to_string())
        .parse()
        .expect("Invalid WEB_BIND_ADDR");
    let bootstrap_peer_ids_str = env::var("BOOTSTRAP_PEER_IDS")
        .unwrap_or_else(|_| DEFAULT_BOOTSTRAP_PEER_IDS.to_string());

    // Load or create node key
    let node_key = load_or_create_node_key(&node_key_file)?;
    let node_key_bytes = node_key.to_bytes();

    // Create iroh Endpoint
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .secret_key(node_key)
        .bind()
        .await?;
    let my_node_id = endpoint.id();
    println!("  Node ID: {}", my_node_id);

    // Create Gossip
    let gossip = Gossip::builder()
        .max_message_size(65536)
        .spawn(endpoint.clone());

    // Create Router
    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    let signing_key = SigningKey::from_bytes(&node_key_bytes);
    println!(
        "  Monitor pubkey: {}",
        hex::encode(signing_key.verifying_key().to_bytes())
    );

    println!("  Topic: \"{}\"", TOPIC_STRING);
    let topic_id = iroh_gossip::proto::TopicId::from(TOPIC_ID_BYTES);
    println!("  Topic ID: {}", hex::encode(TOPIC_ID_BYTES));

    let bootstrap_peers = gather_bootstrap_peers(&bootstrap_peer_ids_str, &peers_file, my_node_id);
    if bootstrap_peers.is_empty() {
        println!("  Warning: no bootstrap peers configured — waiting for inbound connections only.");
    } else {
        println!("  Subscribing with {} bootstrap peers...", bootstrap_peers.len());
    }
    let topic = gossip.subscribe(topic_id, bootstrap_peers).await?;
    let (gossip_sender, gossip_receiver) = topic.split();
    println!("  Subscribed to gossip topic.");

    // Shared state for resilience
    let shared_sender: Arc<tokio::sync::RwLock<GossipSender>> =
        Arc::new(tokio::sync::RwLock::new(gossip_sender));
    let shared_gossip: Arc<tokio::sync::RwLock<Gossip>> =
        Arc::new(tokio::sync::RwLock::new(gossip));
    let broadcast_failures = Arc::new(AtomicU64::new(0));
    let notifications_received = Arc::new(AtomicU64::new(0));
    let reconnect_count = Arc::new(AtomicU64::new(0));
    let neighbor_count = Arc::new(AtomicU32::new(0));
    let neighbor_ids: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));
    let unique_sources: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let reconnect_requested = Arc::new(AtomicBool::new(false));
    let reconnect_notify = Arc::new(Notify::new());
    let force_endpoint_reset = Arc::new(AtomicBool::new(false));
    let receive_generation = Arc::new(AtomicU64::new(0));

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let last_notification_time = Arc::new(AtomicU64::new(now_secs));

    // Create registry and start web server
    let registry = Arc::new(PeerRegistry::new());
    let topology_analysis: Arc<tokio::sync::RwLock<Option<topology::TopologyAnalysis>>> =
        Arc::new(tokio::sync::RwLock::new(None));
    let suggestion_log = Arc::new(swarm::SuggestionLog::new());
    let sse_tx = web::start_web_server(web_bind_addr, registry.clone(), topology_analysis.clone(), suggestion_log.clone());
    println!("  Web server listening on {}", web_bind_addr);

    // Periodic task: broadcast SwarmSnapshot via SSE every 5 seconds
    let sse_registry = registry.clone();
    let sse_sender = sse_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let snapshot = sse_registry.snapshot();
            if let Ok(json) = serde_json::to_string(&snapshot) {
                let _ = sse_sender.send(json);
            }
        }
    });

    // Periodic task: prune stale peers every 300 seconds
    let prune_registry = registry.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            let pruned = prune_registry.prune_stale();
            if pruned > 0 {
                println!("  Pruned {} stale peers", pruned);
            }
        }
    });

    // Periodic task: topology analysis and PeerSuggest broadcast every 60 seconds
    let topo_registry = registry.clone();
    let topo_shared_sender = shared_sender.clone();
    let topo_broadcast_failures = broadcast_failures.clone();
    let topo_signing_key = signing_key.clone();
    let topo_analysis = topology_analysis.clone();
    let topo_suggestion_log = suggestion_log.clone();
    let monitor_pubkey = hex::encode(signing_key.verifying_key().to_bytes());
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        let mut last_suggest: HashMap<String, Instant> = HashMap::new();
        loop {
            interval.tick().await;
            let snapshot = topo_registry.snapshot();
            let analysis = topology::analyze_topology(&snapshot);

            // Store analysis for web API
            {
                let mut guard = topo_analysis.write().await;
                *guard = Some(analysis.clone());
            }

            let now_instant = Instant::now();
            for suggestion in &analysis.suggestions {
                // Rate limit: skip if we suggested to this target less than 300s ago
                if let Some(last) = last_suggest.get(&suggestion.target_node_id) {
                    if now_instant.duration_since(*last).as_secs() < 300 {
                        continue;
                    }
                }

                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let msg_type = "peer_suggest".to_string();

                // Build canonical form for signing (fields in alphabetical order)
                let canonical = CanonicalPeerSuggest {
                    reason: &suggestion.reason,
                    sender: &monitor_pubkey,
                    suggested_peers: &suggestion.suggested_peers,
                    target_node_id: &suggestion.target_node_id,
                    timestamp,
                    msg_type: &msg_type,
                };
                let canonical_bytes = match serde_json::to_vec(&canonical) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("  [WARN] Failed to serialize canonical PeerSuggest: {}", e);
                        continue;
                    }
                };

                let signature = topo_signing_key.sign(&canonical_bytes);
                let signature_hex = hex::encode(signature.to_bytes());

                let msg = PeerSuggest {
                    msg_type: msg_type.clone(),
                    sender: monitor_pubkey.clone(),
                    target_node_id: suggestion.target_node_id.clone(),
                    suggested_peers: suggestion.suggested_peers.clone(),
                    reason: suggestion.reason.clone(),
                    timestamp,
                    signature: signature_hex,
                };

                match serde_json::to_vec(&msg) {
                    Ok(data) => {
                        let sender = topo_shared_sender.read().await;
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(BROADCAST_TIMEOUT_SECS),
                            sender.broadcast(data.into()),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                topo_broadcast_failures.store(0, Ordering::Relaxed);
                                // Look up friendly name for the target
                                let target_name = {
                                    let snap = topo_registry.snapshot();
                                    snap.peers.iter()
                                        .find(|p| p.node_id == suggestion.target_node_id)
                                        .and_then(|p| p.friendly_name.clone())
                                };
                                println!(
                                    "  [SUGGEST] Suggesting {} peers to {} (reason: {})",
                                    suggestion.suggested_peers.len(),
                                    target_name.as_deref().unwrap_or(&suggestion.target_node_id),
                                    suggestion.reason
                                );
                                topo_suggestion_log.push(swarm::SuggestionLogEntry {
                                    timestamp,
                                    target_node_id: suggestion.target_node_id.clone(),
                                    target_name,
                                    suggested_peers: suggestion.suggested_peers.clone(),
                                    reason: suggestion.reason.clone(),
                                });
                                last_suggest
                                    .insert(suggestion.target_node_id.clone(), now_instant);
                            }
                            Ok(Err(e)) => {
                                let count =
                                    topo_broadcast_failures.fetch_add(1, Ordering::Relaxed) + 1;
                                if count <= 3 {
                                    eprintln!(
                                        "  [WARN] Failed to broadcast PeerSuggest: {}",
                                        e
                                    );
                                }
                            }
                            Err(_) => {
                                let count =
                                    topo_broadcast_failures.fetch_add(1, Ordering::Relaxed) + 1;
                                if count <= 3 {
                                    eprintln!(
                                        "  [WARN] PeerSuggest broadcast timed out ({}s)",
                                        BROADCAST_TIMEOUT_SECS
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  [WARN] Failed to serialize PeerSuggest: {}", e);
                    }
                }
            }
        }
    });

    // --- Re-bootstrap watchdog: if no notification in REBOOTSTRAP_TIMEOUT, re-join peers ---
    {
        let watchdog_last = last_notification_time.clone();
        let watchdog_shared = shared_sender.clone();
        let watchdog_peers_file = peers_file.clone();
        let watchdog_bootstrap_str = bootstrap_peer_ids_str.clone();
        let watchdog_failures = broadcast_failures.clone();
        tokio::spawn(async move {
            let mut stall_count: u64 = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(REBOOTSTRAP_TIMEOUT)).await;
                let last = watchdog_last.load(Ordering::Relaxed);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if now - last >= REBOOTSTRAP_TIMEOUT {
                    stall_count += 1;
                    println!(
                        "[WATCHDOG] No gossip notifications for {}s, re-bootstrapping (stall #{})...",
                        now - last, stall_count
                    );

                    let mut peers: Vec<iroh::EndpointId> = watchdog_bootstrap_str
                        .split(',')
                        .filter(|s| !s.trim().is_empty())
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    let file_peers = load_known_peers(&watchdog_peers_file);
                    for p in file_peers {
                        if !peers.contains(&p) {
                            peers.push(p);
                        }
                    }

                    if peers.is_empty() {
                        println!("[WATCHDOG] No known peers to re-bootstrap with");
                    } else {
                        println!("[WATCHDOG] Re-joining {} peers...", peers.len());
                        let sender = watchdog_shared.read().await;
                        if let Err(e) = join_peers_timeout(&sender, peers).await {
                            eprintln!("[WARN] Re-bootstrap failed: {}", e);
                        }
                    }

                    // If join_peers hasn't helped after two consecutive stalls, escalate to
                    // a full reconnect
                    if stall_count >= 2 {
                        println!("[WATCHDOG] Stall persists after re-bootstrap, triggering full reconnect...");
                        watchdog_failures.store(RECONNECT_AFTER_FAILURES, Ordering::Relaxed);
                        stall_count = 0;
                    }
                } else {
                    stall_count = 0;
                }
            }
        });
    }

    // --- Periodic re-join: proactively re-join peers to prevent gossip topology drift ---
    {
        let rejoin_shared = shared_sender.clone();
        let rejoin_peers_file = peers_file.clone();
        let rejoin_bootstrap_str = bootstrap_peer_ids_str.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(REJOIN_INTERVAL_SECS)).await;

                let mut peers: Vec<iroh::EndpointId> = rejoin_bootstrap_str
                    .split(',')
                    .filter(|s| !s.trim().is_empty())
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
                let file_peers = load_known_peers(&rejoin_peers_file);
                for p in file_peers {
                    if !peers.contains(&p) {
                        peers.push(p);
                    }
                }

                if !peers.is_empty() {
                    println!(
                        "[REJOIN] Proactive re-join with {} peers to refresh gossip topology",
                        peers.len()
                    );
                    let sender = rejoin_shared.read().await;
                    if let Err(e) = join_peers_timeout(&sender, peers).await {
                        eprintln!("[WARN] Periodic re-join failed: {}", e);
                    }
                }
            }
        });
    }

    // --- Reconnection monitor: watches for consecutive broadcast failures ---
    {
        let reconnect_shutdown = shutdown_flag.clone();
        let reconnect_failures = broadcast_failures.clone();
        let reconnect_requested_flag = reconnect_requested.clone();
        let reconnect_notify_handle = reconnect_notify.clone();
        let reconnect_force_reset = force_endpoint_reset.clone();
        let reconnect_counter = reconnect_count.clone();
        let reconnect_neighbor_count = neighbor_count.clone();
        let reconnect_neighbor_ids = neighbor_ids.clone();
        let reconnect_unique_sources = unique_sources.clone();
        let reconnect_shared = shared_sender.clone();
        let reconnect_gossip_handle = shared_gossip.clone();
        let reconnect_endpoint = endpoint.clone();
        let reconnect_node_key_bytes = node_key_bytes;
        let reconnect_bootstrap_ids = bootstrap_peer_ids_str.clone();
        let reconnect_peers_file = peers_file.clone();
        let reconnect_my_node_id = my_node_id;
        let reconnect_last_notif = last_notification_time.clone();
        let reconnect_notif_count = notifications_received.clone();
        let reconnect_receive_generation = receive_generation.clone();
        let reconnect_registry = registry.clone();
        tokio::spawn(async move {
            let mut _current_router = router;
            let mut _current_endpoint = reconnect_endpoint.clone();
            let mut last_reconnect = Instant::now();
            let mut consecutive_reconnects: u32 = 0;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                    _ = reconnect_notify_handle.notified() => {}
                }
                if reconnect_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let failures = reconnect_failures.load(Ordering::Relaxed);
                let requested = reconnect_requested_flag.swap(false, Ordering::Relaxed);
                let forced = reconnect_force_reset.swap(false, Ordering::Relaxed);
                if !(failures >= RECONNECT_AFTER_FAILURES || requested || forced) {
                    // No reconnect needed — gossip is stable
                    if consecutive_reconnects > 0
                        && last_reconnect.elapsed()
                            > std::time::Duration::from_secs(ISOLATION_CHECK_INTERVAL_SECS)
                    {
                        consecutive_reconnects = 0;
                    }
                    continue;
                }
                {
                    // Reset failures immediately to prevent re-entrant reconnects
                    reconnect_failures.store(0, Ordering::Relaxed);

                    if !forced && last_reconnect.elapsed() < std::time::Duration::from_secs(30) {
                        eprintln!("[RECONNECT] Skipping — reconnect cooldown active");
                        continue;
                    }

                    consecutive_reconnects += 1;
                    if forced {
                        // Health-thread-driven recycle: force the fresh-endpoint branch
                        consecutive_reconnects = ENDPOINT_RESET_AFTER_RECONNECTS + 1;
                    }

                    // If we've reconnected multiple times without recovery, the endpoint
                    // itself may be degraded. Create a fresh one.
                    if consecutive_reconnects > ENDPOINT_RESET_AFTER_RECONNECTS {
                        eprintln!(
                            "[RECONNECT] {} consecutive reconnects without recovery — creating fresh endpoint",
                            consecutive_reconnects
                        );
                        let node_key =
                            iroh::SecretKey::from_bytes(&reconnect_node_key_bytes);
                        match iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                            .secret_key(node_key)
                            .bind()
                            .await
                        {
                            Ok(new_ep) => {
                                let old_ep = _current_endpoint.clone();
                                tokio::spawn(async move {
                                    tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        old_ep.close(),
                                    )
                                    .await
                                    .ok();
                                });
                                _current_endpoint = new_ep;
                                eprintln!("[RECONNECT] Fresh endpoint created successfully.");
                            }
                            Err(e) => {
                                eprintln!(
                                    "[RECONNECT] Failed to create fresh endpoint: {}. Reusing old one.",
                                    e
                                );
                            }
                        }
                    } else {
                        eprintln!(
                            "[RECONNECT] {} consecutive broadcast failures — reconnecting gossip topic (attempt {})...",
                            failures, consecutive_reconnects
                        );
                    }

                    // Shut down the old Gossip actor so all its internal dtt actors stop.
                    // Time-capped: a hung actor must not block the reconnect (the actor is
                    // abandoned either way once the new Gossip replaces it).
                    {
                        let old_gossip = reconnect_gossip_handle.read().await;
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(RECONNECT_SHUTDOWN_TIMEOUT_SECS),
                            old_gossip.shutdown(),
                        )
                        .await;
                    }

                    // Spawn a fresh Gossip actor on the current endpoint
                    let new_gossip = Gossip::builder()
                        .max_message_size(65536)
                        .spawn(_current_endpoint.clone());
                    // Re-register with a fresh Router
                    let new_router_builder = Router::builder(_current_endpoint.clone())
                        .accept(iroh_gossip::ALPN, new_gossip.clone());
                    _current_router = new_router_builder.spawn();
                    // Time-capped (kept from v0.11.1): subscribe is near-instant, but the
                    // cap guards a sick gossip actor/endpoint from wedging this task.
                    let bootstrap_peers = gather_bootstrap_peers(
                        &reconnect_bootstrap_ids,
                        &reconnect_peers_file,
                        reconnect_my_node_id,
                    );
                    let join_result = tokio::time::timeout(
                        std::time::Duration::from_secs(RECONNECT_JOIN_TIMEOUT_SECS),
                        new_gossip.subscribe(
                            iroh_gossip::proto::TopicId::from(TOPIC_ID_BYTES),
                            bootstrap_peers,
                        ),
                    )
                    .await;
                    match join_result {
                        Ok(Ok(new_topic)) => {
                                let (new_sender, new_receiver) = new_topic.split();
                                // Replace the shared sender and gossip handle
                                {
                                    let mut sender_guard = reconnect_shared.write().await;
                                    *sender_guard = new_sender;
                                }
                                {
                                    let mut gossip_guard =
                                        reconnect_gossip_handle.write().await;
                                    *gossip_guard = new_gossip.clone();
                                }
                                reconnect_failures.store(0, Ordering::Relaxed);

                                // Spawn a fresh receive task
                                spawn_receive_task(
                                    new_receiver,
                                    reconnect_peers_file.clone(),
                                    reconnect_my_node_id,
                                    reconnect_last_notif.clone(),
                                    reconnect_notif_count.clone(),
                                    reconnect_failures.clone(),
                                    reconnect_requested_flag.clone(),
                                    reconnect_notify_handle.clone(),
                                    reconnect_receive_generation.clone(),
                                    reconnect_receive_generation
                                        .fetch_add(1, Ordering::Relaxed)
                                        + 1,
                                    reconnect_shutdown.clone(),
                                    reconnect_neighbor_count.clone(),
                                    reconnect_neighbor_ids.clone(),
                                    reconnect_unique_sources.clone(),
                                    reconnect_registry.clone(),
                                );

                                // Reset neighbor tracking — fresh subscription starts with 0 neighbors
                                reconnect_neighbor_ids.write().unwrap().clear();
                                reconnect_neighbor_count.store(0, Ordering::Relaxed);
                                last_reconnect = Instant::now();
                                reconnect_counter.fetch_add(1, Ordering::Relaxed);
                                println!("[RECONNECT] Gossip topic reconnected successfully (bootstrap-only mode).");
                            }
                        Ok(Err(e)) => {
                            eprintln!(
                                "[RECONNECT] Failed to re-join gossip topic: {}. Will retry.",
                                e
                            );
                            reconnect_failures.store(0, Ordering::Relaxed);
                        }
                        Err(_) => {
                            eprintln!(
                                "[RECONNECT] Gossip re-join timed out after {}s. Will retry.",
                                RECONNECT_JOIN_TIMEOUT_SECS
                            );
                            reconnect_failures.store(0, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }

    // --- Isolation detection: trigger reconnect if too few unique source peers ---
    {
        let iso_unique_sources = unique_sources.clone();
        let iso_notifs = notifications_received.clone();
        let iso_failures = broadcast_failures.clone();
        let iso_requested = reconnect_requested.clone();
        let iso_notify = reconnect_notify.clone();
        let iso_shutdown = shutdown_flag.clone();
        tokio::spawn(async move {
            // Skip the first check to allow the swarm to stabilize after startup
            tokio::time::sleep(std::time::Duration::from_secs(ISOLATION_CHECK_INTERVAL_SECS))
                .await;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(ISOLATION_CHECK_INTERVAL_SECS))
                    .await;
                if iso_shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let (peer_count, notifs) = {
                    let mut sources = iso_unique_sources.lock().unwrap();
                    let count = sources.len();
                    sources.clear();
                    (count, iso_notifs.load(Ordering::Relaxed))
                };

                if notifs == 0 {
                    // Haven't received any notifications yet, skip check
                    continue;
                }

                if peer_count < ISOLATION_MIN_UNIQUE_PEERS {
                    eprintln!(
                        "[ISOLATION] Only {} unique source peers in last {}s (min: {}). \
                         Gossip topology may be isolated — triggering full reconnect.",
                        peer_count, ISOLATION_CHECK_INTERVAL_SECS, ISOLATION_MIN_UNIQUE_PEERS
                    );
                    iso_failures.store(RECONNECT_AFTER_FAILURES, Ordering::Relaxed);
                    iso_requested.store(true, Ordering::Relaxed);
                    iso_notify.notify_one();
                } else {
                    println!(
                        "[ISOLATION] Topology OK: {} unique source peers in last {}s",
                        peer_count, ISOLATION_CHECK_INTERVAL_SECS
                    );
                }
            }
        });
    }

    // --- Periodic health report ---
    {
        let h_notifs = notifications_received.clone();
        let h_failures = broadcast_failures.clone();
        let h_last_notif = last_notification_time.clone();
        let h_force_reset = force_endpoint_reset.clone();
        let h_reconnect_requested = reconnect_requested.clone();
        let h_reconnect_notify = reconnect_notify.clone();
        tokio::spawn(async move {
            let mut prev_notifs: u64 = 0;
            let mut prev_failures: u64 = 0;
            let mut last_reset = std::time::Instant::now();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let notifs = h_notifs.load(Ordering::Relaxed);
                let failures = h_failures.load(Ordering::Relaxed);
                let last_notif = h_last_notif.load(Ordering::Relaxed);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let since_last = now.saturating_sub(last_notif);
                let delta_notifs = notifs - prev_notifs;
                let delta_failures = failures.saturating_sub(prev_failures);
                let rss_bytes = read_rss_bytes();

                let status = if since_last > REBOOTSTRAP_TIMEOUT {
                    "STALLED"
                } else if delta_failures > 0 {
                    "DEGRADED"
                } else {
                    "OK"
                };

                println!(
                    "[HEALTH] {} | recv={} | bcast_failures={} | last_notif={}s ago | rss={}MB",
                    status, delta_notifs, delta_failures, since_last, rss_bytes / 1_048_576,
                );

                // Memory-bounded endpoint recycle: time-based (12h) + RSS-ceiling safety valve
                let time_elapsed = last_reset.elapsed() >= std::time::Duration::from_secs(PERIODIC_RESET_INTERVAL_SECS);
                let rss_exceeded = rss_bytes > RSS_CEILING_BYTES;
                if time_elapsed || rss_exceeded {
                    let reason = if rss_exceeded {
                        format!("RSS {}MB exceeds ceiling {}MB", rss_bytes / 1_048_576, RSS_CEILING_BYTES / 1_048_576)
                    } else {
                        format!("{}h elapsed since last reset", last_reset.elapsed().as_secs() / 3600)
                    };
                    eprintln!("[HEALTH] Triggering endpoint reset — {}", reason);
                    h_force_reset.store(true, Ordering::Relaxed);
                    h_reconnect_requested.store(true, Ordering::Relaxed);
                    h_reconnect_notify.notify_one();
                    last_reset = std::time::Instant::now();
                }

                prev_notifs = notifs;
                prev_failures = failures;
            }
        });
    }

    // Spawn the initial receive task
    println!("  Listening for gossip events... (Ctrl+C to stop)");
    let initial_receive_generation = receive_generation.fetch_add(1, Ordering::Relaxed) + 1;
    spawn_receive_task(
        gossip_receiver,
        peers_file.clone(),
        my_node_id,
        last_notification_time.clone(),
        notifications_received.clone(),
        broadcast_failures.clone(),
        reconnect_requested.clone(),
        reconnect_notify.clone(),
        receive_generation.clone(),
        initial_receive_generation,
        shutdown_flag.clone(),
        neighbor_count.clone(),
        neighbor_ids.clone(),
        unique_sources.clone(),
        registry.clone(),
    );

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    println!("\n  Shutting down...");
    shutdown_flag.store(true, Ordering::Relaxed);

    // Hard shutdown timeout — if endpoint.close() hangs, force exit after 5 seconds
    let shutdown_deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(shutdown_deadline);

    tokio::select! {
        _ = endpoint.close() => {
            println!("  Clean shutdown.");
        }
        _ = &mut shutdown_deadline => {
            eprintln!("  Shutdown timed out after 5s, forcing exit.");
            std::process::exit(0);
        }
    }

    Ok(())
}

/// Spawn an async task that processes incoming gossip events. Called on startup
/// and again after reconnection.
fn spawn_receive_task(
    mut receiver: GossipReceiver,
    peers_file: String,
    my_node_id: iroh::EndpointId,
    last_notification_time: Arc<AtomicU64>,
    notifications_received: Arc<AtomicU64>,
    reconnect_failures: Arc<AtomicU64>,
    reconnect_requested: Arc<AtomicBool>,
    reconnect_notify: Arc<Notify>,
    receive_generation_counter: Arc<AtomicU64>,
    receive_generation: u64,
    shutdown: Arc<AtomicBool>,
    neighbor_count: Arc<AtomicU32>,
    neighbor_ids: Arc<RwLock<HashSet<String>>>,
    unique_sources: Arc<Mutex<HashSet<String>>>,
    registry: Arc<PeerRegistry>,
) {
    tokio::spawn(async move {
        let my_node_id_str = my_node_id.to_string();
        loop {
            let event = match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                receiver.next(),
            ).await {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(_) => {
                    let last = last_notification_time.load(Ordering::Relaxed);
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    if now.saturating_sub(last) > REBOOTSTRAP_TIMEOUT * 2 {
                        eprintln!(
                            "[RECV] No gossip messages received for {}s — triggering reconnect",
                            now.saturating_sub(last)
                        );
                        break;
                    }
                    continue;
                }
            };
            // Stop processing if a newer receive task has been spawned (reconnect happened)
            if receive_generation_counter.load(Ordering::Relaxed) != receive_generation {
                println!(
                    "[RECV] Stale receive task (gen {}) stopping, newer generation active.",
                    receive_generation
                );
                return;
            }
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            match event {
                Ok(Event::Received(msg)) => {
                    // Track unique sources for isolation detection
                    if let Ok(mut sources) = unique_sources.lock() {
                        sources.insert(msg.delivered_from.to_string());
                    }

                    // Try to parse as PeerAnnounce
                    if let Ok(announce) = serde_json::from_slice::<PeerAnnounce>(&msg.content) {
                        if announce.msg_type == "peer_announce" {
                            registry.update(&announce);

                            // Update last notification time
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            last_notification_time.store(now, Ordering::Relaxed);
                            notifications_received.fetch_add(1, Ordering::Relaxed);

                            save_peer_if_new(&peers_file, &announce.node_id, &my_node_id_str);
                        }
                        // Ignore peer_endorse and other announce subtypes for the monitor
                    }
                    // Ignore PeerSuggest messages (the monitor doesn't act on its own suggestions)
                }
                Ok(Event::NeighborUp(node_id)) => {
                    neighbor_count.fetch_add(1, Ordering::Relaxed);
                    neighbor_ids.write().unwrap().insert(node_id.to_string());
                    println!("  [+] Neighbor up: {}", node_id);
                    save_peer_if_new(&peers_file, &node_id.to_string(), &my_node_id_str);
                }
                Ok(Event::NeighborDown(node_id)) => {
                    neighbor_count.fetch_sub(1, Ordering::Relaxed);
                    neighbor_ids.write().unwrap().remove(&node_id.to_string());
                    println!("  [-] Neighbor down: {}", node_id);
                }
                Ok(Event::Lagged) => {
                    eprintln!("  [WARN] Gossip receiver lagged");
                }
                Err(e) => {
                    eprintln!("  [WARN] Gossip receive error: {}", e);
                    break;
                }
            }
        }
        println!("[RECV] Gossip receive task ended.");
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        if receive_generation_counter.load(Ordering::Relaxed) == receive_generation {
            eprintln!("[RECONNECT] Active receive task exited — reconnecting gossip topic...");
            reconnect_failures.store(RECONNECT_AFTER_FAILURES, Ordering::Relaxed);
            reconnect_requested.store(true, Ordering::Relaxed);
            reconnect_notify.notify_one();
        }
    });
}

fn load_or_create_node_key(path: &str) -> anyhow::Result<SecretKey> {
    if Path::new(path).exists() {
        let raw = fs::read(path)?;
        // Try string-based parse first (backward compat)
        if let Ok(s) = std::str::from_utf8(&raw) {
            if let Ok(key) = s.trim().parse::<SecretKey>() {
                println!("  Loaded node key from {}", path);
                return Ok(key);
            }
        }
        // Fall back to raw 32-byte format
        let key_bytes: [u8; 32] = raw
            .try_into()
            .map_err(|_| anyhow::anyhow!("key file {} has invalid length", path))?;
        println!("  Loaded node key from {}", path);
        Ok(SecretKey::from_bytes(&key_bytes))
    } else {
        let key = SecretKey::generate();
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::write(path, key.to_bytes())?;
        println!("  Generated new node key -> {}", path);
        Ok(key)
    }
}

fn load_known_peers(path: &str) -> Vec<iroh::EndpointId> {
    let contents = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

/// Seeds from the compiled-in/env bootstrap list + known-peers file, deduplicated,
/// self excluded. Used at startup and on every reconnect.
fn gather_bootstrap_peers(
    bootstrap_ids_str: &str,
    peers_file: &str,
    my_node_id: iroh::EndpointId,
) -> Vec<iroh::EndpointId> {
    let mut peers: Vec<iroh::EndpointId> = bootstrap_ids_str
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    for p in load_known_peers(peers_file) {
        if !peers.contains(&p) {
            peers.push(p);
        }
    }
    peers.retain(|p| *p != my_node_id);
    peers
}

/// join_peers with the 10s cap the dtt wrapper used to provide internally.
async fn join_peers_timeout(
    sender: &iroh_gossip::api::GossipSender,
    peers: Vec<iroh::EndpointId>,
) -> Result<(), String> {
    match tokio::time::timeout(
        std::time::Duration::from_secs(JOIN_PEERS_TIMEOUT_SECS),
        sender.join_peers(peers),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "join_peers timed out after {}s",
            JOIN_PEERS_TIMEOUT_SECS
        )),
    }
}

fn save_peer_if_new(path: &str, node_id: &str, my_node_id: &str) {
    if node_id == my_node_id {
        return;
    }
    let mut peers: Vec<String> = fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    if peers.iter().any(|l| l == node_id) {
        return;
    }

    peers.push(node_id.to_string());

    // Evict oldest entries if over the cap
    if peers.len() > MAX_KNOWN_PEERS {
        let drain_count = peers.len() - MAX_KNOWN_PEERS;
        peers.drain(..drain_count);
    }

    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            let _ = fs::create_dir_all(parent);
        }
    }
    if let Ok(mut f) = fs::File::create(path) {
        for p in &peers {
            let _ = writeln!(f, "{}", p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn topic_id_matches_sha512_derivation() {
        let mut hasher = sha2::Sha512::new();
        hasher.update(TOPIC_STRING.as_bytes());
        let hash: [u8; 32] = hasher.finalize()[..32].try_into().unwrap();
        assert_eq!(TOPIC_ID_BYTES, hash);
    }
}
