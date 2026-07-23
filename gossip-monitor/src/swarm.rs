use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Threshold in seconds before a peer is considered stale.
const STALE_THRESHOLD_SECS: u64 = 600;

/// Threshold in seconds before a peer is pruned entirely.
const PRUNE_THRESHOLD_SECS: u64 = 1800;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PeerAnnounce {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub node_id: String,
    pub version: String,
    pub timestamp: u64,
    #[serde(default)]
    pub sender: Option<String>,
    #[serde(default)]
    pub node_list: Option<Vec<String>>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub friendly_name: Option<String>,
    #[serde(default)]
    pub cpu_percent: Option<f32>,
    #[serde(default)]
    pub memory_mb: Option<u64>,
    #[serde(default)]
    pub thread_count: Option<u32>,
    #[serde(default)]
    pub neighbor_count: Option<u32>,
    #[serde(default)]
    pub uptime_secs: Option<u64>,
    #[serde(default)]
    pub msgs_received: Option<u64>,
    #[serde(default)]
    pub msgs_sent: Option<u64>,
    #[serde(default)]
    pub last_msg_age_secs: Option<u64>,
    #[serde(default)]
    pub reconnect_count: Option<u64>,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub build_type: Option<String>,
    #[serde(default)]
    pub neighbors: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Metric history
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct MetricSample {
    pub timestamp: u64,
    pub cpu_percent: Option<f32>,
    pub memory_mb: Option<u64>,
}

/// Maximum number of metric samples retained per peer (~5 hours at 5-min intervals).
const MAX_HISTORY_SAMPLES: usize = 60;

/// Maximum number of suggestion log entries retained.
const MAX_SUGGESTION_LOG: usize = 200;

// ---------------------------------------------------------------------------
// Suggestion log
// ---------------------------------------------------------------------------

/// A log entry recording a peer suggestion sent by the monitor.
#[derive(Debug, Clone, Serialize)]
pub struct SuggestionLogEntry {
    pub timestamp: u64,
    pub target_node_id: String,
    pub target_name: Option<String>,
    pub suggested_peers: Vec<String>,
    pub reason: String,
}

/// Thread-safe suggestion log.
pub struct SuggestionLog {
    entries: std::sync::Mutex<Vec<SuggestionLogEntry>>,
}

impl SuggestionLog {
    pub fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, entry: SuggestionLogEntry) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(entry);
        if entries.len() > MAX_SUGGESTION_LOG {
            entries.remove(0);
        }
    }

    pub fn entries(&self) -> Vec<SuggestionLogEntry> {
        self.entries.lock().unwrap().clone()
    }
}

// ---------------------------------------------------------------------------
// Snapshot types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct PeerState {
    pub node_id: String,
    pub friendly_name: Option<String>,
    pub version: String,
    pub last_seen: u64,
    pub stale: bool,
    pub cpu_percent: Option<f32>,
    pub memory_mb: Option<u64>,
    pub thread_count: Option<u32>,
    pub neighbor_count: Option<u32>,
    pub uptime_secs: Option<u64>,
    pub msgs_received: Option<u64>,
    pub msgs_sent: Option<u64>,
    pub last_msg_age_secs: Option<u64>,
    pub reconnect_count: Option<u64>,
    pub os: Option<String>,
    pub build_type: Option<String>,
    pub neighbors: Vec<String>,
    #[serde(default)]
    pub history: Vec<MetricSample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopologyEdge {
    pub source: String,
    pub target: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SwarmSnapshot {
    pub peers: Vec<PeerState>,
    pub edges: Vec<TopologyEdge>,
    pub total_peers: usize,
    pub active_peers: usize,
    pub stale_peers: usize,
    pub timestamp: u64,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub struct PeerRegistry {
    peers: RwLock<HashMap<String, PeerState>>,
    history: RwLock<HashMap<String, Vec<MetricSample>>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            history: RwLock::new(HashMap::new()),
        }
    }

    /// Update (or insert) a peer from an incoming announce message.
    pub fn update(&self, announce: &PeerAnnounce) {
        if announce.msg_type != "peer_announce" {
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = PeerState {
            node_id: announce.node_id.clone(),
            friendly_name: announce.friendly_name.clone(),
            version: announce.version.clone(),
            last_seen: now,
            stale: false,
            cpu_percent: announce.cpu_percent,
            memory_mb: announce.memory_mb,
            thread_count: announce.thread_count,
            neighbor_count: announce.neighbor_count,
            uptime_secs: announce.uptime_secs,
            msgs_received: announce.msgs_received,
            msgs_sent: announce.msgs_sent,
            last_msg_age_secs: announce.last_msg_age_secs,
            reconnect_count: announce.reconnect_count,
            os: announce.os.clone(),
            build_type: announce.build_type.clone(),
            neighbors: announce.neighbors.clone().unwrap_or_default(),
            history: Vec::new(), // populated in snapshot()
        };

        // Record a metric sample when we have cpu or memory data
        if announce.cpu_percent.is_some() || announce.memory_mb.is_some() {
            let sample = MetricSample {
                timestamp: now,
                cpu_percent: announce.cpu_percent,
                memory_mb: announce.memory_mb,
            };
            let mut hist = self.history.write().unwrap();
            let samples = hist.entry(announce.node_id.clone()).or_default();
            samples.push(sample);
            if samples.len() > MAX_HISTORY_SAMPLES {
                let excess = samples.len() - MAX_HISTORY_SAMPLES;
                samples.drain(..excess);
            }
        }

        let mut map = self.peers.write().unwrap();
        map.insert(announce.node_id.clone(), state);
    }

    /// Build a point-in-time snapshot of the swarm.
    pub fn snapshot(&self) -> SwarmSnapshot {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut map = self.peers.write().unwrap();

        // Mark stale peers
        for peer in map.values_mut() {
            peer.stale = now.saturating_sub(peer.last_seen) > STALE_THRESHOLD_SECS;
        }

        // Populate history from the separate history map
        let hist = self.history.read().unwrap();
        for peer in map.values_mut() {
            if let Some(samples) = hist.get(&peer.node_id) {
                peer.history = samples.clone();
            }
        }

        let mut peers: Vec<PeerState> = map.values().cloned().collect();
        peers.sort_by(|a, b| {
            let name_a = a.friendly_name.as_deref().unwrap_or(&a.node_id);
            let name_b = b.friendly_name.as_deref().unwrap_or(&b.node_id);
            name_a.to_lowercase().cmp(&name_b.to_lowercase())
        });

        // Build deduplicated topology edges
        let mut edge_set: HashSet<(String, String)> = HashSet::new();
        for peer in &peers {
            for neighbor in &peer.neighbors {
                let pair = if peer.node_id < *neighbor {
                    (peer.node_id.clone(), neighbor.clone())
                } else {
                    (neighbor.clone(), peer.node_id.clone())
                };
                edge_set.insert(pair);
            }
        }
        let mut edges: Vec<TopologyEdge> = edge_set
            .into_iter()
            .map(|(source, target)| TopologyEdge { source, target })
            .collect();
        edges.sort_by(|a, b| (&a.source, &a.target).cmp(&(&b.source, &b.target)));

        let total_peers = peers.len();
        let stale_peers = peers.iter().filter(|p| p.stale).count();
        let active_peers = total_peers - stale_peers;

        SwarmSnapshot {
            peers,
            edges,
            total_peers,
            active_peers,
            stale_peers,
            timestamp: now,
        }
    }

    /// Remove peers not seen within the prune threshold. Returns count removed.
    pub fn prune_stale(&self) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut map = self.peers.write().unwrap();
        let before = map.len();
        map.retain(|_, peer| now.saturating_sub(peer.last_seen) <= PRUNE_THRESHOLD_SECS);
        let removed = before - map.len();

        // Also prune history for removed peers
        if removed > 0 {
            let mut hist = self.history.write().unwrap();
            hist.retain(|id, _| map.contains_key(id));
        }

        removed
    }
}
