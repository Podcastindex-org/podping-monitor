# podping-monitor

Live web dashboard for the podping [Iroh](https://iroh.computer/) p2p gossip
swarm — shows swarm topology, peer history, and podping notifications as
they arrive.

> **Naming:** the repo follows the Podcastindex-org naming convention; the
> crate and binary keep their original name **`gossip-monitor`** from the
> [podping.alpha](https://github.com/Podcastindex-org/podping.alpha) R&D repo,
> where this code was developed.

## What it does

`gossip-monitor` joins the `gossipping/v1/all` gossip topic as an observer,
discovers peers via DHT and a local bootstrap list, and serves a live web UI
on port 8090 showing swarm topology, per-peer history cards, podping events,
and a swarm-management log. It re-bootstraps from known peers when the swarm
goes quiet and recycles its iroh endpoint periodically to bound memory.

## HTTP surface

| Route | Purpose |
|---|---|
| `GET /` | Dashboard UI (assets compiled into the binary — no runtime files) |
| `GET /api/swarm` | Peer/swarm state |
| `GET /api/topology` | Topology graph data |
| `GET /api/events` | Recent events |
| `GET /api/version` | Monitor version |
| `GET /api/suggestions` | Suggested bootstrap peers |

## Running with Docker

```sh
docker run -d --name podping-monitor \
  -v $(pwd)/data:/data/gossip \
  -e IROH_NODE_KEY_FILE=/data/gossip/node.key \
  -e KNOWN_PEERS_FILE=/data/gossip/known_peers.txt \
  -p 8090:8090 \
  podcastindexorg/podping-monitor:latest
```

Then open http://localhost:8090/.

## Building from source

```sh
cargo build --release --locked -p gossip-monitor
./target/release/gossip-monitor
```

The workspace vendors `dtt/`, a fork of
[distributed-topic-tracker](https://crates.io/crates/distributed-topic-tracker)
0.2.8 (MIT, © Zacharias Boehler) with local modifications to peer management
and memory behavior.

`Cargo.lock` pins pre-release transitive deps that ed25519-dalek 3.0.0-pre.1
requires (`ed25519 3.0.0-rc.4`, `pkcs8 0.11.0-rc.11`). Always build
`--locked`; do not re-resolve these.

## Configuration

All configuration is via environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `BOOTSTRAP_PEER_IDS` | 5 podping.cloud writer nodes | Comma-separated iroh node IDs to join directly, alongside DHT discovery. Defaults to the stable podping.cloud writer nodes for fast joins; set your own list to override, or an empty string for DHT-only |
| `IROH_NODE_KEY_FILE` | `gossip_monitor_node.key` | Iroh transport key (created if missing) |
| `KNOWN_PEERS_FILE` | `gossip_monitor_known_peers.txt` | Learned-peer cache for DHT-less restarts (max 15) |
| `DHT_INITIAL_SECRET` | `podping_gossip_default_secret` | Shared secret for DHT topic discovery |
| `WEB_BIND_ADDR` | `0.0.0.0:8090` | Web UI listen address |

## Releases

Tagging `vX.Y.Z` publishes `podcastindexorg/podping-monitor:X.Y.Z` and
`:latest` to Docker Hub via GitHub Actions.
