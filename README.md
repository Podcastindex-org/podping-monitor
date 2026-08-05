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
discovers peers via a compiled-in bootstrap list and a learned known-peers
file, and serves a live web UI on port 8090 showing swarm topology, per-peer
history cards, podping events, and a swarm-management log. It re-bootstraps
from known peers when the swarm goes quiet and recycles its iroh endpoint
periodically to bound memory.

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

## Peer discovery

No DHT is used. Peer discovery is seed-based: 5 compiled-in podping.cloud
writer node IDs (overridable via `BOOTSTRAP_PEER_IDS`), persisted to
`KNOWN_PEERS_FILE` (capped at 15 entries) as new peers are seen, plus
periodic `PeerSuggest` gossip messages that let the monitor recommend
bootstrap peers to poorly-connected nodes in the swarm.

## Configuration

All configuration is via environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `BOOTSTRAP_PEER_IDS` | 5 podping.cloud writer nodes | Comma-separated iroh node IDs to join directly. Defaults to the stable podping.cloud writer nodes for fast joins; set your own list to override, or an empty string to rely solely on `KNOWN_PEERS_FILE` and inbound connections |
| `IROH_NODE_KEY_FILE` | `gossip_monitor_node.key` | Iroh transport key (created if missing) |
| `KNOWN_PEERS_FILE` | `gossip_monitor_known_peers.txt` | Learned-peer cache for fast restarts (max 15) |
| `WEB_BIND_ADDR` | `0.0.0.0:8090` | Web UI listen address |

## Releases

Tagging `vX.Y.Z` publishes `podcastindexorg/podping-monitor:X.Y.Z` and
`:latest` to Docker Hub via GitHub Actions.
