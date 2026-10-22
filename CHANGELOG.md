## 0.3.0

- **Message replay** — when a node reconnects after being offline, it automatically receives messages it missed (up to a configurable window). Each broadcast message is assigned a UUID and timestamp; `LocalState`/`MergeRemoteState` sync the in-memory log between nodes on connect/reconnect. Deduplication by UUID prevents duplicate delivery.
- New API: `setReplayWindow({int maxEntries, Duration window})` — configure the replay log size (default 500 entries, 24 hours).
- New `Gossiper.create` parameters: `replayMaxEntries` and `replayWindow`.
- New Go export: `MemberlistSetReplayWindow`.

## 0.2.0

- Initial release.
- Gossip protocol via hashicorp/memberlist compiled as a native shared library (Windows `.dll`, Linux `.so`).
- Zero-config peer discovery via multicast DNS (mDNS) using hashicorp/mdns.
- Dart FFI bindings: `Gossiper.create`, `join`, `members`, `localNode`, `leave`, `dispose`.
- mDNS API: `startDiscovery`, `stopDiscovery`, `discoverPeers`, `joinViaDiscovery`.
- Node event stream: join / leave / update events via `Gossiper.events()`.
- **Broadcast messaging** — `broadcast(data)` queues a message to be gossiped to every cluster node; `messages()` returns a `Stream<String>` of messages received from peers.
- Fake bindings for unit testing without the native library.
