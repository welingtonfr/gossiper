## 0.4.1

- **Bug fix** — `onJoin` no longer silenced by `processBeacon`'s `catch` block; only JSON parse errors are swallowed now, so `GossiperException` from `join()` propagates correctly.

## 0.4.0

- **Beacon discovery** — `startDiscovery()` now runs UDP broadcast alongside mDNS. Nodes automatically join the cluster as soon as any peer is found, with no extra API calls needed.
- `startDiscovery()` signature changed to `Future<void>` (was `void`); `beaconPort`, `beaconInterval`, and `beaconType` parameters added.
- New static method `Gossiper.resolveLanIp()` — resolves the best LAN IPv4 address, skipping loopback, link-local, and known virtual adapter ranges (Docker, WSL2, VirtualBox, VMware, KVM/libvirt).
- **Bug fix** — `discoverPeers`, `members`, `events`, and `messages` no longer throw when the native layer returns JSON `null` instead of `[]`.
- `BeaconUtils` extracted as an internal utility class (`subnetBroadcast`, `isVirtualInterface`, `processBeacon`).

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
