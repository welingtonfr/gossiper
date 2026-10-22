## 0.2.0

- Initial release.
- Gossip protocol via hashicorp/memberlist compiled as a native shared library (Windows `.dll`, Linux `.so`).
- Zero-config peer discovery via multicast DNS (mDNS) using hashicorp/mdns.
- Dart FFI bindings: `Gossiper.create`, `join`, `members`, `localNode`, `leave`, `dispose`.
- mDNS API: `startDiscovery`, `stopDiscovery`, `discoverPeers`, `joinViaDiscovery`.
- Node event stream: join / leave / update events via `Gossiper.events()`.
- **Broadcast messaging** — `broadcast(data)` queues a message to be gossiped to every cluster node; `messages()` returns a `Stream<String>` of messages received from peers.
- Fake bindings for unit testing without the native library.
