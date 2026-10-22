# gossiper

Gossip protocol for Dart and Flutter, powered by
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) compiled as a
native shared library and accessed via `dart:ffi`.  
Includes zero-config peer discovery via **multicast DNS (mDNS)** — no seed
list, no service registry, no config files.

[![pub version](https://img.shields.io/pub/v/gossiper)](https://pub.dev/packages/gossiper)
[![CI](https://github.com/welingtonfr/gossiper/actions/workflows/ci.yml/badge.svg)](https://github.com/welingtonfr/gossiper/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## Features

- **Gossip protocol** — eventual consistency, fault-tolerant cluster membership
- **Broadcast messaging** — send a message to every node in the cluster; receive messages as a `Stream<String>`
- **Auto-discovery** — nodes find each other on the same LAN via mDNS with no configuration
- **Event stream** — subscribe to join / leave / update events as a Dart `Stream`
- **Flutter FFI plugin** — native library bundled automatically by Flutter's build system; no manual `.dll` copying
- **Cross-platform** — Windows (`.dll`), Linux (`.so`)

---

## Platform support

| Platform | Library | Status |
|---|---|---|
| Windows x64 | `gossiper.dll` | ✅ |
| Linux x64 | `libgossiper.so` | ✅ |
| macOS arm64 | — | ❌ (planned) |
| Android / iOS | — | ❌ (planned) |

---

## Installation

```yaml
dependencies:
  gossiper: ^0.2.0
```

This is a **Flutter FFI plugin**. Flutter's build system automatically bundles
the correct native library (`.dll` on Windows, `.so` on Linux) alongside your
app — no manual copying required.

---

## Quick start

```dart
import 'package:gossiper/gossiper.dart';

void main() async {
  // 1. Create a node
  final node = Gossiper.create(
    name: 'my-node',       // optional — hostname used if omitted
    bindAddr: '0.0.0.0',
    bindPort: 7946,
  );

  // 2. Advertise on the LAN
  node.startDiscovery(serviceName: '_myapp._tcp');

  // 3. Discover peers and join (blocks for up to 2 s)
  final contacted = node.joinViaDiscovery(
    serviceName: '_myapp._tcp',
    discoveryTimeout: Duration(seconds: 2),
  );
  print('Joined via $contacted peer(s)');

  // 4. Listen for events
  node.events().listen((e) {
    print('[${e.type.name}] ${e.node.name} @ ${e.node.address}');
  });

  // 5. Broadcast a message to every peer
  node.broadcast('{"hello":"world"}');

  // 6. Receive messages from other nodes
  node.messages().listen((msg) => print('received: $msg'));

  // 7. Inspect members
  for (final m in node.members()) {
    print('  • ${m.name}  ${m.address}');
  }

  // 8. Graceful shutdown
  await Future.delayed(Duration(minutes: 1));
  node.leave();
  node.dispose();
}
```

---

## mDNS discovery

All nodes that call `startDiscovery` with the same `serviceName` on the same
L2 network automatically see each other — no seed list needed.

```
Node A                        Node B
  │                             │
  ├─ startDiscovery()           ├─ startDiscovery()
  │   broadcasts on mDNS        │   broadcasts on mDNS
  │                             │
  ├─ joinViaDiscovery()         ├─ joinViaDiscovery()
  │   queries mDNS → finds B    │   queries mDNS → finds A
  │   memberlist.Join(B) ───────► memberlist.Join(A)
  │                             │
  └─────────── gossip ──────────┘
```

`discoverPeers()` is synchronous and blocks for `timeout`. Run it in an
isolate if you need a non-blocking call:

```dart
final peers = await Isolate.run(
  () => node.discoverPeers(serviceName: '_myapp._tcp'),
);
```

---

## API reference

### `Gossiper.create`

```dart
static Gossiper create({
  String? name,           // node name — unique in the cluster
  String bindAddr,        // default '0.0.0.0'
  int bindPort,           // default 7946
  String? libPath,        // override library search path
})
```

### Cluster membership

| Method | Returns | Description |
|---|---|---|
| `join(addresses)` | `int` | Join via explicit `["ip:port", …]` list |
| `members()` | `List<MemberNode>` | Alive cluster members |
| `localNode()` | `MemberNode` | This node's own info |
| `numMembers()` | `int` | Count of alive members |
| `leave()` | `void` | Graceful leave with broadcast |

### Discovery

| Method | Returns | Description |
|---|---|---|
| `startDiscovery({serviceName})` | `void` | Advertise via mDNS |
| `stopDiscovery()` | `void` | Stop mDNS advertisement |
| `discoverPeers({serviceName, timeout})` | `List<String>` | Query mDNS for peers |
| `joinViaDiscovery({serviceName, discoveryTimeout})` | `int` | Discover + join in one call |

### Events

```dart
Stream<NodeEvent> events({Duration pollInterval = const Duration(milliseconds: 500)})
```

Each `NodeEvent` has:
- `type` — `NodeEventType.join`, `.leave`, or `.update`
- `node` — `MemberNode(name, addr, port)`

### Broadcast messaging

```dart
// Send a message to every node in the cluster (best-effort, eventual delivery)
node.broadcast('{"op":"set","key":"x","val":42}');

// Receive messages from other nodes
node.messages().listen((msg) => print('got: $msg'));
```

| Method | Returns | Description |
|---|---|---|
| `broadcast(data)` | `void` | Queue a message to be gossiped cluster-wide |
| `messages({pollInterval})` | `Stream<String>` | Stream of raw messages received from peers |
| `setReplayWindow({maxEntries, window})` | `void` | Configure the replay log (default: 500 entries / 24 h) |

Delivery is best-effort and unordered — use any encoding (JSON, plain text, Protobuf).

#### Message replay on reconnect

Every broadcast message is assigned a UUID and timestamp. The library keeps an
in-memory log of recent messages (up to `replayMaxEntries` entries or
`replayWindow` of history). When a node reconnects after being offline,
memberlist automatically syncs state — the reconnecting node receives all
messages it missed, deduplicated by UUID.

```dart
// Node A — sender (online the whole time)
final a = Gossiper.create(
  replayMaxEntries: 1000,
  replayWindow: Duration(hours: 8),
);
a.broadcast('{"op":"price_update","sku":"ABC","price":9.99}');

// Node B — PDV that went offline and came back
final b = Gossiper.create();
b.messages().listen((msg) {
  // receives missed messages automatically on rejoin
  print('replayed: $msg');
});
b.joinViaDiscovery(); // triggers state sync; missed messages arrive here
```

Configure the window at creation time or at any point via `setReplayWindow`:

```dart
node.setReplayWindow(maxEntries: 200, window: Duration(hours: 4));
```

---

## Building the native library

The pre-built binaries are included in the published package. You only need to
rebuild if you modify the Go wrapper in `native/memberlist_wrapper/`.

**Prerequisites**

| Platform | Requirement |
|---|---|
| Windows | Go 1.21+, [tdm-gcc](https://jmeubank.github.io/tdm-gcc/) or MSYS2 mingw-w64 |
| Linux | Go 1.21+, gcc |

**Windows**

```powershell
cd native\memberlist_wrapper
go mod tidy
.\build.ps1
```

**Linux**

```bash
cd native/memberlist_wrapper
make linux
```

The compiled library lands in `blobs/<platform>/`.

---

## Architecture

```
Dart API (lib/)
   └── dart:ffi
          │
   gossiper.dll / libgossiper.so
          │
   Go wrapper (native/memberlist_wrapper/wrapper.go)
     ├── hashicorp/memberlist   — gossip protocol
     └── hashicorp/mdns         — peer discovery
```

The Go wrapper exposes a plain C ABI via CGO. Complex data (member lists,
events, addresses) is exchanged as JSON strings. Node instances are managed
server-side and referenced from Dart by opaque `int64` handles.

---

## Roadmap

- [x] Broadcast messages across the cluster
- [x] Flutter plugin wrapper
- [x] Pre-built blobs in CI (GitHub Actions matrix: windows/linux)
- [x] Message replay on reconnect (missed-message delivery via `LocalState`/`MergeRemoteState`)
- [ ] User data / metadata per node (memberlist `Delegate`)
- [ ] macOS / Android / iOS support

---

## Contributing

Issues and PRs welcome at
[github.com/welingtonfr/gossiper](https://github.com/welingtonfr/gossiper).

---

## License

MIT — see [LICENSE](LICENSE).

This package bundles compiled versions of
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) and
[hashicorp/mdns](https://github.com/hashicorp/mdns), both under the
Mozilla Public License 2.0. Their source is available at the links above.
