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
- **Auto-discovery** — nodes find each other on the same LAN via mDNS with no configuration
- **Event stream** — subscribe to join / leave / update events as a Dart `Stream`
- **Cross-platform** — Windows (`.dll`), Linux (`.so`), macOS (`.dylib`)
- **Pure Dart API** — no platform channels, no code generation

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
  gossiper: ^0.1.0
```

The pre-built native libraries ship inside the package under `blobs/`.  
Copy the correct file for your platform next to your executable, or pass
`libPath:` to `Gossiper.create` to point at an arbitrary path.

```
blobs/
├── windows/gossiper.dll
├── linux/libgossiper.so
└── macos/libgossiper.dylib
```

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

  // 5. Inspect members
  for (final m in node.members()) {
    print('  • ${m.name}  ${m.address}');
  }

  // 6. Graceful shutdown
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

---

## Building the native library

The pre-built binaries are included in the published package. You only need to
rebuild if you modify the Go wrapper in `native/memberlist_wrapper/`.

**Prerequisites**

| Platform | Requirement |
|---|---|
| Windows | Go 1.21+, [tdm-gcc](https://jmeubank.github.io/tdm-gcc/) or MSYS2 mingw-w64 |
| Linux | Go 1.21+, gcc |
| macOS | Go 1.21+, `xcode-select --install` |

**Windows**

```powershell
cd native\memberlist_wrapper
go mod tidy
.\build.ps1
```

**Linux / macOS**

```bash
cd native/memberlist_wrapper
make linux   # or: make macos
```

The compiled library lands in `blobs/<platform>/`.

---

## Architecture

```
Dart API (lib/)
   └── dart:ffi
          │
   gossiper.dll / libgossiper.so / libgossiper.dylib
          │
   Go wrapper (native/memberlist_wrapper/wrapper.go)
     ├── hashicorp/memberlist   — gossip protocol
     └── hashicorp/mdns         — peer discovery
```

The Go wrapper exposes a plain C ABI via CGO. Complex data (member lists,
events, addresses) is exchanged as JSON strings. Node instances are managed
server-side and referenced from Dart by opaque `int64` handles.

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
