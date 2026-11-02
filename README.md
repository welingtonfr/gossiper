# gossiper

Gossip protocol for Dart and Flutter, powered by
[hashicorp/memberlist](https://github.com/hashicorp/memberlist) compiled as a
native shared library and accessed via `dart:ffi`.  
Includes zero-config peer discovery via **mDNS + UDP beacon** — no seed
list, no service registry, no config files — and pluggable interfaces for
your own discovery and message storage.

[![pub version](https://img.shields.io/pub/v/gossiper)](https://pub.dev/packages/gossiper)
[![CI](https://github.com/welingtonfr/gossiper/actions/workflows/ci.yml/badge.svg)](https://github.com/welingtonfr/gossiper/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## Features

- **Gossip protocol** — eventual consistency, fault-tolerant cluster membership
- **Broadcast messaging** — send a message to every node in the cluster; receive messages as a `Stream<Message>`
- **Message replay** — nodes that were offline receive the messages they missed, deduplicated by id
- **Pluggable discovery** — built-in **mDNS** and **UDP beacon** strategies, or implement `DiscoveryStrategy` yourself; run several at once
- **Auto-join rules** — decide per peer whether a strategy should join it automatically
- **Pluggable message storage** — keep message history across restarts with the built-in `JsonLinesMessageStore` or your own `MessageStore`
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
  gossiper: ^0.4.1
```

This is a **Flutter FFI plugin**. Flutter's build system automatically bundles
the correct native library (`.dll` on Windows, `.so` on Linux) alongside your
app — no manual copying required.

---

## Quick start

```dart
import 'package:gossiper/gossiper.dart';

void main() async {
  // 1. Resolve the LAN IP (optional — improves interface selection)
  final ip = await Gossiper.resolveLanIp();

  // 2. Create a node (messageStore is optional)
  final node = Gossiper.create(
    name: 'my-node',   // optional — hostname used if omitted
    bindAddr: ip,
    bindPort: 7946,
    messageStore: JsonLinesMessageStore('gossiper.log'),
  );

  // 3. Start discovery — mDNS + UDP beacon by default; loads the store
  //    history first, then auto-joins when a peer is found
  await node.startDiscovery();

  // 4. Listen for events
  node.events().listen((e) {
    print('[${e.type.name}] ${e.node.name} @ ${e.node.address}');
  });

  // 5. Broadcast a message to every peer
  final sent = node.broadcast('{"hello":"world"}');
  print('sent ${sent.id}');

  // 6. Receive messages from other nodes
  node.messages().listen((m) => print('received: ${m.data}'));

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

## Peer discovery

`startDiscovery([strategies])` starts every `DiscoveryStrategy` in the list
concurrently — `[MdnsDiscovery(), BeaconDiscovery()]` when omitted. The cluster forms as soon as any strategy finds a peer. Calling
it again stops the previous strategies first; `stopDiscovery()` and
`dispose()` stop them all.

### Built-in strategies

| Strategy | How it works | Default auto-join | Best for |
|---|---|---|---|
| `MdnsDiscovery({serviceName, autoJoin, lookupTimeout})` | advertises via multicast DNS and queries the LAN once on start | `AutoJoinRules.always` | fast initial join |
| `BeaconDiscovery({port, interval, beaconType, autoJoin})` | broadcasts `{"type","addr"}` on the /24 subnet every 5 s and listens for others | `AutoJoinRules.whenAlone` | late joiners, networks where multicast is filtered |

```
Node A                                  Node B
  │                                       │
  ├─ startDiscovery([mdns, beacon])       ├─ startDiscovery([mdns, beacon])
  │   mDNS advertising + lookup           │   mDNS advertising + lookup
  │   beacon broadcasting + listening     │   beacon broadcasting + listening
  │                                       │
  │   ← beacon / mDNS response from B ────┤
  │   autoJoin(B)? → join(B) ─────────────►
  │                                       │
  └──────────────── gossip ───────────────┘
```

- **mDNS lookup blocks.** The lookup is a blocking native call that lasts up to `lookupTimeout` (default 2 s), so `startDiscovery` takes that long when mDNS auto-join is on.
- **Retrying.** Keep a reference to retry later or query by hand:

```dart
final mdns = MdnsDiscovery(serviceName: '_myapp._tcp');
await node.startDiscovery([mdns, BeaconDiscovery()]);

mdns.discoverPeers();   // List<String> "ip:port", excluding this node
mdns.joinPeers();       // discover + join all, ignoring autoJoin
```

- **Beacon scope.** `beaconType` scopes the beacon to a logical cluster: nodes with different types on the same network ignore each other.

### Auto-join rules

Every built-in strategy takes an optional `autoJoin` rule, consulted before each
automatic join. Pass `null` to only advertise and decide yourself when to call
`join`.

```dart
typedef AutoJoinRule = FutureOr<bool> Function(String peerAddr, DiscoveryContext ctx);
```

```dart
await node.startDiscovery([
  // only join peers on the 192.168.1.x subnet
  MdnsDiscovery(autoJoin: (peer, ctx) => peer.startsWith('192.168.1.')),
  // join any beacon heard, even when already in a cluster
  BeaconDiscovery(autoJoin: AutoJoinRules.always),
]);

// advertise only — no automatic join
BeaconDiscovery(autoJoin: null);
```

Rules may be `async` (e.g. to check an allow-list service). Built-ins:
`AutoJoinRules.always` and `AutoJoinRules.whenAlone` (`numMembers <= 1`).

### Custom discovery

Implement `DiscoveryStrategy` to find peers any other way (static list, HTTP
registry, Consul, cloud APIs…) and mix it with the built-in ones:

```dart
class ConsulDiscovery implements DiscoveryStrategy {
  Timer? _timer;

  @override
  Future<void> start(DiscoveryContext ctx) async {
    _timer = Timer.periodic(const Duration(seconds: 5), (_) async {
      final peers = await fetchPeersFromConsul();
      final others = peers.where((p) => p != ctx.selfAddr).toList();
      if (others.isNotEmpty && ctx.numMembers <= 1) ctx.join(others);
    });
  }

  @override
  void stop() => _timer?.cancel();
}

await node.startDiscovery([MdnsDiscovery(), ConsulDiscovery()]);
```

`DiscoveryContext` gives the strategy `lanIp`, `gossipPort`, `selfAddr`
(`"lanIp:gossipPort"`), `numMembers` and `join(addresses)`. If `start` throws,
`startDiscovery` stops the strategies already started and rethrows. `stop` must
be safe to call more than once.

---

## Messages

```dart
// Send a message to every node in the cluster (best-effort, eventual delivery)
final sent = node.broadcast('{"op":"set","key":"x","val":42}');

// Receive messages from other nodes
node.messages().listen((m) => print('${m.id} @ ${m.at}: ${m.data}'));
```

Each `Message` has:
- `id` — UUID assigned by the sender, used for dedup across nodes and restarts
- `data` — the string passed to `broadcast`, untouched (use JSON, plain text, Protobuf…)
- `at` — send time in unix milliseconds

Delivery is best-effort and unordered.

### Message replay on reconnect

The library keeps an in-memory replay log of recent messages (up to
`replayMaxEntries` entries or `replayWindow` of history). When a node
reconnects after being offline, memberlist automatically syncs state: the
reconnecting node receives every message it missed on `messages()`,
deduplicated by `id`.

```dart
// Node A — online the whole time
final a = Gossiper.create(replayMaxEntries: 1000, replayWindow: Duration(hours: 8));
a.broadcast('{"op":"price_update","sku":"ABC","price":9.99}');

// Node B — went offline and came back
final b = Gossiper.create();
b.messages().listen((m) => print('replayed: ${m.data}'));  // missed messages arrive here
await b.startDiscovery([MdnsDiscovery()]);                 // join triggers the sync
```

Change the window at any point with
`node.setReplayWindow(maxEntries: 200, window: Duration(hours: 4))`.

### Message storage

The replay log lives in memory. To survive restarts (e.g. a power loss), pass
a `MessageStore` to `create` — nothing else to call:

```dart
final node = Gossiper.create(messageStore: JsonLinesMessageStore('/data/gossiper.log'));
await node.startDiscovery();
```

| When | What happens |
|---|---|
| `broadcast(data)` | the sent `Message` is saved via `saveMessage` |
| message arrives on `messages()` (live or replayed) | it is saved via `saveMessage` |
| first `startDiscovery()` | `getMessagesSince(now - replayWindow, limit: replayMaxEntries)` is loaded into the replay log — before any strategy starts |

Loading the history before joining matters twice: the node can replay it to
peers that come back, and it won't receive those messages again from peers.
The Go side never touches the filesystem — it only gets what the store returns.

Out of scope, on purpose:
- **Retrying failed sends** — `broadcast` is fire-and-forget; keep your own outbox if you need guaranteed delivery.
- **Nodes joined only with `join()`** (no `startDiscovery()`) don't load the history.
- **Retention** — the library never deletes from the store; how much to keep and when to delete is your call (`JsonLinesMessageStore` has `maxFileSizeBytes` and `deleteBefore(timestamp)` if you want them).
- **Store errors** thrown by `saveMessage` are not caught.

`JsonLinesMessageStore` writes one `{"id","data","at"}` object per line,
serializes concurrent operations, skips corrupt lines, and offers
`deleteBefore(timestamp)` for your own cleanup. To cap the file size:

```dart
// when a save makes the file exceed 5 MB, it is rewritten keeping only the
// newest messages that fit in 75% of the limit (no limit by default)
JsonLinesMessageStore('/data/gossiper.log', maxFileSizeBytes: 5 * 1024 * 1024);
``` To store messages
elsewhere (SQLite, Hive, a remote service…), implement `MessageStore`:

```dart
abstract interface class MessageStore {
  Future<void> saveMessage(Message message);
  Future<List<Message>> getMessagesSince(int afterTimestamp, {int? limit}); // oldest first; limit = most recent N
}
```

---

## API reference

### `Gossiper.create`

```dart
static Gossiper create({
  String? name,                    // node name — unique in the cluster (default: hostname)
  String bindAddr = '0.0.0.0',
  int bindPort = 7946,
  int replayMaxEntries = 500,
  Duration replayWindow = const Duration(hours: 24),
  MessageStore? messageStore,     // saves every message; history loaded on startDiscovery
})
```

### `Gossiper.resolveLanIp`

```dart
static Future<String> resolveLanIp()
```

Scans network interfaces and returns the best LAN IPv4 address, skipping
loopback, link-local, and known virtual adapters (Docker, WSL2, VirtualBox,
VMware, KVM/libvirt). Pass the result to `bindAddr` for reliable interface
selection on multi-homed machines.

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
| `startDiscovery([strategies])` | `Future<void>` | Load store history (first call), then start every strategy (default mDNS + beacon) |
| `stopDiscovery()` | `void` | Stop all running strategies |
| `MdnsDiscovery.discoverPeers({timeout})` | `List<String>` | One-shot mDNS query (after start) |
| `MdnsDiscovery.joinPeers({timeout})` | `int` | One-shot discover + join (after start) |

### Events

```dart
Stream<NodeEvent> events({Duration pollInterval = const Duration(milliseconds: 500)})
```

Each `NodeEvent` has:
- `type` — `NodeEventType.join`, `.leave`, or `.update`
- `node` — `MemberNode(name, addr, port)`

### Messages

| Method | Returns | Description |
|---|---|---|
| `broadcast(data)` | `Message` | Queue a message to be gossiped cluster-wide (no retry) |
| `messages({pollInterval})` | `Stream<Message>` | Messages received from peers, including replayed ones |
| `setReplayWindow({maxEntries, window})` | `void` | Configure the replay log (default: 500 entries / 24 h) |

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
   ├── Gossiper                         — membership, messages, lifecycle
   ├── DiscoveryStrategy                — MdnsDiscovery, BeaconDiscovery, or yours
   ├── MessageStore                     — JsonLinesMessageStore, or yours
   └── dart:ffi
          │
   gossiper.dll / libgossiper.so
          │
   Go wrapper (native/memberlist_wrapper/src/)
     ├── hashicorp/memberlist   — gossip protocol + in-memory replay log
     └── hashicorp/mdns         — mDNS advertise / lookup
```

The Go wrapper exposes a plain C ABI via CGO. Complex data (member lists,
events, messages) is exchanged as JSON strings. Node instances are managed
server-side and referenced from Dart by opaque `int64` handles. The Go side
never touches the filesystem — persistence is done in Dart by the
`MessageStore`.

---

## Roadmap

- [x] Broadcast messages across the cluster
- [x] Flutter plugin wrapper
- [x] Pre-built blobs in CI (GitHub Actions matrix: windows/linux)
- [x] Message replay on reconnect (missed-message delivery via `LocalState`/`MergeRemoteState`)
- [x] UDP beacon discovery alongside mDNS; `Gossiper.resolveLanIp()` helper
- [x] Pluggable discovery strategies and auto-join rules
- [x] Pluggable message storage (`MessageStore`)
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
