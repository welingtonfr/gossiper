# Gossiper

Dart gossip-protocol library that wraps **[hashicorp/memberlist](https://github.com/hashicorp/memberlist)** (Go) via CGO shared library + Dart FFI, with **pluggable peer discovery** (built-in mDNS via `hashicorp/mdns` and UDP beacon) and **pluggable message storage**.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Dart (consumer)                                         │
│  ┌────────────────────────────────────────────────────┐  │
│  │  lib/gossiper.dart            (public API)         │  │
│  │  lib/src/gossiper_base.dart   (Gossiper class)     │  │
│  │  lib/src/domain/contracts/    (interfaces)         │  │
│  │  lib/src/discovery/           (built-in discovery) │  │
│  │  lib/src/storage/             (built-in stores)    │  │
│  │  lib/src/gossiper_bindings.dart  (FFI layer)       │  │
│  └──────────────────┬─────────────────────────────────┘  │
│                     │ dart:ffi                           │
└─────────────────────┼────────────────────────────────────┘
                      │
             gossiper.dll / libgossiper.so
                      │
┌─────────────────────┼────────────────────────────────────┐
│  Go (native/memberlist_wrapper/src/)                     │
│  ┌──────────────────┴─────────────────────────────────┐  │
│  │  CGO exports (C-compatible function sigs)          │  │
│  │  ├── memberlist core  (create/join/leave/…)        │  │
│  │  ├── messages         (broadcast/poll/replay log)  │  │
│  │  └── mDNS             (start/stop/discover)        │  │
│  └────────────────────────────────────────────────────┘  │
│           ↓                         ↓                    │
│  hashicorp/memberlist       hashicorp/mdns               │
└──────────────────────────────────────────────────────────┘
```

**Key design choices:**

| Decision | Why |
|---|---|
| JSON over the FFI boundary | Avoids complex C structs; Go marshals, Dart unmarshals |
| `int64_t` handles | Safe opaque reference across the CGO boundary |
| Event polling via timer | Avoids CGO callback complexity; 500 ms default is fine for gossip |
| Discovery as `DiscoveryStrategy` list | Users plug their own mechanism; several run at once |
| Go never touches the filesystem | Go keeps only the in-memory replay log; persistence is the Dart `MessageStore`'s job |

### Layers inside `lib/src/`

| Folder | Holds | Rule |
|---|---|---|
| `domain/contracts/` | public interfaces users implement: `DiscoveryStrategy`, `DiscoveryContext`, `AutoJoinRule`, `MessageStore` | no FFI, no `dart:io` |
| `domain/entities/`, `vos/`, `exceptions/` | `MemberNode`, `NodeEvent`, `Message`, `NodeEventType`, `GossiperException` | plain data |
| `discovery/` | built-in strategies `MdnsDiscovery`, `BeaconDiscovery` | may use FFI via the internal `NativeDiscoveryContext` (not exported) |
| `storage/` | built-in `JsonLinesMessageStore` | `dart:io` only |

---

## File map

```
gossiper/
├── CLAUDE.md
├── pubspec.yaml                         Flutter plugin manifest
├── lib/
│   ├── gossiper.dart                    re-exports public symbols
│   └── src/
│       ├── gossiper_base.dart           Gossiper (+ private _GossiperDiscoveryContext)
│       ├── gossiper_bindings.dart       raw FFI typedefs + loader
│       ├── beacon_utils.dart            subnetBroadcast, isVirtualInterface, parseBeacon
│       ├── domain/
│       │   ├── contracts/               discovery_strategy, discovery_context,
│       │   │                            auto_join_rule, message_store
│       │   ├── entities/                member_node, node_event, message
│       │   ├── vos/                     node_event_type
│       │   └── exceptions/              gossiper_exception
│       ├── discovery/                   mdns_discovery, beacon_discovery,
│       │                                native_discovery_context (internal)
│       └── storage/                     json_lines_message_store
├── windows/CMakeLists.txt               bundles blobs/windows/gossiper.dll
├── linux/CMakeLists.txt                 bundles blobs/linux/libgossiper.so
├── native/memberlist_wrapper/
│   ├── src/
│   │   ├── wrapper.go                   handle registry, FreeString
│   │   ├── delegate.go                  memberlist delegate: events, replay log
│   │   ├── exports_memberlist.go        core + message exports
│   │   ├── exports_mdns.go              mDNS exports
│   │   └── helpers.go                   setErr, localIPs, newUUID
│   ├── go.mod                           Go module (memberlist + mdns)
│   ├── build.ps1                        Windows build → blobs/windows/
│   └── Makefile                         Linux build → blobs/linux/
├── blobs/                               compiled .dll/.so (gitignored, built by CI)
├── example/gossiper_example.dart
└── test/gossiper_test.dart
```

---

## Build the native library

### Prerequisites

| Platform | Requirement |
|---|---|
| Windows | Go 1.21+, [tdm-gcc](https://jmeubank.github.io/tdm-gcc/) or MSYS2 mingw-w64 |
| Linux | Go 1.21+, gcc |

### Windows

```powershell
cd native\memberlist_wrapper
go mod tidy
.\build.ps1
# → blobs\windows\gossiper.dll
```

### Linux

```bash
cd native/memberlist_wrapper
make linux
# → blobs/linux/libgossiper.so
```

### Place the library

`Gossiper.create` opens `gossiper.dll` (Windows) or `libgossiper.so` (other platforms) from the OS dynamic-linker search path. In a Flutter app the plugin bundles it automatically; for plain Dart, put it beside the executable or on `LD_LIBRARY_PATH`.

**Any change to an exported Go signature requires rebuilding the library** — an old `.so`/`.dll` with a new Dart side fails at load (missing symbol) or, worse, at runtime (changed return type).

---

## Dart API quick reference

```dart
// Create a node
final gossiper = Gossiper.create(
  name: 'node-1',                 // optional, defaults to hostname
  bindAddr: '0.0.0.0',
  bindPort: 7946,
  replayMaxEntries: 500,          // in-memory replay log size
  replayWindow: Duration(hours: 24),
  messageStore: JsonLinesMessageStore('gossiper.log'), // optional, saves every message
);

// Discovery — default mDNS + beacon; first call loads the store history
await gossiper.startDiscovery();

// …or any combination of strategies
await gossiper.startDiscovery([
  MdnsDiscovery(serviceName: '_gossiper._tcp'),        // autoJoin: AutoJoinRules.always
  BeaconDiscovery(beaconType: 'gossiper_hello'),       // autoJoin: AutoJoinRules.whenAlone
]);
gossiper.stopDiscovery();

// Or join with explicit addresses
gossiper.join(['192.168.1.5:7946', '192.168.1.6:7946']);

// Messages
final sent = gossiper.broadcast('{"op":"ping"}');      // Message(id, data, at)
gossiper.messages().listen((m) => print(m.data));      // Stream<Message>

// Stream of join/leave/update events
gossiper.events().listen((e) => print('${e.type.name}: ${e.node}'));

// Inspect members
print(gossiper.members());     // List<MemberNode>
print(gossiper.localNode());   // MemberNode
print(gossiper.numMembers());  // int

// Teardown
gossiper.leave();
gossiper.dispose();
```

---

## Discovery

`startDiscovery([List<DiscoveryStrategy>?])` (default `[MdnsDiscovery(), BeaconDiscovery()]`)
stops any running strategies, loads the store history on the first call, builds one
`DiscoveryContext` (`lanIp`, `gossipPort`, `selfAddr`, `numMembers`, `join`) and
calls `start(ctx)` on each in order. If one throws, those already started are
stopped and the error is rethrown.

| Strategy | start | Auto-join |
|---|---|---|
| `MdnsDiscovery` | `MemberlistStartMDNS`, then a blocking `MemberlistDiscoverMDNS` lookup (`lookupTimeout`, 2 s) | each found peer passing `autoJoin` (default `always`); join errors swallowed |
| `BeaconDiscovery` | binds UDP `port` (7947), broadcasts `{"type","addr"}` every `interval`; socket failure is non-fatal | each heard beacon passing `autoJoin` (default `whenAlone`) |

`autoJoin: null` disables automatic joining (mDNS then skips the lookup).
`MdnsDiscovery.discoverPeers()` / `joinPeers()` are manual, post-start helpers;
`joinPeers` ignores `autoJoin`.

To add a built-in strategy: implement `DiscoveryStrategy` in `lib/src/discovery/`,
export it from `lib/gossiper.dart`. If it needs FFI, require
`NativeDiscoveryContext` (see `MdnsDiscovery.start`).

---

## Messages and storage

Go owns the **in-memory replay log** (`msgLog` in `delegate.go`): UUID dedup in
`NotifyMsg`, replay via `LocalState` / `MergeRemoteState`, pruning by
`maxLog` / `logWindow`. It never reads or writes files.

Persistence flow (Dart, `gossiper_base.dart`) — zero conf, no public calls:

```
broadcast(data)          → MemberlistBroadcast returns {id,data,at} → store.saveMessage (not awaited)
_pollMessages()          → each Message → store.saveMessage + messages() stream
1st startDiscovery()     → _loadHistory(): store.getMessagesSince(cutoff, limit: replayMaxEntries)
                           → MemberlistRestoreLog (seeded entries are not delivered; they are
                             replayed to peers via LocalState and dedup MergeRemoteState)
```

`cutoff = now - replayWindow`. The library never deletes from the store — retention
is the client's call. Retrying failed sends (outbox) is also the client's job. `join()` alone does not load the history.


---

## Adding a new exported function

Follow this three-step pattern to keep the layers in sync.

### 1. Go — add a CGO export in `src/exports_memberlist.go` (or `src/exports_mdns.go`)

```go
// MemberlistFoo does X.
//
//export MemberlistFoo
func MemberlistFoo(handle C.int64_t, ...) C.int {
    d := lookupDelegate(handle)   // or lookup(handle) for the *memberlist.Memberlist
    ...
}
```

Returned strings are `C.CString(...)` and must be freed by Dart via `FreeString`.

### 2. Dart — add typedefs in `gossiper_bindings.dart`

```dart
typedef _MemberlistFooNative = Int32 Function(Int64 handle, ...);
typedef MemberlistFooDart   = int   Function(int    handle, ...);
```

Register it in `GossiperBindings.fromLibrary`:

```dart
memberlistFoo: lib
    .lookup<NativeFunction<_MemberlistFooNative>>('MemberlistFoo')
    .asFunction(),
```

Add the field to `GossiperBindings._({...})` and the constructor, and the
parameter to `GossiperBindings.fake(...)` (and `_fakeBindings` in the tests).

### 3. Dart — expose it in `gossiper_base.dart`

```dart
int foo() => _bindings.memberlistFoo(_handle, ...);
```

---

## Testing

```bash
dart pub get
dart test
```

Unit tests use `GossiperBindings.fake(...)` + `Gossiper.withBindings(bindings, messageStore: ...)` — no native library needed. When adding a binding, add it to `_fakeBindings` in `test/gossiper_test.dart` and to every `GossiperBindings.fake(...)` copy there.

---

## Roadmap

- [ ] User data / metadata per node (memberlist `Delegate`)
- [ ] macOS / Android / iOS support
