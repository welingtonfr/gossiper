# Gossiper

Dart gossip-protocol library that wraps **[hashicorp/memberlist](https://github.com/hashicorp/memberlist)** (Go) via CGO shared library + Dart FFI, with **automatic peer discovery via mDNS** (`hashicorp/mdns`).

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Dart (consumer)                                    │
│  ┌──────────────────────────────────────────────┐  │
│  │  lib/gossiper.dart           (public API)    │  │
│  │  lib/src/gossiper_base.dart  (Gossiper class)│  │
│  │  lib/src/gossiper_bindings.dart  (FFI layer) │  │
│  └──────────────────┬───────────────────────────┘  │
│                     │ dart:ffi                      │
└─────────────────────┼───────────────────────────────┘
                      │
             gossiper.dll / libgossiper.so / .dylib
                      │
┌─────────────────────┼───────────────────────────────┐
│  Go (native/memberlist_wrapper/wrapper.go)           │
│  ┌──────────────────┴───────────────────────────┐   │
│  │  CGO exports (C-compatible function sigs)    │   │
│  │  ├── memberlist core  (create/join/leave/…)  │   │
│  │  └── mDNS             (start/stop/discover)  │   │
│  └──────────────────────────────────────────────┘   │
│           ↓                         ↓               │
│  hashicorp/memberlist       hashicorp/mdns           │
└──────────────────────────────────────────────────────┘
```

**Key design choices:**

| Decision | Why |
|---|---|
| JSON over the FFI boundary | Avoids complex C structs; Go marshals, Dart unmarshals |
| `int64_t` handles | Safe opaque reference across the CGO boundary |
| Event polling via timer | Avoids CGO callback complexity; 500 ms default is fine for gossip |
| mDNS for discovery | Zero-config LAN clustering; no seed list required |

---

## File map

```
gossiper/
├── CLAUDE.md
├── pubspec.yaml                         dart package manifest (ffi dep)
├── lib/
│   ├── gossiper.dart                    re-exports public symbols
│   └── src/
│       ├── gossiper_base.dart           Gossiper, MemberNode, NodeEvent
│       └── gossiper_bindings.dart       raw FFI typedefs + loader
├── native/
│   └── memberlist_wrapper/
│       ├── wrapper.go                   CGO exports for memberlist + mDNS
│       ├── go.mod                       Go module (memberlist + mdns)
│       ├── build.ps1                    Windows build → blobs/windows/
│       └── Makefile                     Linux/macOS build → blobs/linux|macos/
├── blobs/                               compiled .dll/.so/.dylib (gitignored)
│   ├── windows/gossiper.dll
│   ├── linux/libgossiper.so
│   └── macos/libgossiper.dylib
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
| macOS | Go 1.21+, Xcode CLT (`xcode-select --install`) |

### Windows

```powershell
cd native\memberlist_wrapper
go mod tidy
.\build.ps1
# → blobs\windows\gossiper.dll
```

### Linux / macOS

```bash
cd native/memberlist_wrapper
make linux   # or: make macos
```

### Place the library

The Dart runtime searches for the `.dll`/`.so`/`.dylib` next to the executable (or wherever the OS dynamic-linker looks). For development, copy the built file beside your script or pass `libPath:` to `Gossiper.create`.

---

## Dart API quick reference

```dart
// Create a node
final gossiper = Gossiper.create(
  name: 'node-1',       // optional, defaults to hostname
  bindAddr: '0.0.0.0',
  bindPort: 7946,
  libPath: 'gossiper.dll', // optional override
);

// Advertise on LAN
gossiper.startDiscovery(serviceName: '_gossiper._tcp');

// Auto-discover peers and join
final contacted = gossiper.joinViaDiscovery();

// Or join with explicit addresses
gossiper.join(['192.168.1.5:7946', '192.168.1.6:7946']);

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

## mDNS discovery

mDNS uses multicast UDP 224.0.0.251:5353. All nodes on the same L2 segment
hear each other automatically — no seed list, no config file, no service registry.

Flow:

```
Node A starts             Node B starts
  │                         │
  ├─ startDiscovery()        ├─ startDiscovery()
  │   registers              │   registers
  │   _gossiper._tcp          │   _gossiper._tcp
  │                         │
  ├─ joinViaDiscovery()     ├─ joinViaDiscovery()
  │   ← mDNS query →         │   ← mDNS query →
  │   finds B                │   finds A
  │   memberlist.Join(B)     │   memberlist.Join(A)
  │                         │
  └─ gossip starts ──────────┘
```

`discoverPeers()` blocks for up to `timeout` — run it in an isolate if you need
non-blocking behaviour:

```dart
final peers = await Isolate.run(() => gossiper.discoverPeers(timeout: Duration(seconds: 3)));
```

---

## Adding a new exported function

Follow this three-step pattern to keep the layers in sync.

### 1. Go — add a CGO export in `wrapper.go`

```go
// MemberlistFoo does X.
//
//export MemberlistFoo
func MemberlistFoo(handle C.int64_t, ...) C.int {
    m := lookup(handle)
    ...
}
```

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

Add the field to `GossiperBindings._({...})` and the constructor.

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

Integration tests that spin up real nodes require the compiled `.dll`/`.so` in the path — set the `GOSSIPER_LIB` env var or pass `libPath:` directly.

---

## Roadmap

- [ ] User data / metadata per node (memberlist `Delegate`)
- [ ] Broadcast messages across the cluster
- [ ] Pre-built blobs in CI (GitHub Actions matrix: windows/linux/macos)
- [ ] Flutter plugin wrapper
