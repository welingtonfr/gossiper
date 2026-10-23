import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';
import 'package:gossiper/src/domain/entities/member_node.dart';
import 'package:gossiper/src/domain/entities/node_event.dart';
import 'package:gossiper/src/domain/exceptions/gossiper_exception.dart';
import 'package:gossiper/src/domain/vos/node_event_type.dart';

import 'beacon_utils.dart';
import 'gossiper_bindings.dart';

// Reads an error C string from errPtr safely and frees it.
// Returns 'unknown error' when the native side didn't set the pointer.
String _readErr(GossiperBindings bindings, Pointer<Pointer<Utf8>> errPtr) {
  if (errPtr.value == nullptr) return 'unknown error';
  final msg = errPtr.value.cast<Utf8>().toDartString();
  bindings.freeString(errPtr.value.cast());
  errPtr.value = nullptr;
  return msg;
}

class Gossiper {
  final GossiperBindings _bindings;
  final int _handle;
  Timer? _pollTimer;
  Timer? _messagesPollTimer;
  final _eventController = StreamController<NodeEvent>.broadcast();
  final _messagesController = StreamController<String>.broadcast();
  RawDatagramSocket? _beaconSocket;
  Timer? _beaconTimer;

  Gossiper._(this._bindings, this._handle);

  /// Creates a Gossiper from pre-built bindings — for use in unit tests only.
  static Gossiper withBindings(GossiperBindings bindings, {int handle = 1}) =>
      Gossiper._(bindings, handle);

  static GossiperBindings _loadLib(String? libPath) {
    final path = libPath ?? _defaultLibPath();
    return GossiperBindings.fromLibrary(DynamicLibrary.open(path));
  }

  static String _defaultLibPath() {
    if (Platform.isWindows) return 'gossiper.dll';
    return 'libgossiper.so';
  }

  /// Creates a new Gossiper node.
  ///
  /// [name] is the unique node name (defaults to hostname if omitted).
  /// [bindAddr] is the address to bind to (default "0.0.0.0").
  /// [bindPort] is the gossip port (default 7946).
  /// [libPath] overrides the shared-library search path.
  static Gossiper create({
    String? name,
    String bindAddr = '0.0.0.0',
    int bindPort = 7946,
    String? libPath,
    int replayMaxEntries = 500,
    Duration replayWindow = const Duration(hours: 24),
    String? persistPath,
  }) {
    final bindings = _loadLib(libPath);

    final namePtr = name != null ? name.toNativeUtf8() : nullptr;
    final addrPtr = bindAddr.toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();

    try {
      final handle = bindings.memberlistCreate(
        namePtr.cast(),
        addrPtr.cast(),
        bindPort,
        errPtr.cast(),
      );

      if (handle < 0) {
        throw GossiperException('Failed to create node: ${_readErr(bindings, errPtr)}');
      }

      final g = Gossiper._(bindings, handle);
      g.setReplayWindow(maxEntries: replayMaxEntries, window: replayWindow);
      if (persistPath != null) g.setPersistPath(persistPath);
      return g;
    } finally {
      if (namePtr != nullptr) calloc.free(namePtr);
      calloc.free(addrPtr);
      calloc.free(errPtr);
    }
  }

  // -------------------------------------------------------------------------
  // Cluster membership
  // -------------------------------------------------------------------------

  /// Joins an existing cluster via [addresses] (e.g. ["192.168.1.2:7946"]).
  /// Returns the number of seed nodes successfully contacted.
  int join(List<String> addresses) {
    final addrsJson = jsonEncode(addresses).toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final n = _bindings.memberlistJoin(_handle, addrsJson.cast(), errPtr.cast());
      if (n < 0) {
        throw GossiperException('Join failed: ${_readErr(_bindings, errPtr)}');
      }
      return n;
    } finally {
      calloc.free(addrsJson);
      calloc.free(errPtr);
    }
  }

  /// Returns the list of alive cluster members.
  List<MemberNode> members() {
    final ptr = _bindings.memberlistMembers(_handle);
    final json = ptr.cast<Utf8>().toDartString();
    _bindings.freeString(ptr.cast());
    return (jsonDecode(json) as List).map((e) => MemberNode.fromJson(e as Map<String, dynamic>)).toList();
  }

  /// Returns this node's own info.
  MemberNode localNode() {
    final ptr = _bindings.memberlistLocalNode(_handle);
    final json = ptr.cast<Utf8>().toDartString();
    _bindings.freeString(ptr.cast());
    return MemberNode.fromJson(jsonDecode(json) as Map<String, dynamic>);
  }

  /// Returns the number of alive cluster members.
  int numMembers() => _bindings.memberlistNumMembers(_handle);

  /// Gracefully leaves the cluster.
  void leave({Duration timeout = const Duration(seconds: 5)}) {
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final result = _bindings.memberlistLeave(_handle, timeout.inMilliseconds, errPtr.cast());
      if (result < 0) {
        throw GossiperException('Leave failed: ${_readErr(_bindings, errPtr)}');
      }
    } finally {
      calloc.free(errPtr);
    }
  }

  // -------------------------------------------------------------------------
  // Events
  // -------------------------------------------------------------------------

  /// Stream of [NodeEvent]s delivered by polling the native layer.
  /// Each call resets the poll timer to [pollInterval].
  Stream<NodeEvent> events({Duration pollInterval = const Duration(milliseconds: 500)}) {
    _pollTimer?.cancel();
    _pollTimer = Timer.periodic(pollInterval, (_) => _pollEvents());
    return _eventController.stream;
  }

  void _pollEvents() {
    final ptr = _bindings.memberlistPollEvents(_handle);
    final json = ptr.cast<Utf8>().toDartString();
    _bindings.freeString(ptr.cast());

    for (final item in jsonDecode(json) as List) {
      final map = item as Map<String, dynamic>;
      final type = switch (map['type'] as int) {
        0 => NodeEventType.join,
        1 => NodeEventType.leave,
        _ => NodeEventType.update,
      };
      _eventController.add(NodeEvent(type: type, node: MemberNode.fromJson(map)));
    }
  }

  // -------------------------------------------------------------------------
  // Replay window configuration
  // -------------------------------------------------------------------------

  /// Configures durable message log storage.
  ///
  /// When set, every broadcast message is appended to [path] as a JSON line.
  /// On startup the file is loaded before the node joins the cluster, so a
  /// PDV that restarts recovers its message history automatically — even if
  /// every other node was also restarted in the meantime.
  ///
  /// Pass an empty string to disable persistence.
  void setPersistPath(String path) {
    final pathPtr = path.toNativeUtf8();
    try {
      _bindings.memberlistSetPersistPath(_handle, pathPtr.cast());
    } finally {
      calloc.free(pathPtr);
    }
  }

  /// Configures the in-memory message replay log.
  ///
  /// When a node reconnects after being offline, memberlist automatically
  /// calls [MergeRemoteState] and delivers messages the node missed —
  /// up to [maxEntries] entries or [window] hours of history, whichever
  /// is reached first. Defaults: 500 entries / 24 hours.
  void setReplayWindow({
    int maxEntries = 500,
    Duration window = const Duration(hours: 24),
  }) {
    _bindings.memberlistSetReplayWindow(_handle, maxEntries, window.inHours);
  }

  // -------------------------------------------------------------------------
  // Broadcast messaging
  // -------------------------------------------------------------------------

  /// Queues [data] to be gossiped to every node in the cluster.
  ///
  /// The message is propagated via memberlist's gossip broadcast mechanism.
  /// Delivery is best-effort and eventual — not guaranteed or ordered.
  /// [data] is opaque: use any encoding (JSON, plain text, Protobuf, etc.).
  void broadcast(String data) {
    final dataPtr = data.toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final result = _bindings.memberlistBroadcast(_handle, dataPtr.cast(), errPtr.cast());
      if (result < 0) {
        throw GossiperException('Broadcast failed: ${_readErr(_bindings, errPtr)}');
      }
    } finally {
      calloc.free(dataPtr);
      calloc.free(errPtr);
    }
  }

  /// Stream of messages received from other cluster nodes.
  ///
  /// Messages are raw strings exactly as sent by [broadcast] on the remote node.
  /// Each call resets the poll interval to [pollInterval].
  Stream<String> messages({Duration pollInterval = const Duration(milliseconds: 500)}) {
    _messagesPollTimer?.cancel();
    _messagesPollTimer = Timer.periodic(pollInterval, (_) => _pollMessages());
    return _messagesController.stream;
  }

  void _pollMessages() {
    final ptr = _bindings.memberlistPollMessages(_handle);
    final json = ptr.cast<Utf8>().toDartString();
    _bindings.freeString(ptr.cast());

    for (final msg in jsonDecode(json) as List) {
      _messagesController.add(msg as String);
    }
  }

  // -------------------------------------------------------------------------
  // Discovery (mDNS + beacon)
  // -------------------------------------------------------------------------

  /// Starts all discovery mechanisms for this node:
  ///
  /// 1. mDNS advertisement — responds to queries from peers calling [discoverPeers].
  /// 2. UDP beacon — broadcasts this node's address on the LAN every [beaconInterval]
  ///    and automatically calls [join] when it hears another node's beacon.
  ///
  /// Both mechanisms run concurrently in the background. The node joins the
  /// cluster as soon as any peer is found — no additional calls needed.
  /// Call [stopDiscovery] or [dispose] to stop both.
  ///
  /// [beaconType] scopes the beacon to a logical cluster — nodes with
  /// different types on the same network will not discover each other.
  Future<void> startDiscovery({
    String serviceName = '_gossiper._tcp',
    int beaconPort = 7947,
    Duration beaconInterval = const Duration(seconds: 5),
    String beaconType = 'gossiper_hello',
  }) async {
    // 1. mDNS advertisement
    final svcPtr = serviceName.toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final result = _bindings.memberlistStartMDNS(_handle, svcPtr.cast(), errPtr.cast());
      if (result < 0) {
        throw GossiperException('mDNS start failed: ${_readErr(_bindings, errPtr)}');
      }
    } finally {
      calloc.free(svcPtr);
      calloc.free(errPtr);
    }

    // 2. Beacon — non-fatal if UDP is unavailable
    final lanIp = await _resolveLanIp();
    final gossipPort = localNode().port;
    final selfAddr = '$lanIp:$gossipPort';

    _stopBeacon();
    try {
      _beaconSocket = await RawDatagramSocket.bind(InternetAddress.anyIPv4, beaconPort);
      _beaconSocket!.broadcastEnabled = true;
      _beaconSocket!.listen((event) {
        if (event != RawSocketEvent.read) return;
        final dg = _beaconSocket?.receive();
        if (dg == null) return;
        _handleBeacon(
          utf8.decode(dg.data, allowMalformed: true),
          selfAddr,
          beaconType,
        );
      });

      _sendBeacon(lanIp, gossipPort, beaconPort, beaconType);
      _beaconTimer = Timer.periodic(beaconInterval, (_) {
        _sendBeacon(lanIp, gossipPort, beaconPort, beaconType);
      });
    } catch (_) {
      // beacon failure is non-fatal — mDNS still works
    }
  }

  void _sendBeacon(String lanIp, int gossipPort, int beaconPort, String beaconType) {
    final socket = _beaconSocket;
    if (socket == null) return;
    final payload = Uint8List.fromList(
      utf8.encode(jsonEncode({'type': beaconType, 'addr': '$lanIp:$gossipPort'})),
    );
    try {
      socket.send(payload, InternetAddress(BeaconUtils.subnetBroadcast(lanIp)), beaconPort);
    } catch (_) {}
  }

  void _handleBeacon(String payload, String selfAddr, String beaconType) {
    BeaconUtils.processBeacon(
      payload: payload,
      selfAddr: selfAddr,
      beaconType: beaconType,
      numMembers: numMembers(),
      onJoin: (addr) => join([addr]),
    );
  }

  Future<String> _resolveLanIp() async {
    final nodeAddr = localNode().addr;
    if (nodeAddr != '0.0.0.0' && !nodeAddr.startsWith('127.')) return nodeAddr;

    final interfaces = await NetworkInterface.list(
      type: InternetAddressType.IPv4,
      includeLinkLocal: false,
    );
    String? fallback;
    for (final iface in interfaces) {
      for (final addr in iface.addresses) {
        final ip = addr.address;
        if (ip.startsWith('127.') || ip.startsWith('169.254.')) continue;
        if (BeaconUtils.isVirtualInterface(iface, ip)) continue;
        if (ip.startsWith('192.168.') || ip.startsWith('10.')) return ip;
        fallback ??= ip;
      }
    }
    return fallback ?? '0.0.0.0';
  }

  void _stopBeacon() {
    _beaconTimer?.cancel();
    _beaconTimer = null;
    _beaconSocket?.close();
    _beaconSocket = null;
  }

  /// Stops mDNS advertisement and beacon broadcasting for this node.
  void stopDiscovery() {
    _stopBeacon();
    _bindings.memberlistStopMDNS(_handle);
  }

  /// Queries the local network for nodes advertising [serviceName] via mDNS.
  ///
  /// Returns a list of `"addr:port"` strings that can be passed directly to
  /// [join]. Blocks for up to [timeout] waiting for responses.
  ///
  /// This is a synchronous call — run it in an isolate or wrap with
  /// [Future] if you need non-blocking behaviour.
  List<String> discoverPeers({
    String serviceName = '_gossiper._tcp',
    Duration timeout = const Duration(seconds: 2),
  }) {
    final svcPtr = serviceName.toNativeUtf8();
    try {
      final ptr = _bindings.memberlistDiscoverMDNS(svcPtr.cast(), timeout.inMilliseconds);
      final json = ptr.cast<Utf8>().toDartString();
      _bindings.freeString(ptr.cast());
      return (jsonDecode(json) as List).cast<String>();
    } finally {
      calloc.free(svcPtr);
    }
  }

  /// Convenience method: discovers peers via mDNS and immediately joins them.
  ///
  /// Returns the number of seed nodes successfully contacted.
  /// Returns 0 if no peers were found (this node will wait for others to join it).
  int joinViaDiscovery({
    String serviceName = '_gossiper._tcp',
    Duration discoveryTimeout = const Duration(seconds: 2),
  }) {
    final peers = discoverPeers(serviceName: serviceName, timeout: discoveryTimeout);
    if (peers.isEmpty) return 0;
    return join(peers);
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /// Stops polling, discovery (mDNS + beacon), and shuts down the node.
  void dispose() {
    _pollTimer?.cancel();
    _messagesPollTimer?.cancel();
    stopDiscovery();
    _eventController.close();
    _messagesController.close();
    _bindings.memberlistDestroy(_handle);
  }
}

