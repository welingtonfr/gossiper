import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';
import 'package:gossiper/src/domain/contracts/discovery_strategy.dart';
import 'package:gossiper/src/domain/contracts/message_store.dart';
import 'package:gossiper/src/domain/entities/message.dart';
import 'package:gossiper/src/domain/entities/member_node.dart';
import 'package:gossiper/src/domain/entities/node_event.dart';
import 'package:gossiper/src/domain/exceptions/gossiper_exception.dart';
import 'package:gossiper/src/domain/vos/node_event_type.dart';

import 'beacon_utils.dart';
import 'discovery/beacon_discovery.dart';
import 'discovery/mdns_discovery.dart';
import 'discovery/native_discovery_context.dart';
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
  final _messagesController = StreamController<Message>.broadcast();
  final _discoveries = <DiscoveryStrategy>[];
  int _replayMaxEntries = 500;
  Duration _replayWindow = const Duration(hours: 24);
  MessageStore? _messageStore;
  bool _historyLoaded = false;

  Gossiper._(this._bindings, this._handle);

  /// Creates a Gossiper from pre-built bindings — for use in unit tests only.
  static Gossiper withBindings(
    GossiperBindings bindings, {
    int handle = 1,
    MessageStore? messageStore,
  }) =>
      Gossiper._(bindings, handle).._messageStore = messageStore;

  static GossiperBindings _loadLib() {
    final path = _defaultLibPath();
    return GossiperBindings.fromLibrary(DynamicLibrary.open(path));
  }

  static String _defaultLibPath() {
    if (Platform.isWindows) return 'gossiper.dll';
    return 'libgossiper.so';
  }

  /// Resolves the best LAN IPv4 address on this machine.
  ///
  /// Skips loopback, link-local, and known virtual adapter addresses
  /// (Docker, WSL2, VirtualBox, VMware, KVM). Prefers `192.168.x.x` and
  /// `10.x.x.x` ranges; falls back to the first other routable address,
  /// or `'0.0.0.0'` if nothing is found.
  ///
  /// Useful for passing an explicit [bindAddr] to [create] on machines
  /// with multiple network interfaces.
  static Future<String> resolveLanIp() async {
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

  /// Creates a new Gossiper node.
  ///
  /// [name] is the unique node name (defaults to hostname if omitted).
  /// [bindAddr] is the address to bind to (default "0.0.0.0").
  /// [bindPort] is the gossip port (default 7946).
  ///
  /// [messageStore], when given, saves every message sent or received, and
  /// its history is loaded into the replay log on the first [startDiscovery]
  /// so peers that come back can still receive it after a restart.
  static Gossiper create({
    String? name,
    String bindAddr = '0.0.0.0',
    int bindPort = 7946,
    int replayMaxEntries = 500,
    Duration replayWindow = const Duration(hours: 24),
    MessageStore? messageStore,
  }) {
    final bindings = _loadLib();

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
      g._messageStore = messageStore;
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
    final decoded = jsonDecode(json);
    if (decoded is! List) return [];
    return decoded.map((e) => MemberNode.fromJson(e as Map<String, dynamic>)).toList();
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

    final decoded = jsonDecode(json);
    if (decoded is! List) return;
    for (final item in decoded) {
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
    _replayMaxEntries = maxEntries;
    _replayWindow = window;
    _bindings.memberlistSetReplayWindow(_handle, maxEntries, window.inHours);
  }

  // -------------------------------------------------------------------------
  // Message persistence
  // -------------------------------------------------------------------------

  /// Loads the [MessageStore] history within the replay window into the
  /// native replay log, after deleting older entries from the store.
  ///
  /// Restored messages are not delivered on [messages]; they are replayed to
  /// peers and used to dedup what peers send back.
  Future<void> _loadHistory() async {
    final store = _messageStore;
    if (store == null) return;
    final cutoff = DateTime.now().subtract(_replayWindow).millisecondsSinceEpoch;
    await store.deleteBefore(cutoff);
    final history = await store.getMessagesSince(cutoff, limit: _replayMaxEntries);
    if (history.isEmpty) return;

    final json = jsonEncode(history.map((m) => m.toJson()).toList()).toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final result = _bindings.memberlistRestoreLog(_handle, json.cast(), errPtr.cast());
      if (result < 0) {
        throw GossiperException('Restore log failed: ${_readErr(_bindings, errPtr)}');
      }
    } finally {
      calloc.free(json);
      calloc.free(errPtr);
    }
  }

  // -------------------------------------------------------------------------
  // Broadcast messaging
  // -------------------------------------------------------------------------

  /// Queues [data] to be gossiped to every node in the cluster.
  ///
  /// The message is propagated via memberlist's gossip broadcast mechanism.
  /// Delivery is best-effort and eventual — not guaranteed or ordered.
  /// [data] is opaque: use any encoding (JSON, plain text, Protobuf, etc.).
  ///
  /// Returns the sent [Message], with the [Message.id] and [Message.at]
  /// peers will receive. It is also saved to the [MessageStore], if any.
  /// Delivery failures are not retried — keep your own outbox if you need that.
  Message broadcast(String data) {
    final dataPtr = data.toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final ptr = _bindings.memberlistBroadcast(_handle, dataPtr.cast(), errPtr.cast());
      if (ptr == nullptr) {
        throw GossiperException('Broadcast failed: ${_readErr(_bindings, errPtr)}');
      }
      final json = ptr.cast<Utf8>().toDartString();
      _bindings.freeString(ptr.cast());
      final message = Message.fromJson(jsonDecode(json) as Map<String, dynamic>);
      _messageStore?.saveMessage(message);
      return message;
    } finally {
      calloc.free(dataPtr);
      calloc.free(errPtr);
    }
  }

  /// Stream of [Message]s received from other cluster nodes, including
  /// messages missed while offline and replayed on reconnect.
  ///
  /// [Message.data] is exactly what [broadcast] was given on the remote node.
  /// Each message is also saved to the [MessageStore], if any.
  /// Each call resets the poll interval to [pollInterval].
  Stream<Message> messages({Duration pollInterval = const Duration(milliseconds: 500)}) {
    _messagesPollTimer?.cancel();
    _messagesPollTimer = Timer.periodic(pollInterval, (_) => _pollMessages());
    return _messagesController.stream;
  }

  void _pollMessages() {
    final ptr = _bindings.memberlistPollMessages(_handle);
    final json = ptr.cast<Utf8>().toDartString();
    _bindings.freeString(ptr.cast());

    final decoded = jsonDecode(json);
    if (decoded is! List) return;
    for (final item in decoded) {
      final message = Message.fromJson(item as Map<String, dynamic>);
      _messageStore?.saveMessage(message);
      _messagesController.add(message);
    }
  }

  // -------------------------------------------------------------------------
  // Discovery (pluggable strategies)
  // -------------------------------------------------------------------------

  /// Starts every strategy in [strategies] concurrently on this node.
  ///
  /// Defaults to `[MdnsDiscovery(), BeaconDiscovery()]`. Pass your own
  /// [DiscoveryStrategy] implementations — alone or mixed with the built-in
  /// ones — to discover peers some other way.
  ///
  /// On the first call, the [MessageStore] history (if any) is loaded into the
  /// replay log before any strategy starts, so this node knows what it already
  /// has before talking to peers.
  ///
  /// Any strategies already running are stopped first. If a strategy throws,
  /// the ones already started are stopped and the error is rethrown.
  /// Call [stopDiscovery] or [dispose] to stop them all.
  Future<void> startDiscovery([List<DiscoveryStrategy>? strategies]) async {
    stopDiscovery();
    if (!_historyLoaded) {
      await _loadHistory();
      _historyLoaded = true;
    }
    final ctx = _GossiperDiscoveryContext(this, await _resolveLanIp(), localNode().port);
    for (final strategy in strategies ?? [MdnsDiscovery(), BeaconDiscovery()]) {
      _discoveries.add(strategy);
      try {
        await strategy.start(ctx);
      } catch (_) {
        stopDiscovery();
        rethrow;
      }
    }
  }

  Future<String> _resolveLanIp() async {
    final nodeAddr = localNode().addr;
    if (nodeAddr != '0.0.0.0' && !nodeAddr.startsWith('127.')) return nodeAddr;
    return Gossiper.resolveLanIp();
  }

  /// Stops every strategy started by [startDiscovery].
  void stopDiscovery() {
    for (final strategy in _discoveries) {
      strategy.stop();
    }
    _discoveries.clear();
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /// Stops polling, all discovery strategies, and shuts down the node.
  void dispose() {
    _pollTimer?.cancel();
    _messagesPollTimer?.cancel();
    stopDiscovery();
    _eventController.close();
    _messagesController.close();
    _bindings.memberlistDestroy(_handle);
  }
}


class _GossiperDiscoveryContext implements NativeDiscoveryContext {
  final Gossiper _gossiper;

  @override
  final String lanIp;

  @override
  final int gossipPort;

  _GossiperDiscoveryContext(this._gossiper, this.lanIp, this.gossipPort);

  @override
  String get selfAddr => '$lanIp:$gossipPort';

  @override
  int get numMembers => _gossiper.numMembers();

  @override
  int join(List<String> addresses) => _gossiper.join(addresses);

  @override
  GossiperBindings get bindings => _gossiper._bindings;

  @override
  int get handle => _gossiper._handle;
}
