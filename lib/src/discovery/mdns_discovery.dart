import 'dart:convert';
import 'dart:ffi';

import 'package:ffi/ffi.dart';

import '../domain/contracts/auto_join_rule.dart';
import '../domain/contracts/discovery_context.dart';
import '../domain/contracts/discovery_strategy.dart';
import '../domain/exceptions/gossiper_exception.dart';
import '../gossiper_bindings.dart';
import 'native_discovery_context.dart';

/// Discovers peers via mDNS (multicast 224.0.0.251:5353).
///
/// On [start] it advertises this node under [serviceName] and queries the
/// LAN for other nodes advertising the same service, joining those that
/// [autoJoin] allows (default: all, [AutoJoinRules.always]). Pass
/// `autoJoin: null` to only advertise — the lookup is then skipped too.
/// Call [joinPeers] later to query and join again (e.g. on retry).
///
/// The mDNS query is a blocking native call that lasts up to [lookupTimeout].
class MdnsDiscovery implements DiscoveryStrategy {
  final String serviceName;
  final AutoJoinRule? autoJoin;
  final Duration lookupTimeout;

  NativeDiscoveryContext? _ctx;

  MdnsDiscovery({
    this.serviceName = '_gossiper._tcp',
    this.autoJoin = AutoJoinRules.always,
    this.lookupTimeout = const Duration(seconds: 2),
  });

  @override
  Future<void> start(DiscoveryContext ctx) async {
    if (ctx is! NativeDiscoveryContext) {
      throw GossiperException('MdnsDiscovery requires a Gossiper-provided context');
    }
    final svcPtr = serviceName.toNativeUtf8();
    final errPtr = calloc<Pointer<Utf8>>();
    try {
      final result = ctx.bindings.memberlistStartMDNS(ctx.handle, svcPtr.cast(), errPtr.cast());
      if (result < 0) {
        throw GossiperException('mDNS start failed: ${_readErr(ctx.bindings, errPtr)}');
      }
      _ctx = ctx;
    } finally {
      calloc.free(svcPtr);
      calloc.free(errPtr);
    }

    final rule = autoJoin;
    if (rule == null) return;
    final allowed = <String>[];
    for (final peer in discoverPeers()) {
      if (await rule(peer, ctx)) allowed.add(peer);
    }
    if (allowed.isEmpty) return;
    try {
      ctx.join(allowed);
    } on GossiperException {
      // no reachable peer yet — keep advertising; others will join us
    }
  }

  /// Queries the LAN for nodes advertising [serviceName].
  ///
  /// Returns `"addr:port"` strings, excluding this node. Blocks for up to
  /// [timeout] (defaults to [lookupTimeout]). Requires [start] to have run.
  List<String> discoverPeers({Duration? timeout}) {
    final ctx = _requireStarted();
    final svcPtr = serviceName.toNativeUtf8();
    try {
      final ptr = ctx.bindings.memberlistDiscoverMDNS(
        svcPtr.cast(),
        (timeout ?? lookupTimeout).inMilliseconds,
      );
      final json = ptr.cast<Utf8>().toDartString();
      ctx.bindings.freeString(ptr.cast());
      final decoded = jsonDecode(json);
      if (decoded is! List) return [];
      return decoded.cast<String>().where((addr) => addr != ctx.selfAddr).toList();
    } finally {
      calloc.free(svcPtr);
    }
  }

  /// Discovers peers via mDNS and joins all of them, regardless of [autoJoin].
  ///
  /// Returns the number of seed nodes contacted, or 0 if none were found.
  /// Throws [GossiperException] if peers were found but none could be joined.
  int joinPeers({Duration? timeout}) {
    final ctx = _requireStarted();
    final peers = discoverPeers(timeout: timeout);
    if (peers.isEmpty) return 0;
    return ctx.join(peers);
  }

  @override
  void stop() {
    final ctx = _ctx;
    if (ctx == null) return;
    ctx.bindings.memberlistStopMDNS(ctx.handle);
    _ctx = null;
  }

  NativeDiscoveryContext _requireStarted() {
    final ctx = _ctx;
    if (ctx == null) throw GossiperException('MdnsDiscovery is not started');
    return ctx;
  }

  static String _readErr(GossiperBindings bindings, Pointer<Pointer<Utf8>> errPtr) {
    if (errPtr.value == nullptr) return 'unknown error';
    final msg = errPtr.value.cast<Utf8>().toDartString();
    bindings.freeString(errPtr.value.cast());
    errPtr.value = nullptr;
    return msg;
  }
}
