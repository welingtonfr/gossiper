import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../beacon_utils.dart';
import '../domain/contracts/auto_join_rule.dart';
import '../domain/contracts/discovery_context.dart';
import '../domain/contracts/discovery_strategy.dart';

/// Broadcasts this node's address on the /24 subnet every [interval] and
/// listens for other nodes' beacons.
///
/// Each heard peer is joined when [autoJoin] allows it — by default only
/// while this node is alone ([AutoJoinRules.whenAlone]). Pass
/// `autoJoin: null` to only advertise.
///
/// [beaconType] scopes the beacon to a logical cluster — nodes with
/// different types on the same network will not discover each other.
///
/// Failure to open the UDP socket is non-fatal: [start] completes normally
/// and the strategy stays idle.
class BeaconDiscovery implements DiscoveryStrategy {
  final int port;
  final Duration interval;
  final String beaconType;
  final AutoJoinRule? autoJoin;

  RawDatagramSocket? _socket;
  Timer? _timer;

  BeaconDiscovery({
    this.port = 7947,
    this.interval = const Duration(seconds: 5),
    this.beaconType = 'gossiper_hello',
    this.autoJoin = AutoJoinRules.whenAlone,
  });

  @override
  Future<void> start(DiscoveryContext ctx) async {
    stop();
    try {
      final socket = await RawDatagramSocket.bind(InternetAddress.anyIPv4, port);
      socket.broadcastEnabled = true;
      _socket = socket;
      socket.listen((event) {
        if (event != RawSocketEvent.read) return;
        final dg = _socket?.receive();
        if (dg == null) return;
        final addr = BeaconUtils.parseBeacon(
          payload: utf8.decode(dg.data, allowMalformed: true),
          selfAddr: ctx.selfAddr,
          beaconType: beaconType,
        );
        if (addr != null) _maybeJoin(addr, ctx);
      });

      _send(ctx);
      _timer = Timer.periodic(interval, (_) => _send(ctx));
    } catch (_) {
      // beacon failure is non-fatal — other strategies may still work
    }
  }

  Future<void> _maybeJoin(String addr, DiscoveryContext ctx) async {
    final rule = autoJoin;
    if (rule == null || _socket == null) return;
    if (await rule(addr, ctx) && _socket != null) ctx.join([addr]);
  }

  void _send(DiscoveryContext ctx) {
    final socket = _socket;
    if (socket == null) return;
    final payload = Uint8List.fromList(
      utf8.encode(jsonEncode({'type': beaconType, 'addr': ctx.selfAddr})),
    );
    try {
      socket.send(payload, InternetAddress(BeaconUtils.subnetBroadcast(ctx.lanIp)), port);
    } catch (_) {}
  }

  @override
  void stop() {
    _timer?.cancel();
    _timer = null;
    _socket?.close();
    _socket = null;
  }
}
