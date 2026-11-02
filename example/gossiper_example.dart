import 'dart:io';
import 'package:gossiper/gossiper.dart';

void main(List<String> args) async {
  final explicitSeeds = args.toList();

  final node = Gossiper.create(
    name: 'node-$pid',
    bindAddr: '0.0.0.0',
    bindPort: 7946,
    // Saves every message; history is loaded back on startDiscovery
    messageStore: JsonLinesMessageStore('gossiper-$pid.log'),
  );

  stdout.writeln('Local node: ${node.localNode()}');

  // Membership events
  node.events().listen((e) {
    stdout.writeln('[event] ${e.type.name.padRight(6)} ${e.node}');
  });

  // Broadcast messages received from peers
  node.messages().listen((msg) {
    stdout.writeln('[msg]   ${msg.data}  (id=${msg.id})');
  });

  // Advertise on LAN and join any peers found. With explicit seeds, mDNS
  // only advertises so the lookup doesn't delay the join below.
  final mdns = MdnsDiscovery(
    serviceName: '_gossiper._tcp',
    autoJoin: explicitSeeds.isEmpty ? AutoJoinRules.always : null,
  );
  await node.startDiscovery([mdns, BeaconDiscovery()]);
  stdout.writeln('Discovery started (mDNS + beacon).');

  if (explicitSeeds.isNotEmpty) {
    final n = node.join(explicitSeeds);
    stdout.writeln('Joined via explicit seeds — contacted $n node(s).');
  } else if (node.numMembers() > 1) {
    stdout.writeln('Joined cluster via mDNS — ${node.numMembers()} member(s).');
  } else {
    stdout.writeln('No peers found yet — running as bootstrap node (beacon keeps listening).');
  }

  // Broadcast a message to every peer in the cluster
  node.broadcast('{"from":"node-$pid","hello":"world"}');
  stdout.writeln('Broadcast sent.');

  // Print member list every 3 seconds
  for (var i = 0; i < 5; i++) {
    await Future.delayed(const Duration(seconds: 3));
    final members = node.members();
    final names = members.map((m) => m.name).join(', ');
    stdout.writeln('Members (${members.length}): $names');
  }

  node.leave();
  node.dispose();
}
