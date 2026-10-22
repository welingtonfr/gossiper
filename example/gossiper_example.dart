import 'dart:io';
import 'package:gossiper/gossiper.dart';

void main(List<String> args) async {
  final explicitSeeds = args.toList();

  final node = Gossiper.create(
    name: 'node-$pid',
    bindAddr: '0.0.0.0',
    bindPort: 7946,
  );

  stdout.writeln('Local node: ${node.localNode()}');

  // Membership events
  node.events().listen((e) {
    stdout.writeln('[event] ${e.type.name.padRight(6)} ${e.node}');
  });

  // Broadcast messages received from peers
  node.messages().listen((msg) {
    stdout.writeln('[msg]   $msg');
  });

  // Advertise on LAN so other nodes can discover this one
  node.startDiscovery(serviceName: '_gossiper._tcp');
  stdout.writeln('mDNS advertisement started.');

  if (explicitSeeds.isNotEmpty) {
    final n = node.join(explicitSeeds);
    stdout.writeln('Joined via explicit seeds — contacted $n node(s).');
  } else {
    stdout.writeln('Scanning for peers via mDNS (2 s)…');
    final contacted = node.joinViaDiscovery(
      serviceName: '_gossiper._tcp',
      discoveryTimeout: const Duration(seconds: 2),
    );
    if (contacted > 0) {
      stdout.writeln('Joined cluster via mDNS — contacted $contacted node(s).');
    } else {
      stdout.writeln('No peers found — running as bootstrap node.');
    }
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
