import 'dart:io';
import 'package:gossiper/gossiper.dart';

void main(List<String> args) async {
  // Optional: pass explicit seed addresses as arguments.
  // Without them the node uses mDNS to discover peers automatically.
  final explicitSeeds = args.toList();

  final gossiper = Gossiper.create(
    name: 'node-$pid',
    bindAddr: '0.0.0.0',
    bindPort: 7946,
    // libPath: r'blobs\windows\gossiper.dll', // override if needed
  );

  print('Local node: ${gossiper.localNode()}');

  // Subscribe to join/leave/update events before doing anything else
  gossiper.events().listen((event) {
    print('[event] ${event.type.name.padRight(6)} ${event.node}');
  });

  // Advertise ourselves so other nodes can find us automatically
  gossiper.startDiscovery(serviceName: '_gossiper._tcp');
  print('mDNS advertisement started.');

  if (explicitSeeds.isNotEmpty) {
    final n = gossiper.join(explicitSeeds);
    print('Joined via explicit seeds — contacted $n node(s).');
  } else {
    // Auto-discover peers on the LAN and join them
    print('Scanning for peers via mDNS (2 s)…');
    final contacted = gossiper.joinViaDiscovery(
      serviceName: '_gossiper._tcp',
      discoveryTimeout: const Duration(seconds: 2),
    );
    if (contacted > 0) {
      print('Joined cluster via mDNS — contacted $contacted node(s).');
    } else {
      print('No peers found — running as bootstrap node, waiting for others.');
    }
  }

  // Print member list every 3 seconds
  for (var i = 0; i < 5; i++) {
    await Future.delayed(const Duration(seconds: 3));
    final members = gossiper.members();
    final names = members.map((m) => m.name).join(', ');
    print('Members (${members.length}): $names');
  }

  gossiper.leave();
  gossiper.dispose();
}
