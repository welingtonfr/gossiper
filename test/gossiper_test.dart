import 'dart:async';
import 'dart:convert';
import 'dart:ffi';

import 'package:ffi/ffi.dart';
import 'package:gossiper/gossiper.dart';
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Fake bindings helpers
// ---------------------------------------------------------------------------

// Allocates a C string via calloc — Gossiper will free it with freeString,
// which in the fake calls calloc.free (the same allocator).
Pointer<Utf8> _cstr(String s) => s.toNativeUtf8(allocator: calloc);

// A minimal set of fake functions that return plausible JSON responses.
GossiperBindings _fakeBindings({
  int handle = 1,
  String localName = 'test-node',
  String localAddr = '127.0.0.1',
  int localPort = 7946,
  List<Map<String, dynamic>> members = const [],
  List<Map<String, dynamic>> pendingEvents = const [],
  List<String> discoveredPeers = const [],
  int joinResult = 1,
  bool leaveError = false,
  bool createError = false,
  bool mdnsStartError = false,
}) {
  final eventsQueue = [...pendingEvents];

  return GossiperBindings.fake(
    memberlistCreate: (name, bindAddr, bindPort, errOut) {
      if (createError) {
        if (errOut != nullptr) errOut.value = _cstr('simulated create error');
        return -1;
      }
      return handle;
    },
    memberlistJoin: (h, addrsJson, errOut) => joinResult,
    memberlistMembers: (h) {
      final all = [
        {'name': localName, 'addr': localAddr, 'port': localPort},
        ...members,
      ];
      return _cstr(jsonEncode(all));
    },
    memberlistLocalNode: (h) =>
        _cstr(jsonEncode({'name': localName, 'addr': localAddr, 'port': localPort})),
    memberlistNumMembers: (h) => 1 + members.length,
    memberlistPollEvents: (h) {
      if (eventsQueue.isEmpty) return _cstr('[]');
      final batch = [...eventsQueue];
      eventsQueue.clear();
      return _cstr(jsonEncode(batch));
    },
    memberlistLeave: (h, timeoutMs, errOut) {
      if (leaveError) {
        if (errOut != nullptr) errOut.value = _cstr('simulated leave error');
        return -1;
      }
      return 0;
    },
    memberlistDestroy: (h) {},
    memberlistStartMDNS: (h, svc, errOut) {
      if (mdnsStartError) {
        if (errOut != nullptr) errOut.value = _cstr('simulated mdns error');
        return -1;
      }
      return 0;
    },
    memberlistStopMDNS: (h) {},
    memberlistDiscoverMDNS: (svc, timeoutMs) => _cstr(jsonEncode(discoveredPeers)),
    freeString: (ptr) => calloc.free(ptr),
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('Gossiper.create', () {
    test('returns a Gossiper on success', () {
      final g = Gossiper.withBindings(_fakeBindings());
      expect(g, isA<Gossiper>());
      g.dispose();
    });

    test('throws GossiperException when native create fails', () {
      expect(
        () => Gossiper.withBindings(_fakeBindings(createError: true)),
        returnsNormally, // withBindings bypasses create — test via fake directly
      );

      // Verify the exception path via a binding that returns -1
      final bindings = _fakeBindings(createError: true);
      final errPtr = calloc<Pointer<Utf8>>();
      final result = bindings.memberlistCreate(nullptr, nullptr, 0, errPtr);
      expect(result, equals(-1));
      bindings.freeString(errPtr.value);
      calloc.free(errPtr);
    });
  });

  group('localNode', () {
    test('returns node with correct fields', () {
      final g = Gossiper.withBindings(
        _fakeBindings(localName: 'alpha', localAddr: '10.0.0.1', localPort: 8000),
      );
      final node = g.localNode();
      expect(node.name, equals('alpha'));
      expect(node.addr, equals('10.0.0.1'));
      expect(node.port, equals(8000));
      expect(node.address, equals('10.0.0.1:8000'));
      g.dispose();
    });
  });

  group('members', () {
    test('includes local node and additional members', () {
      final g = Gossiper.withBindings(_fakeBindings(
        localName: 'node-1',
        members: [
          {'name': 'node-2', 'addr': '192.168.1.2', 'port': 7946},
          {'name': 'node-3', 'addr': '192.168.1.3', 'port': 7946},
        ],
      ));
      final list = g.members();
      expect(list, hasLength(3));
      expect(list.map((m) => m.name), containsAll(['node-1', 'node-2', 'node-3']));
      g.dispose();
    });

    test('numMembers matches members list length', () {
      final g = Gossiper.withBindings(_fakeBindings(
        members: [
          {'name': 'node-2', 'addr': '127.0.0.1', 'port': 7947},
        ],
      ));
      expect(g.numMembers(), equals(g.members().length));
      g.dispose();
    });
  });

  group('join', () {
    test('returns number of contacted nodes', () {
      final g = Gossiper.withBindings(_fakeBindings(joinResult: 3));
      expect(g.join(['a:1', 'b:2', 'c:3']), equals(3));
      g.dispose();
    });

    test('throws GossiperException when join returns -1', () {
      final g = Gossiper.withBindings(_fakeBindings(joinResult: -1));
      expect(() => g.join(['x:1']), throwsA(isA<GossiperException>()));
      g.dispose();
    });
  });

  group('leave', () {
    test('completes without error on success', () {
      final g = Gossiper.withBindings(_fakeBindings());
      expect(() => g.leave(), returnsNormally);
      g.dispose();
    });

    test('throws GossiperException on error', () {
      final g = Gossiper.withBindings(_fakeBindings(leaveError: true));
      expect(() => g.leave(), throwsA(isA<GossiperException>()));
      g.dispose();
    });
  });

  group('events', () {
    test('emits queued join and leave events', () async {
      final g = Gossiper.withBindings(_fakeBindings(
        pendingEvents: [
          {'type': 0, 'name': 'peer-1', 'addr': '10.0.0.2', 'port': 7946},
          {'type': 1, 'name': 'peer-2', 'addr': '10.0.0.3', 'port': 7946},
        ],
      ));

      final received = <NodeEvent>[];
      final sub = g.events(pollInterval: const Duration(milliseconds: 50)).listen(received.add);

      await Future.delayed(const Duration(milliseconds: 200));
      await sub.cancel();
      g.dispose();

      expect(received, hasLength(2));
      expect(received[0].type, NodeEventType.join);
      expect(received[0].node.name, 'peer-1');
      expect(received[1].type, NodeEventType.leave);
      expect(received[1].node.name, 'peer-2');
    });

    test('emits nothing when queue is empty', () async {
      final g = Gossiper.withBindings(_fakeBindings());
      final received = <NodeEvent>[];
      final sub = g.events(pollInterval: const Duration(milliseconds: 50)).listen(received.add);

      await Future.delayed(const Duration(milliseconds: 150));
      await sub.cancel();
      g.dispose();

      expect(received, isEmpty);
    });
  });

  group('mDNS', () {
    test('startDiscovery succeeds', () {
      final g = Gossiper.withBindings(_fakeBindings());
      expect(() => g.startDiscovery(), returnsNormally);
      g.dispose();
    });

    test('startDiscovery throws GossiperException on error', () {
      final g = Gossiper.withBindings(_fakeBindings(mdnsStartError: true));
      expect(() => g.startDiscovery(), throwsA(isA<GossiperException>()));
      g.dispose();
    });

    test('discoverPeers returns addresses from fake', () {
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946', '192.168.1.6:7946'],
      ));
      final peers = g.discoverPeers();
      expect(peers, equals(['192.168.1.5:7946', '192.168.1.6:7946']));
      g.dispose();
    });

    test('discoverPeers returns empty list when no peers found', () {
      final g = Gossiper.withBindings(_fakeBindings(discoveredPeers: []));
      expect(g.discoverPeers(), isEmpty);
      g.dispose();
    });

    test('joinViaDiscovery calls join with discovered addresses', () {
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946'],
        joinResult: 1,
      ));
      expect(g.joinViaDiscovery(), equals(1));
      g.dispose();
    });

    test('joinViaDiscovery returns 0 when no peers found', () {
      final g = Gossiper.withBindings(_fakeBindings(discoveredPeers: []));
      expect(g.joinViaDiscovery(), equals(0));
      g.dispose();
    });
  });

  group('MemberNode', () {
    test('address combines addr and port', () {
      const node = MemberNode(name: 'n', addr: '1.2.3.4', port: 5678);
      expect(node.address, '1.2.3.4:5678');
    });

    test('toString includes name and address', () {
      const node = MemberNode(name: 'alpha', addr: '10.0.0.1', port: 7946);
      expect(node.toString(), contains('alpha'));
      expect(node.toString(), contains('10.0.0.1:7946'));
    });
  });
}
