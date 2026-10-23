import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';
import 'package:gossiper/gossiper.dart';
import 'package:gossiper/src/beacon_utils.dart';
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Fake bindings helpers
// ---------------------------------------------------------------------------

Pointer<Utf8> _cstr(String s) => s.toNativeUtf8(allocator: calloc);

GossiperBindings _fakeBindings({
  int handle = 1,
  String localName = 'test-node',
  String localAddr = '127.0.0.1',
  int localPort = 7946,
  List<Map<String, dynamic>> members = const [],
  List<Map<String, dynamic>> pendingEvents = const [],
  List<String> pendingMessages = const [],
  List<String> discoveredPeers = const [],
  int joinResult = 1,
  bool leaveError = false,
  bool createError = false,
  bool mdnsStartError = false,
  bool broadcastError = false,
}) {
  final eventsQueue = [...pendingEvents];
  final messagesQueue = [...pendingMessages];

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
    memberlistBroadcast: (h, data, errOut) {
      if (broadcastError) {
        if (errOut != nullptr) errOut.value = _cstr('simulated broadcast error');
        return -1;
      }
      return 0;
    },
    memberlistPollMessages: (h) {
      if (messagesQueue.isEmpty) return _cstr('[]');
      final batch = [...messagesQueue];
      messagesQueue.clear();
      return _cstr(jsonEncode(batch));
    },
    memberlistSetReplayWindow: (h, maxEntries, windowHours) {},
    memberlistSetPersistPath: (h, path) {},
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
        members: [{'name': 'node-2', 'addr': '127.0.0.1', 'port': 7947}],
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

  group('broadcast', () {
    test('succeeds without error', () {
      final g = Gossiper.withBindings(_fakeBindings());
      expect(() => g.broadcast('{"op":"ping"}'), returnsNormally);
      g.dispose();
    });

    test('throws GossiperException on error', () {
      final g = Gossiper.withBindings(_fakeBindings(broadcastError: true));
      expect(() => g.broadcast('hello'), throwsA(isA<GossiperException>()));
      g.dispose();
    });
  });

  group('messages', () {
    test('emits messages queued in the fake', () async {
      final g = Gossiper.withBindings(_fakeBindings(
        pendingMessages: ['{"op":"set","key":"x","val":1}', 'hello'],
      ));

      final received = <String>[];
      final sub = g.messages(pollInterval: const Duration(milliseconds: 50)).listen(received.add);

      await Future.delayed(const Duration(milliseconds: 200));
      await sub.cancel();
      g.dispose();

      expect(received, hasLength(2));
      expect(received[0], '{"op":"set","key":"x","val":1}');
      expect(received[1], 'hello');
    });

    test('emits nothing when no messages pending', () async {
      final g = Gossiper.withBindings(_fakeBindings());
      final received = <String>[];
      final sub = g.messages(pollInterval: const Duration(milliseconds: 50)).listen(received.add);

      await Future.delayed(const Duration(milliseconds: 150));
      await sub.cancel();
      g.dispose();

      expect(received, isEmpty);
    });

    test('events and messages can both be active simultaneously', () async {
      final g = Gossiper.withBindings(_fakeBindings(
        pendingEvents: [
          {'type': 0, 'name': 'peer-1', 'addr': '10.0.0.2', 'port': 7946},
        ],
        pendingMessages: ['ping'],
      ));

      final events = <NodeEvent>[];
      final msgs = <String>[];
      final s1 = g.events(pollInterval: const Duration(milliseconds: 50)).listen(events.add);
      final s2 = g.messages(pollInterval: const Duration(milliseconds: 50)).listen(msgs.add);

      await Future.delayed(const Duration(milliseconds: 200));
      await s1.cancel();
      await s2.cancel();
      g.dispose();

      expect(events, hasLength(1));
      expect(msgs, hasLength(1));
      expect(msgs.first, 'ping');
    });
  });

  group('setPersistPath', () {
    test('completes without error', () {
      final g = Gossiper.withBindings(_fakeBindings());
      expect(() => g.setPersistPath('/tmp/gossiper_test.log'), returnsNormally);
      g.dispose();
    });

    test('create applies persistPath', () {
      var capturedPath = '';
      final bindings = _fakeBindings();
      final b = GossiperBindings.fake(
        memberlistCreate: bindings.memberlistCreate,
        memberlistJoin: bindings.memberlistJoin,
        memberlistMembers: bindings.memberlistMembers,
        memberlistLocalNode: bindings.memberlistLocalNode,
        memberlistNumMembers: bindings.memberlistNumMembers,
        memberlistPollEvents: bindings.memberlistPollEvents,
        memberlistLeave: bindings.memberlistLeave,
        memberlistDestroy: bindings.memberlistDestroy,
        memberlistStartMDNS: bindings.memberlistStartMDNS,
        memberlistStopMDNS: bindings.memberlistStopMDNS,
        memberlistDiscoverMDNS: bindings.memberlistDiscoverMDNS,
        memberlistBroadcast: bindings.memberlistBroadcast,
        memberlistPollMessages: bindings.memberlistPollMessages,
        memberlistSetReplayWindow: bindings.memberlistSetReplayWindow,
        memberlistSetPersistPath: (h, path) {
          capturedPath = path.cast<Utf8>().toDartString();
        },
        freeString: bindings.freeString,
      );
      final g = Gossiper.withBindings(b);
      g.setPersistPath('/data/gossiper.log');
      expect(capturedPath, equals('/data/gossiper.log'));
      g.dispose();
    });
  });

  group('setReplayWindow', () {
    test('completes without error with default params', () {
      final g = Gossiper.withBindings(_fakeBindings());
      expect(() => g.setReplayWindow(), returnsNormally);
      g.dispose();
    });

    test('create applies replayMaxEntries and replayWindow', () {
      var capturedEntries = 0;
      var capturedHours = 0;
      final bindings = _fakeBindings();
      // override setReplayWindow to capture what was passed
      final b = GossiperBindings.fake(
        memberlistCreate: bindings.memberlistCreate,
        memberlistJoin: bindings.memberlistJoin,
        memberlistMembers: bindings.memberlistMembers,
        memberlistLocalNode: bindings.memberlistLocalNode,
        memberlistNumMembers: bindings.memberlistNumMembers,
        memberlistPollEvents: bindings.memberlistPollEvents,
        memberlistLeave: bindings.memberlistLeave,
        memberlistDestroy: bindings.memberlistDestroy,
        memberlistStartMDNS: bindings.memberlistStartMDNS,
        memberlistStopMDNS: bindings.memberlistStopMDNS,
        memberlistDiscoverMDNS: bindings.memberlistDiscoverMDNS,
        memberlistBroadcast: bindings.memberlistBroadcast,
        memberlistPollMessages: bindings.memberlistPollMessages,
        memberlistSetReplayWindow: (h, max, hours) {
          capturedEntries = max;
          capturedHours = hours;
        },
        memberlistSetPersistPath: bindings.memberlistSetPersistPath,
        freeString: bindings.freeString,
      );
      final g = Gossiper.withBindings(b);
      g.setReplayWindow(maxEntries: 200, window: const Duration(hours: 12));
      expect(capturedEntries, equals(200));
      expect(capturedHours, equals(12));
      g.dispose();
    });
  });

  group('mDNS', () {
    test('startDiscovery succeeds', () async {
      final g = Gossiper.withBindings(_fakeBindings());
      await g.startDiscovery();
      g.dispose();
    });

    test('startDiscovery throws GossiperException on mDNS error', () async {
      final g = Gossiper.withBindings(_fakeBindings(mdnsStartError: true));
      await expectLater(g.startDiscovery(), throwsA(isA<GossiperException>()));
      g.dispose();
    });

    test('discoverPeers returns addresses from fake', () {
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946', '192.168.1.6:7946'],
      ));
      expect(g.discoverPeers(), equals(['192.168.1.5:7946', '192.168.1.6:7946']));
      g.dispose();
    });

    test('discoverPeers returns empty list when no peers found', () {
      final g = Gossiper.withBindings(_fakeBindings(discoveredPeers: []));
      expect(g.discoverPeers(), isEmpty);
      g.dispose();
    });

    test('discoverPeers returns empty list when native returns null', () {
      final bindings = _fakeBindings();
      final nullBindings = GossiperBindings.fake(
        memberlistCreate: bindings.memberlistCreate,
        memberlistJoin: bindings.memberlistJoin,
        memberlistMembers: bindings.memberlistMembers,
        memberlistLocalNode: bindings.memberlistLocalNode,
        memberlistNumMembers: bindings.memberlistNumMembers,
        memberlistPollEvents: bindings.memberlistPollEvents,
        memberlistLeave: bindings.memberlistLeave,
        memberlistDestroy: bindings.memberlistDestroy,
        memberlistStartMDNS: bindings.memberlistStartMDNS,
        memberlistStopMDNS: bindings.memberlistStopMDNS,
        memberlistDiscoverMDNS: (svc, timeout) => _cstr('null'),
        memberlistBroadcast: bindings.memberlistBroadcast,
        memberlistPollMessages: bindings.memberlistPollMessages,
        memberlistSetReplayWindow: bindings.memberlistSetReplayWindow,
        memberlistSetPersistPath: bindings.memberlistSetPersistPath,
        freeString: bindings.freeString,
      );
      final g = Gossiper.withBindings(nullBindings);
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

  group('BeaconUtils.subnetBroadcast', () {
    test('returns .255 broadcast for 192.168.x.x', () {
      expect(BeaconUtils.subnetBroadcast('192.168.1.5'), equals('192.168.1.255'));
    });

    test('returns .255 broadcast for 10.x.x.x', () {
      expect(BeaconUtils.subnetBroadcast('10.0.0.1'), equals('10.0.0.255'));
    });

    test('returns .255 broadcast for 172.x.x.x', () {
      expect(BeaconUtils.subnetBroadcast('172.20.0.5'), equals('172.20.0.255'));
    });

    test('returns 255.255.255.255 for invalid IP', () {
      expect(BeaconUtils.subnetBroadcast('not-an-ip'), equals('255.255.255.255'));
    });

    test('returns 255.255.255.255 for empty string', () {
      expect(BeaconUtils.subnetBroadcast(''), equals('255.255.255.255'));
    });
  });

  group('BeaconUtils.isVirtualInterface', () {
    NetworkInterface iface(String name) => _FakeNetworkInterface(name);

    test('docker0 is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('docker0'), '172.17.0.1'), isTrue);
    });

    test('br-abc123 is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('br-abc123'), '172.18.0.1'), isTrue);
    });

    test('veth12345 is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('veth12345'), '10.0.0.1'), isTrue);
    });

    test('vboxnet0 is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('vboxnet0'), '192.168.56.1'), isTrue);
    });

    test('vmnet8 is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('vmnet8'), '192.168.204.1'), isTrue);
    });

    test('virbr0 is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('virbr0'), '192.168.122.1'), isTrue);
    });

    test('172.16-31 range is virtual regardless of interface name', () {
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '172.16.0.1'), isTrue);
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '172.31.255.254'), isTrue);
    });

    test('172.15 and 172.32 are NOT virtual by range', () {
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '172.15.0.1'), isFalse);
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '172.32.0.1'), isFalse);
    });

    test('VirtualBox host-only 192.168.56.x is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '192.168.56.100'), isTrue);
    });

    test('KVM 192.168.122.x is virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '192.168.122.50'), isTrue);
    });

    test('regular LAN addresses are not virtual', () {
      expect(BeaconUtils.isVirtualInterface(iface('eth0'), '192.168.1.5'), isFalse);
      expect(BeaconUtils.isVirtualInterface(iface('wlan0'), '10.0.0.50'), isFalse);
      expect(BeaconUtils.isVirtualInterface(iface('en0'), '192.168.0.100'), isFalse);
    });
  });

  group('BeaconUtils.processBeacon', () {
    test('calls onJoin with remote addr when beacon is valid', () {
      String? joined;
      BeaconUtils.processBeacon(
        payload: '{"type":"test_hello","addr":"192.168.1.10:7946"}',
        selfAddr: '192.168.1.1:7946',
        beaconType: 'test_hello',
        numMembers: 1,
        onJoin: (addr) => joined = addr,
      );
      expect(joined, equals('192.168.1.10:7946'));
    });

    test('ignores beacon from self', () {
      String? joined;
      BeaconUtils.processBeacon(
        payload: '{"type":"test_hello","addr":"192.168.1.1:7946"}',
        selfAddr: '192.168.1.1:7946',
        beaconType: 'test_hello',
        numMembers: 1,
        onJoin: (addr) => joined = addr,
      );
      expect(joined, isNull);
    });

    test('ignores beacon with wrong type', () {
      String? joined;
      BeaconUtils.processBeacon(
        payload: '{"type":"other_app","addr":"192.168.1.10:7946"}',
        selfAddr: '192.168.1.1:7946',
        beaconType: 'test_hello',
        numMembers: 1,
        onJoin: (addr) => joined = addr,
      );
      expect(joined, isNull);
    });

    test('does not join when already in a cluster (numMembers > 1)', () {
      String? joined;
      BeaconUtils.processBeacon(
        payload: '{"type":"test_hello","addr":"192.168.1.10:7946"}',
        selfAddr: '192.168.1.1:7946',
        beaconType: 'test_hello',
        numMembers: 3,
        onJoin: (addr) => joined = addr,
      );
      expect(joined, isNull);
    });

    test('ignores malformed JSON without throwing', () {
      expect(
        () => BeaconUtils.processBeacon(
          payload: 'not-json',
          selfAddr: '192.168.1.1:7946',
          beaconType: 'test_hello',
          numMembers: 1,
          onJoin: (_) {},
        ),
        returnsNormally,
      );
    });

    test('ignores beacon missing addr field', () {
      String? joined;
      BeaconUtils.processBeacon(
        payload: '{"type":"test_hello"}',
        selfAddr: '192.168.1.1:7946',
        beaconType: 'test_hello',
        numMembers: 1,
        onJoin: (addr) => joined = addr,
      );
      expect(joined, isNull);
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

class _FakeNetworkInterface implements NetworkInterface {
  _FakeNetworkInterface(this.name);

  @override
  final String name;

  @override
  int get index => 0;

  @override
  List<InternetAddress> get addresses => [];
}
