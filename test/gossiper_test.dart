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
  List<List<String>>? joinCalls,
  List<String>? restoredLogs,
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
    memberlistJoin: (h, addrsJson, errOut) {
      joinCalls?.add((jsonDecode(addrsJson.toDartString()) as List).cast<String>());
      return joinResult;
    },
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
        return nullptr;
      }
      return _cstr(jsonEncode({'id': 'sent-1', 'data': data.toDartString(), 'at': 42}));
    },
    memberlistPollMessages: (h) {
      if (messagesQueue.isEmpty) return _cstr('[]');
      final batch = [
        for (final (i, m) in messagesQueue.indexed) {'id': 'msg-$i', 'data': m, 'at': i},
      ];
      messagesQueue.clear();
      return _cstr(jsonEncode(batch));
    },
    memberlistSetReplayWindow: (h, maxEntries, windowHours) {},
    memberlistRestoreLog: (h, entriesJson, errOut) {
      restoredLogs?.add(entriesJson.toDartString());
      return 0;
    },

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
    test('returns the sent message', () {
      final g = Gossiper.withBindings(_fakeBindings());
      final m = g.broadcast('{"op":"ping"}');
      expect(m.id, equals('sent-1'));
      expect(m.data, equals('{"op":"ping"}'));
      expect(m.at, equals(42));
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

      final received = <Message>[];
      final sub = g.messages(pollInterval: const Duration(milliseconds: 50)).listen(received.add);

      await Future.delayed(const Duration(milliseconds: 200));
      await sub.cancel();
      g.dispose();

      expect(received, hasLength(2));
      expect(received[0].data, '{"op":"set","key":"x","val":1}');
      expect(received[0].id, 'msg-0');
      expect(received[1].data, 'hello');
    });

    test('emits nothing when no messages pending', () async {
      final g = Gossiper.withBindings(_fakeBindings());
      final received = <Message>[];
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
      final msgs = <Message>[];
      final s1 = g.events(pollInterval: const Duration(milliseconds: 50)).listen(events.add);
      final s2 = g.messages(pollInterval: const Duration(milliseconds: 50)).listen(msgs.add);

      await Future.delayed(const Duration(milliseconds: 200));
      await s1.cancel();
      await s2.cancel();
      g.dispose();

      expect(events, hasLength(1));
      expect(msgs, hasLength(1));
      expect(msgs.first.data, 'ping');
    });
  });

  group('message store', () {
    final noDiscovery = <DiscoveryStrategy>[];

    test('broadcast saves the sent message', () async {
      final store = _FakeMessageStore();
      final g = Gossiper.withBindings(_fakeBindings(), messageStore: store);
      final sent = g.broadcast('hello');
      await Future<void>.delayed(Duration.zero);
      expect(store.messages.single.id, equals(sent.id));
      expect(store.messages.single.data, equals('hello'));
      g.dispose();
    });

    test('received messages are saved', () async {
      final store = _FakeMessageStore();
      final g = Gossiper.withBindings(
        _fakeBindings(pendingMessages: ['a', 'b']),
        messageStore: store,
      );
      final sub = g.messages(pollInterval: const Duration(milliseconds: 20)).listen((_) {});
      await Future<void>.delayed(const Duration(milliseconds: 100));
      await sub.cancel();
      expect(store.messages.map((m) => m.data), equals(['a', 'b']));
      g.dispose();
    });

    test('works without a store', () async {
      final g = Gossiper.withBindings(_fakeBindings(pendingMessages: ['a']));
      expect(() => g.broadcast('x'), returnsNormally);
      await expectLater(g.startDiscovery(noDiscovery), completes);
      g.dispose();
    });

    test('first startDiscovery loads history within the replay window', () async {
      final now = DateTime.now().millisecondsSinceEpoch;
      final store = _FakeMessageStore([
        Message(id: 'old', data: 'x', at: now - const Duration(hours: 25).inMilliseconds),
        Message(id: 'new', data: 'y', at: now - 1000),
      ]);
      final restored = <String>[];
      final g = Gossiper.withBindings(_fakeBindings(restoredLogs: restored), messageStore: store);
      await g.startDiscovery(noDiscovery);
      final ids = (jsonDecode(restored.single) as List).map((e) => e['id']).toList();
      expect(ids, equals(['new']));
      expect(store.messages, hasLength(2), reason: 'library never deletes from the store');
      g.dispose();
    });

    test('history load respects replayMaxEntries', () async {
      final now = DateTime.now().millisecondsSinceEpoch;
      final store = _FakeMessageStore([
        for (var i = 0; i < 5; i++) Message(id: 'm$i', data: '', at: now - 5000 + i),
      ]);
      final restored = <String>[];
      final g = Gossiper.withBindings(_fakeBindings(restoredLogs: restored), messageStore: store);
      g.setReplayWindow(maxEntries: 2);
      await g.startDiscovery(noDiscovery);
      final ids = (jsonDecode(restored.single) as List).map((e) => e['id']).toList();
      expect(ids, equals(['m3', 'm4']));
      g.dispose();
    });

    test('history is loaded before any strategy starts', () async {
      final now = DateTime.now().millisecondsSinceEpoch;
      final restored = <String>[];
      final strategy = _FakeStrategy();
      var restoredBeforeStart = false;
      final g = Gossiper.withBindings(
        _fakeBindings(restoredLogs: restored),
        messageStore: _FakeMessageStore([Message(id: 'a', data: '', at: now)]),
      );
      strategy.onStart = () => restoredBeforeStart = restored.isNotEmpty;
      await g.startDiscovery([strategy]);
      expect(restoredBeforeStart, isTrue);
      g.dispose();
    });

    test('history is loaded only on the first startDiscovery', () async {
      final now = DateTime.now().millisecondsSinceEpoch;
      final restored = <String>[];
      final g = Gossiper.withBindings(
        _fakeBindings(restoredLogs: restored),
        messageStore: _FakeMessageStore([Message(id: 'a', data: '', at: now)]),
      );
      await g.startDiscovery(noDiscovery);
      await g.startDiscovery(noDiscovery);
      expect(restored, hasLength(1));
      g.dispose();
    });

    test('empty store does not call native restore', () async {
      final restored = <String>[];
      final g = Gossiper.withBindings(
        _fakeBindings(restoredLogs: restored),
        messageStore: _FakeMessageStore(),
      );
      await g.startDiscovery(noDiscovery);
      expect(restored, isEmpty);
      g.dispose();
    });
  });

  group('JsonLinesMessageStore', () {
    late Directory dir;
    late String path;

    setUp(() {
      dir = Directory.systemTemp.createTempSync('gossiper_store_');
      path = '${dir.path}/sub/log.jsonl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('returns empty list when file does not exist', () async {
      expect(await JsonLinesMessageStore(path).getMessagesSince(0), isEmpty);
    });

    test('saves and reads back messages oldest first, deduped by id', () async {
      final store = JsonLinesMessageStore(path);
      await Future.wait([
        store.saveMessage(const Message(id: 'b', data: '2', at: 20)),
        store.saveMessage(const Message(id: 'a', data: '1', at: 10)),
        store.saveMessage(const Message(id: 'a', data: '1', at: 10)),
      ]);
      final all = await store.getMessagesSince(0);
      expect(all.map((m) => m.id), equals(['a', 'b']));
    });

    test('getMessagesSince filters by timestamp and keeps the most recent limit', () async {
      final store = JsonLinesMessageStore(path);
      for (var i = 1; i <= 5; i++) {
        await store.saveMessage(Message(id: 'm$i', data: '', at: i * 10));
      }
      expect((await store.getMessagesSince(20)).map((m) => m.id), equals(['m3', 'm4', 'm5']));
      expect((await store.getMessagesSince(0, limit: 2)).map((m) => m.id), equals(['m4', 'm5']));
    });

    test('deleteBefore removes older messages', () async {
      final store = JsonLinesMessageStore(path);
      for (var i = 1; i <= 3; i++) {
        await store.saveMessage(Message(id: 'm$i', data: '', at: i * 10));
      }
      await store.deleteBefore(20);
      expect((await store.getMessagesSince(0)).map((m) => m.id), equals(['m2', 'm3']));
    });

    test('maxFileSizeBytes keeps the file within the limit, dropping the oldest', () async {
      // each line is {"id":"mNN","data":"xxxxxxxxxx","at":NN}\n ≈ 40 bytes
      final store = JsonLinesMessageStore(path, maxFileSizeBytes: 400);
      for (var i = 10; i < 60; i++) {
        await store.saveMessage(Message(id: 'm$i', data: 'xxxxxxxxxx', at: i));
      }
      expect(File(path).lengthSync(), lessThanOrEqualTo(400));
      final ids = (await store.getMessagesSince(0)).map((m) => m.id).toList();
      expect(ids.last, equals('m59'), reason: 'newest kept');
      expect(ids, isNot(contains('m10')), reason: 'oldest dropped');
      expect(ids, equals([...ids]..sort()), reason: 'still in order');
    });

    test('maxFileSizeBytes keeps the newest message even if it alone is too big', () async {
      final store = JsonLinesMessageStore(path, maxFileSizeBytes: 10);
      await store.saveMessage(const Message(id: 'a', data: 'way more than ten bytes', at: 1));
      await store.saveMessage(const Message(id: 'b', data: 'also more than ten bytes', at: 2));
      expect((await store.getMessagesSince(0)).map((m) => m.id), equals(['b']));
    });

    test('without maxFileSizeBytes the file is never trimmed', () async {
      final store = JsonLinesMessageStore(path);
      for (var i = 0; i < 50; i++) {
        await store.saveMessage(Message(id: 'm$i', data: 'xxxxxxxxxx', at: i));
      }
      expect(await store.getMessagesSince(-1), hasLength(50));
    });

    test('skips corrupt lines and reads files from the former persistPath format', () async {
      File(path)
        ..createSync(recursive: true)
        ..writeAsStringSync('{"id":"x","data":"hi","at":5}\nnot-json\n\n');
      final all = await JsonLinesMessageStore(path).getMessagesSince(0);
      expect(all.single.data, equals('hi'));
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
        memberlistRestoreLog: bindings.memberlistRestoreLog,
        freeString: bindings.freeString,
      );
      final g = Gossiper.withBindings(b);
      g.setReplayWindow(maxEntries: 200, window: const Duration(hours: 12));
      expect(capturedEntries, equals(200));
      expect(capturedHours, equals(12));
      g.dispose();
    });
  });

  group('MdnsDiscovery', () {
    test('start succeeds', () async {
      final g = Gossiper.withBindings(_fakeBindings());
      await g.startDiscovery([MdnsDiscovery()]);
      g.dispose();
    });

    test('start throws GossiperException on mDNS error', () async {
      final g = Gossiper.withBindings(_fakeBindings(mdnsStartError: true));
      await expectLater(g.startDiscovery([MdnsDiscovery()]), throwsA(isA<GossiperException>()));
      g.dispose();
    });

    test('start joins all discovered peers by default', () async {
      final joins = <List<String>>[];
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946', '192.168.1.6:7946'],
        joinCalls: joins,
      ));
      await g.startDiscovery([MdnsDiscovery()]);
      expect(joins, equals([['192.168.1.5:7946', '192.168.1.6:7946']]));
      g.dispose();
    });

    test('start does not join when autoJoin is null', () async {
      final joins = <List<String>>[];
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946'],
        joinCalls: joins,
      ));
      await g.startDiscovery([MdnsDiscovery(autoJoin: null)]);
      expect(joins, isEmpty);
      g.dispose();
    });

    test('start keeps advertising when joining discovered peers fails', () async {
      final mdns = MdnsDiscovery();
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946'],
        joinResult: -1,
      ));
      await g.startDiscovery([mdns]);
      expect(() => mdns.joinPeers(), throwsA(isA<GossiperException>()));
      g.dispose();
    });

    test('discoverPeers returns addresses from fake', () async {
      final mdns = MdnsDiscovery(autoJoin: null);
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946', '192.168.1.6:7946'],
      ));
      await g.startDiscovery([mdns]);
      expect(mdns.discoverPeers(), equals(['192.168.1.5:7946', '192.168.1.6:7946']));
      g.dispose();
    });

    test('discoverPeers excludes this node', () async {
      final mdns = MdnsDiscovery(autoJoin: null);
      final g = Gossiper.withBindings(_fakeBindings(
        localAddr: '192.168.1.10',
        discoveredPeers: ['192.168.1.10:7946', '192.168.1.5:7946'],
      ));
      await g.startDiscovery([mdns]);
      expect(mdns.discoverPeers(), equals(['192.168.1.5:7946']));
      g.dispose();
    });

    test('discoverPeers returns empty list when native returns null', () async {
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
        memberlistRestoreLog: bindings.memberlistRestoreLog,
        freeString: bindings.freeString,
      );
      final mdns = MdnsDiscovery();
      final g = Gossiper.withBindings(nullBindings);
      await g.startDiscovery([mdns]);
      expect(mdns.discoverPeers(), isEmpty);
      g.dispose();
    });

    test('joinPeers returns contacted count', () async {
      final mdns = MdnsDiscovery(autoJoin: null);
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946'],
        joinResult: 1,
      ));
      await g.startDiscovery([mdns]);
      expect(mdns.joinPeers(), equals(1));
      g.dispose();
    });

    test('joinPeers returns 0 when no peers found', () async {
      final mdns = MdnsDiscovery();
      final g = Gossiper.withBindings(_fakeBindings(discoveredPeers: []));
      await g.startDiscovery([mdns]);
      expect(mdns.joinPeers(), equals(0));
      g.dispose();
    });

    test('start joins only peers allowed by autoJoin', () async {
      final joins = <List<String>>[];
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946', '10.0.0.7:7946'],
        joinCalls: joins,
      ));
      await g.startDiscovery([
        MdnsDiscovery(autoJoin: (peer, ctx) => peer.startsWith('192.168.')),
      ]);
      expect(joins, equals([['192.168.1.5:7946']]));
      g.dispose();
    });

    test('start supports async autoJoin rules', () async {
      final joins = <List<String>>[];
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946'],
        joinCalls: joins,
      ));
      await g.startDiscovery([
        MdnsDiscovery(autoJoin: (peer, ctx) async => false),
      ]);
      expect(joins, isEmpty);
      g.dispose();
    });

    test('joinPeers ignores autoJoin', () async {
      final joins = <List<String>>[];
      final mdns = MdnsDiscovery(autoJoin: (peer, ctx) => false);
      final g = Gossiper.withBindings(_fakeBindings(
        discoveredPeers: ['192.168.1.5:7946'],
        joinCalls: joins,
      ));
      await g.startDiscovery([mdns]);
      mdns.joinPeers();
      expect(joins, equals([['192.168.1.5:7946']]));
      g.dispose();
    });

    test('discoverPeers throws when not started', () {
      expect(() => MdnsDiscovery().discoverPeers(), throwsA(isA<GossiperException>()));
    });
  });

  group('DiscoveryStrategy', () {
    test('custom strategy receives context and can join', () async {
      final s = _FakeStrategy(peersToJoin: ['10.0.0.9:7946']);
      final g = Gossiper.withBindings(_fakeBindings(localAddr: '192.168.1.10'));
      await g.startDiscovery([s]);
      expect(s.started, isTrue);
      expect(s.ctx!.selfAddr, equals('192.168.1.10:7946'));
      expect(s.joinResult, equals(1));
      g.dispose();
      expect(s.stopped, isTrue);
    });

    test('multiple strategies run together and all stop', () async {
      final a = _FakeStrategy();
      final b = _FakeStrategy();
      final g = Gossiper.withBindings(_fakeBindings());
      await g.startDiscovery([a, b]);
      expect(a.started && b.started, isTrue);
      g.stopDiscovery();
      expect(a.stopped && b.stopped, isTrue);
      g.dispose();
    });

    test('failing strategy stops the ones already started and rethrows', () async {
      final a = _FakeStrategy();
      final g = Gossiper.withBindings(_fakeBindings(mdnsStartError: true));
      await expectLater(
        g.startDiscovery([a, MdnsDiscovery()]),
        throwsA(isA<GossiperException>()),
      );
      expect(a.stopped, isTrue);
      g.dispose();
    });

    test('startDiscovery again stops previous strategies', () async {
      final a = _FakeStrategy();
      final b = _FakeStrategy();
      final g = Gossiper.withBindings(_fakeBindings());
      await g.startDiscovery([a]);
      await g.startDiscovery([b]);
      expect(a.stopped, isTrue);
      expect(b.stopped, isFalse);
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

  group('BeaconUtils.parseBeacon', () {
    String? parse(String payload) => BeaconUtils.parseBeacon(
          payload: payload,
          selfAddr: '192.168.1.1:7946',
          beaconType: 'test_hello',
        );

    test('returns remote addr when beacon is valid', () {
      expect(parse('{"type":"test_hello","addr":"192.168.1.10:7946"}'), equals('192.168.1.10:7946'));
    });

    test('ignores beacon from self', () {
      expect(parse('{"type":"test_hello","addr":"192.168.1.1:7946"}'), isNull);
    });

    test('ignores beacon with wrong type', () {
      expect(parse('{"type":"other_app","addr":"192.168.1.10:7946"}'), isNull);
    });

    test('ignores malformed JSON without throwing', () {
      expect(parse('not-json'), isNull);
    });

    test('ignores beacon missing addr field', () {
      expect(parse('{"type":"test_hello"}'), isNull);
    });
  });

  group('AutoJoinRules', () {
    test('always allows any peer', () async {
      final s = _FakeStrategy();
      final g = Gossiper.withBindings(_fakeBindings(members: _membersOf(3)));
      await g.startDiscovery([s]);
      expect(AutoJoinRules.always('192.168.1.5:7946', s.ctx!), isTrue);
      g.dispose();
    });

    test('whenAlone allows while numMembers <= 1', () async {
      final s = _FakeStrategy();
      final g = Gossiper.withBindings(_fakeBindings());
      await g.startDiscovery([s]);
      expect(AutoJoinRules.whenAlone('192.168.1.5:7946', s.ctx!), isTrue);
      g.dispose();
    });

    test('whenAlone refuses once already in a cluster', () async {
      final s = _FakeStrategy();
      final g = Gossiper.withBindings(_fakeBindings(members: _membersOf(3)));
      await g.startDiscovery([s]);
      expect(AutoJoinRules.whenAlone('192.168.1.5:7946', s.ctx!), isFalse);
      g.dispose();
    });
  });

  group('Gossiper.resolveLanIp', () {
    test('returns a non-empty string', () async {
      final ip = await Gossiper.resolveLanIp();
      expect(ip, isNotEmpty);
    });

    test('never returns a loopback address', () async {
      final ip = await Gossiper.resolveLanIp();
      expect(ip.startsWith('127.'), isFalse);
    });

    test('never returns a link-local address', () async {
      final ip = await Gossiper.resolveLanIp();
      expect(ip.startsWith('169.254.'), isFalse);
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
  List<Never> get addresses => const [];
}

class _FakeStrategy implements DiscoveryStrategy {
  final List<String> peersToJoin;
  DiscoveryContext? ctx;
  bool started = false;
  bool stopped = false;
  int? joinResult;

  _FakeStrategy({this.peersToJoin = const []});

  void Function()? onStart;

  @override
  Future<void> start(DiscoveryContext ctx) async {
    onStart?.call();
    this.ctx = ctx;
    started = true;
    if (peersToJoin.isNotEmpty) joinResult = ctx.join(peersToJoin);
  }

  @override
  void stop() => stopped = true;
}

List<Map<String, dynamic>> _membersOf(int n) => [
      for (var i = 0; i < n; i++) {'name': 'peer-$i', 'addr': '192.168.1.${20 + i}', 'port': 7946},
    ];

class _FakeMessageStore implements MessageStore {
  final List<Message> messages;

  _FakeMessageStore([List<Message> initial = const []]) : messages = [...initial];

  @override
  Future<void> saveMessage(Message message) async => messages.add(message);

  @override
  Future<List<Message>> getMessagesSince(int afterTimestamp, {int? limit}) async {
    final found = messages.where((m) => m.at > afterTimestamp).toList()
      ..sort((a, b) => a.at.compareTo(b.at));
    return limit != null && found.length > limit ? found.sublist(found.length - limit) : found;
  }

}
