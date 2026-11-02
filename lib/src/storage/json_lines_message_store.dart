import 'dart:convert';
import 'dart:io';

import '../domain/contracts/message_store.dart';
import '../domain/entities/message.dart';

/// [MessageStore] backed by a JSON-lines file — one `{"id","data","at"}`
/// object per line. Same format as the former `persistPath`, so existing
/// files keep working.
///
/// Operations are serialized, so concurrent calls never interleave writes.
class JsonLinesMessageStore implements MessageStore {
  final File file;

  Future<void> _queue = Future.value();

  JsonLinesMessageStore(String path) : file = File(path);

  @override
  Future<void> saveMessage(Message message) => _run(() async {
        await file.parent.create(recursive: true);
        await file.writeAsString('${jsonEncode(message.toJson())}\n', mode: FileMode.append, flush: true);
      });

  @override
  Future<List<Message>> getMessagesSince(int afterTimestamp, {int? limit}) => _run(() async {
        final messages = (await _readAll()).where((m) => m.at > afterTimestamp).toList();
        if (limit != null && messages.length > limit) {
          return messages.sublist(messages.length - limit);
        }
        return messages;
      });

  @override
  Future<void> deleteBefore(int timestamp) => _run(() async {
        final all = await _readAll();
        final kept = all.where((m) => m.at >= timestamp).toList();
        if (kept.length == all.length) return;
        final tmp = File('${file.path}.tmp');
        await tmp.writeAsString(kept.map((m) => '${jsonEncode(m.toJson())}\n').join(), flush: true);
        await tmp.rename(file.path);
      });

  /// Reads every valid line, deduped by id and sorted by [Message.at].
  Future<List<Message>> _readAll() async {
    if (!await file.exists()) return [];
    final byId = <String, Message>{};
    for (final line in await file.readAsLines()) {
      if (line.isEmpty) continue;
      try {
        final m = Message.fromJson(jsonDecode(line) as Map<String, dynamic>);
        if (m.id.isNotEmpty) byId.putIfAbsent(m.id, () => m);
      } catch (_) {
        // skip corrupt / partial lines
      }
    }
    return byId.values.toList()..sort((a, b) => a.at.compareTo(b.at));
  }

  Future<T> _run<T>(Future<T> Function() op) {
    final result = _queue.then((_) => op());
    _queue = result.then((_) {}, onError: (_) {});
    return result;
  }
}
