import 'dart:convert';
import 'dart:io';

import '../domain/contracts/message_store.dart';
import '../domain/entities/message.dart';

/// [MessageStore] backed by a JSON-lines file — one `{"id","data","at"}`
/// object per line. Same format as the former `persistPath`, so existing
/// files keep working.
///
/// Operations are serialized, so concurrent calls never interleave writes.
///
/// When [maxFileSizeBytes] is set and a save makes the file exceed it, the
/// file is rewritten keeping only the newest messages that fit in 75% of the
/// limit (the slack avoids a rewrite on every save near the limit). The
/// newest message is always kept, even if it alone exceeds the limit.
/// `null` (default) means no limit.
class JsonLinesMessageStore implements MessageStore {
  final File file;
  final int? maxFileSizeBytes;

  Future<void> _queue = Future.value();

  JsonLinesMessageStore(String path, {this.maxFileSizeBytes})
      : assert(maxFileSizeBytes == null || maxFileSizeBytes > 0),
        file = File(path);

  @override
  Future<void> saveMessage(Message message) => _run(() async {
        await file.parent.create(recursive: true);
        await file.writeAsString(_line(message), mode: FileMode.append, flush: true);
        final max = maxFileSizeBytes;
        if (max != null && await file.length() > max) await _compact(max * 3 ~/ 4);
      });

  @override
  Future<List<Message>> getMessagesSince(int afterTimestamp, {int? limit}) => _run(() async {
        final messages = (await _readAll()).where((m) => m.at > afterTimestamp).toList();
        if (limit != null && messages.length > limit) {
          return messages.sublist(messages.length - limit);
        }
        return messages;
      });

  /// Deletes messages with `at < timestamp`. Never called by the library —
  /// call it yourself to keep the file from growing (e.g. on startup).
  Future<void> deleteBefore(int timestamp) => _run(() async {
        final all = await _readAll();
        final kept = all.where((m) => m.at >= timestamp).toList();
        if (kept.length == all.length) return;
        await _writeAll(kept);
      });

  /// Rewrites the file with the newest messages whose lines fit in [targetBytes].
  Future<void> _compact(int targetBytes) async {
    final all = await _readAll();
    final kept = <Message>[];
    var size = 0;
    for (final m in all.reversed) {
      final lineSize = utf8.encode(_line(m)).length;
      if (kept.isNotEmpty && size + lineSize > targetBytes) break;
      kept.add(m);
      size += lineSize;
    }
    await _writeAll(kept.reversed.toList());
  }

  /// Atomically replaces the file with [messages].
  Future<void> _writeAll(List<Message> messages) async {
    final tmp = File('${file.path}.tmp');
    await tmp.writeAsString(messages.map(_line).join(), flush: true);
    await tmp.rename(file.path);
  }

  static String _line(Message m) => '${jsonEncode(m.toJson())}\n';

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
