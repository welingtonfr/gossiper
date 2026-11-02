import '../entities/message.dart';

/// Durable storage for the broadcast replay log.
///
/// Passed to `Gossiper.create(messageStore:)`. Every message sent or
/// received is passed to [saveMessage]. On the first
/// `Gossiper.startDiscovery`, [getMessagesSince] loads the messages within the
/// replay window into the replay log, so a node that restarts (e.g. after a
/// power loss) can still replay its history to peers and won't receive those
/// messages again.
///
/// Retention is up to the implementation: the library never deletes from the
/// store. Errors thrown by [saveMessage] are not caught.
///
/// Built-in implementation: `JsonLinesMessageStore`. Implement this to keep
/// the log somewhere else (SQLite, Hive, a remote service, …).
abstract interface class MessageStore {
  /// Persists [message]. Messages with an already-stored [Message.id]
  /// may be ignored.
  Future<void> saveMessage(Message message);

  /// Returns messages with `at > afterTimestamp`, oldest first.
  ///
  /// When [limit] is given, returns only the [limit] most recent of them.
  Future<List<Message>> getMessagesSince(int afterTimestamp, {int? limit});
}
