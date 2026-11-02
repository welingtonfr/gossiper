/// A broadcast message — as sent, received, and kept in the replay log.
///
/// [id] is a UUID assigned by the sender and used for dedup across nodes and
/// restarts; [at] is the send time in unix milliseconds.
class Message {
  final String id;
  final String data;
  final int at;

  const Message({required this.id, required this.data, required this.at});

  factory Message.fromJson(Map<String, dynamic> j) =>
      Message(id: j['id'] as String, data: j['data'] as String, at: j['at'] as int);

  Map<String, dynamic> toJson() => {'id': id, 'data': data, 'at': at};

  @override
  String toString() => 'Message($id @ $at)';
}
