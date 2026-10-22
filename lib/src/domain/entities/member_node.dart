class MemberNode {
  final String name;
  final String addr;
  final int port;

  const MemberNode({required this.name, required this.addr, required this.port});

  factory MemberNode.fromJson(Map<String, dynamic> j) =>
      MemberNode(name: j['name'] as String, addr: j['addr'] as String, port: j['port'] as int);

  String get address => '$addr:$port';

  @override
  String toString() => 'MemberNode($name @ $address)';
}