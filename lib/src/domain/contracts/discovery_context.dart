/// What a [DiscoveryStrategy] can see and do on the node it belongs to.
///
/// Passed to [DiscoveryStrategy.start] by `Gossiper.startDiscovery`.
abstract interface class DiscoveryContext {
  /// LAN IPv4 address of this node (never `0.0.0.0` unless nothing routable exists).
  String get lanIp;

  /// Gossip (memberlist) port of this node.
  int get gossipPort;

  /// `"lanIp:gossipPort"` — the address other nodes should [join].
  String get selfAddr;

  /// Current number of alive cluster members, including this node.
  int get numMembers;

  /// Joins the cluster via [addresses] (e.g. `["192.168.1.2:7946"]`).
  /// Returns the number of seed nodes successfully contacted.
  int join(List<String> addresses);
}
