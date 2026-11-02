import 'dart:async';

import 'discovery_context.dart';

/// Decides whether a discovery strategy should automatically join [peerAddr]
/// (`"addr:port"`) after finding it.
///
/// May be async (e.g. to consult an allow-list service). Pass `null` as a
/// strategy's `autoJoin` to disable automatic joining entirely.
typedef AutoJoinRule = FutureOr<bool> Function(String peerAddr, DiscoveryContext ctx);

/// Built-in [AutoJoinRule]s.
abstract final class AutoJoinRules {
  /// Joins every discovered peer.
  static bool always(String peerAddr, DiscoveryContext ctx) => true;

  /// Joins only while this node is alone (`numMembers <= 1`). Once in a
  /// cluster, memberlist's gossip propagates membership by itself.
  static bool whenAlone(String peerAddr, DiscoveryContext ctx) => ctx.numMembers <= 1;
}
