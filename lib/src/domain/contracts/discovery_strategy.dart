import 'discovery_context.dart';

/// A peer discovery mechanism plugged into `Gossiper.startDiscovery`.
///
/// Implement this to find peers your own way (static list, HTTP registry,
/// Consul, cloud APIs, …). Call [DiscoveryContext.join] whenever peers are
/// found. Several strategies can run on the same node at once.
abstract interface class DiscoveryStrategy {
  /// Starts advertising and/or looking for peers.
  ///
  /// Throwing here aborts `Gossiper.startDiscovery` and stops the strategies
  /// already started.
  Future<void> start(DiscoveryContext ctx);

  /// Stops all background work started by [start]. Must be safe to call
  /// more than once and before [start].
  void stop();
}
