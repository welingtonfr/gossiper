import '../domain/contracts/discovery_context.dart';
import '../gossiper_bindings.dart';

/// Internal extension of [DiscoveryContext] for built-in strategies that
/// need the native library (e.g. mDNS). Not exported.
abstract interface class NativeDiscoveryContext implements DiscoveryContext {
  GossiperBindings get bindings;
  int get handle;
}
