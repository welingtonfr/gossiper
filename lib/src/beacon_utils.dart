import 'dart:convert';
import 'dart:io';

abstract final class BeaconUtils {
  /// Derives the /24 subnet broadcast address from [ip].
  static String subnetBroadcast(String ip) {
    final p = ip.split('.');
    return p.length == 4 ? '${p[0]}.${p[1]}.${p[2]}.255' : '255.255.255.255';
  }

  /// Returns true when [iface]/[ip] belongs to a known virtual network adapter.
  ///
  /// Covers: Docker, WSL2, VirtualBox, VMware, KVM/libvirt.
  static bool isVirtualInterface(NetworkInterface iface, String ip) {
    final name = iface.name.toLowerCase();
    if (name.startsWith('docker') ||
        name.startsWith('vbox') ||
        name.startsWith('vmnet') ||
        name.startsWith('virbr') ||
        name.startsWith('br-') ||
        name.startsWith('veth')) {
      return true;
    }

    // Docker / WSL2 bridge range: 172.16.0.0/12
    if (ip.startsWith('172.')) {
      final parts = ip.split('.');
      if (parts.length >= 2) {
        final second = int.tryParse(parts[1]) ?? 0;
        if (second >= 16 && second <= 31) { return true; }
      }
    }

    // VirtualBox host-only (192.168.56.x) and KVM/libvirt (192.168.122.x)
    if (ip.startsWith('192.168.56.') || ip.startsWith('192.168.122.')) return true;

    return false;
  }

  /// Parses a raw beacon [payload] and calls [onJoin] with the remote address
  /// when a valid foreign beacon is received.
  ///
  /// Ignores payloads with wrong [beaconType], malformed JSON, and beacons
  /// that originate from [selfAddr].
  static void processBeacon({
    required String payload,
    required String selfAddr,
    required String beaconType,
    required int numMembers,
    required void Function(String addr) onJoin,
  }) {
    String? addrToJoin;
    try {
      final map = jsonDecode(payload) as Map<String, dynamic>;
      if (map['type'] != beaconType) return;
      final remoteAddr = map['addr'] as String?;
      if (remoteAddr == null || remoteAddr == selfAddr) return;
      if (numMembers <= 1) addrToJoin = remoteAddr;
    } catch (_) {
      return;
    }
    if (addrToJoin != null) onJoin(addrToJoin);
  }
}
