// ignore_for_file: camel_case_types, non_constant_identifier_names
import 'dart:ffi';
import 'package:ffi/ffi.dart';

// ---------------------------------------------------------------------------
// Native function typedefs — memberlist core
// ---------------------------------------------------------------------------

typedef _MemberlistCreateNative = Int64 Function(
  Pointer<Utf8> name,
  Pointer<Utf8> bindAddr,
  Int32 bindPort,
  Pointer<Pointer<Utf8>> errOut,
);
typedef MemberlistCreateDart = int Function(
  Pointer<Utf8> name,
  Pointer<Utf8> bindAddr,
  int bindPort,
  Pointer<Pointer<Utf8>> errOut,
);

typedef _MemberlistJoinNative = Int32 Function(
  Int64 handle,
  Pointer<Utf8> addrsJson,
  Pointer<Pointer<Utf8>> errOut,
);
typedef MemberlistJoinDart = int Function(
  int handle,
  Pointer<Utf8> addrsJson,
  Pointer<Pointer<Utf8>> errOut,
);

typedef _MemberlistMembersNative = Pointer<Utf8> Function(Int64 handle);
typedef MemberlistMembersDart = Pointer<Utf8> Function(int handle);

typedef _MemberlistLocalNodeNative = Pointer<Utf8> Function(Int64 handle);
typedef MemberlistLocalNodeDart = Pointer<Utf8> Function(int handle);

typedef _MemberlistNumMembersNative = Int32 Function(Int64 handle);
typedef MemberlistNumMembersDart = int Function(int handle);

typedef _MemberlistPollEventsNative = Pointer<Utf8> Function(Int64 handle);
typedef MemberlistPollEventsDart = Pointer<Utf8> Function(int handle);

typedef _MemberlistLeaveNative = Int32 Function(
  Int64 handle,
  Int32 timeoutMs,
  Pointer<Pointer<Utf8>> errOut,
);
typedef MemberlistLeaveDart = int Function(
  int handle,
  int timeoutMs,
  Pointer<Pointer<Utf8>> errOut,
);

typedef _MemberlistDestroyNative = Void Function(Int64 handle);
typedef MemberlistDestroyDart = void Function(int handle);

// ---------------------------------------------------------------------------
// Native function typedefs — mDNS discovery
// ---------------------------------------------------------------------------

typedef _MemberlistStartMDNSNative = Int32 Function(
  Int64 handle,
  Pointer<Utf8> serviceName,
  Pointer<Pointer<Utf8>> errOut,
);
typedef MemberlistStartMDNSDart = int Function(
  int handle,
  Pointer<Utf8> serviceName,
  Pointer<Pointer<Utf8>> errOut,
);

typedef _MemberlistStopMDNSNative = Void Function(Int64 handle);
typedef MemberlistStopMDNSDart = void Function(int handle);

typedef _MemberlistDiscoverMDNSNative = Pointer<Utf8> Function(
  Pointer<Utf8> serviceName,
  Int32 timeoutMs,
);
typedef MemberlistDiscoverMDNSDart = Pointer<Utf8> Function(
  Pointer<Utf8> serviceName,
  int timeoutMs,
);

// ---------------------------------------------------------------------------
// FreeString
// ---------------------------------------------------------------------------

typedef _FreeStringNative = Void Function(Pointer<Utf8> s);
typedef FreeStringDart = void Function(Pointer<Utf8> s);

// ---------------------------------------------------------------------------
// Binding loader
// ---------------------------------------------------------------------------

class GossiperBindings {
  final MemberlistCreateDart memberlistCreate;
  final MemberlistJoinDart memberlistJoin;
  final MemberlistMembersDart memberlistMembers;
  final MemberlistLocalNodeDart memberlistLocalNode;
  final MemberlistNumMembersDart memberlistNumMembers;
  final MemberlistPollEventsDart memberlistPollEvents;
  final MemberlistLeaveDart memberlistLeave;
  final MemberlistDestroyDart memberlistDestroy;
  final MemberlistStartMDNSDart memberlistStartMDNS;
  final MemberlistStopMDNSDart memberlistStopMDNS;
  final MemberlistDiscoverMDNSDart memberlistDiscoverMDNS;
  final FreeStringDart freeString;

  GossiperBindings._({
    required this.memberlistCreate,
    required this.memberlistJoin,
    required this.memberlistMembers,
    required this.memberlistLocalNode,
    required this.memberlistNumMembers,
    required this.memberlistPollEvents,
    required this.memberlistLeave,
    required this.memberlistDestroy,
    required this.memberlistStartMDNS,
    required this.memberlistStopMDNS,
    required this.memberlistDiscoverMDNS,
    required this.freeString,
  });

  factory GossiperBindings.fromLibrary(DynamicLibrary lib) {
    return GossiperBindings._(
      memberlistCreate:
          lib.lookup<NativeFunction<_MemberlistCreateNative>>('MemberlistCreate').asFunction(),
      memberlistJoin:
          lib.lookup<NativeFunction<_MemberlistJoinNative>>('MemberlistJoin').asFunction(),
      memberlistMembers:
          lib.lookup<NativeFunction<_MemberlistMembersNative>>('MemberlistMembers').asFunction(),
      memberlistLocalNode:
          lib.lookup<NativeFunction<_MemberlistLocalNodeNative>>('MemberlistLocalNode').asFunction(),
      memberlistNumMembers: lib
          .lookup<NativeFunction<_MemberlistNumMembersNative>>('MemberlistNumMembers')
          .asFunction(),
      memberlistPollEvents: lib
          .lookup<NativeFunction<_MemberlistPollEventsNative>>('MemberlistPollEvents')
          .asFunction(),
      memberlistLeave:
          lib.lookup<NativeFunction<_MemberlistLeaveNative>>('MemberlistLeave').asFunction(),
      memberlistDestroy:
          lib.lookup<NativeFunction<_MemberlistDestroyNative>>('MemberlistDestroy').asFunction(),
      memberlistStartMDNS:
          lib.lookup<NativeFunction<_MemberlistStartMDNSNative>>('MemberlistStartMDNS').asFunction(),
      memberlistStopMDNS:
          lib.lookup<NativeFunction<_MemberlistStopMDNSNative>>('MemberlistStopMDNS').asFunction(),
      memberlistDiscoverMDNS: lib
          .lookup<NativeFunction<_MemberlistDiscoverMDNSNative>>('MemberlistDiscoverMDNS')
          .asFunction(),
      freeString: lib.lookup<NativeFunction<_FreeStringNative>>('FreeString').asFunction(),
    );
  }

  /// Constructs bindings from plain Dart functions — for use in unit tests only.
  factory GossiperBindings.fake({
    required MemberlistCreateDart memberlistCreate,
    required MemberlistJoinDart memberlistJoin,
    required MemberlistMembersDart memberlistMembers,
    required MemberlistLocalNodeDart memberlistLocalNode,
    required MemberlistNumMembersDart memberlistNumMembers,
    required MemberlistPollEventsDart memberlistPollEvents,
    required MemberlistLeaveDart memberlistLeave,
    required MemberlistDestroyDart memberlistDestroy,
    required MemberlistStartMDNSDart memberlistStartMDNS,
    required MemberlistStopMDNSDart memberlistStopMDNS,
    required MemberlistDiscoverMDNSDart memberlistDiscoverMDNS,
    required FreeStringDart freeString,
  }) =>
      GossiperBindings._(
        memberlistCreate: memberlistCreate,
        memberlistJoin: memberlistJoin,
        memberlistMembers: memberlistMembers,
        memberlistLocalNode: memberlistLocalNode,
        memberlistNumMembers: memberlistNumMembers,
        memberlistPollEvents: memberlistPollEvents,
        memberlistLeave: memberlistLeave,
        memberlistDestroy: memberlistDestroy,
        memberlistStartMDNS: memberlistStartMDNS,
        memberlistStopMDNS: memberlistStopMDNS,
        memberlistDiscoverMDNS: memberlistDiscoverMDNS,
        freeString: freeString,
      );
}
