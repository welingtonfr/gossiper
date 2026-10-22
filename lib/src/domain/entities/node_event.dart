import 'package:gossiper/src/domain/entities/member_node.dart';
import 'package:gossiper/src/domain/vos/node_event_type.dart';

class NodeEvent {
  final NodeEventType type;
  final MemberNode node;

  const NodeEvent({required this.type, required this.node});

  @override
  String toString() => 'NodeEvent(${type.name}, ${node.name})';
}