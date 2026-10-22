class GossiperException implements Exception {
  final String message;
  const GossiperException(this.message);

  @override
  String toString() => 'GossiperException: $message';
}
