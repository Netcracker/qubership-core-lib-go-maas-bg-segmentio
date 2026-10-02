# Changelog

All notable changes to this library are documented here.

## [Unreleased]

### Fixed

- The offset queries behind `BeginningOffsets`, `EndOffsets` and `OffsetsForTimes`
  reach every partition passed in. They were keyed by topic name, so each
  partition's request overwrote the previous one and only one partition per topic,
  picked by map iteration order, was ever sent to the broker.
- `OffsetsForTimes` reports no offset for a partition with no record at or after
  the requested timestamp, instead of a fabricated offset 0. The corrector reads
  that as "start from the beginning", which replays the whole log or fails with
  `OffsetOutOfRange`.
- `ListConsumerGroupOffsets` omits partitions the group has never committed on.
  Kafka reports those as offset -1, which was passed through as a real offset, so
  the corrector treated partitions added after the group last committed as
  already committed and installed no offsets for them.
- A per-partition broker error from `ListOffsets` or `OffsetFetch` is returned
  instead of being read as an offset. `LeaderNotAvailable` used to surface as a
  resolved offset of -1.

### Documentation

- `ReadMessage` states that it does not redeliver: the reader's position only
  moves forward, so a message whose handling failed has to be retried in place.

## [3.6.2] - 2026-09-17

### Fixed

- `ReadMessage` returns no message when the read failed. It used to return a
  wrapper around the zero value alongside the error, so a caller that checked
  the message before the error processed an empty record as if it had arrived.

### Changed

- The polling example in the README keeps reading after a failed poll instead of
  leaving the loop. Losing the group coordinator fails every poll until the
  group finds a new one, which is exactly when the consumer must keep calling.
  The example now leaves the loop only when the context is cancelled, and pauses
  before a retry, so a permanent failure is a slow log rather than a busy loop.
