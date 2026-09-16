# Changelog

All notable changes to this library are documented here.

## [Unreleased]

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
