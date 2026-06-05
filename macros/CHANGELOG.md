# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-06-04

### Added

- `generate_client!` and `generate_server!`, alongside the existing
  `generate!`. Each emits only its half — the `<Service>Client`, or the
  service trait plus `<Service>Server<T>` — while the `prost` message types
  come along either way. Selection is per-invocation, so one crate can be a
  client for one service and a server for another. `generate!` is unchanged
  and still emits both halves.

## [0.2.0] - 2026-05-28

Initial tracked release, shipped alongside trillium-grpc 0.2.0.
