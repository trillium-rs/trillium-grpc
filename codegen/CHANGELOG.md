# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-06-04

### Added

- The client and server halves of the generated code can now be emitted
  independently. `Builder::client` / `Builder::server` and the matching
  `client` / `server` fields on `Options` select which halves to emit; both
  default to on. The `prost` message types are always emitted regardless.

### Changed

- **Breaking:** `Options` has two new public fields, `client` and `server`.
  Code that constructs `Options` with a struct literal must now set them;
  `Options::default()` (and functional-update from it) leaves both on.

## [0.2.0] - 2026-05-28

Initial tracked release, shipped alongside trillium-grpc 0.2.0.
