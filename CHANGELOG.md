# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-06-04

### Added

- The client and server halves are now independently selectable. New `client`
  and `server` cargo features (both on by default) gate the `client` and
  `server` modules and their re-exports, so a crate that only calls gRPC can
  set `default-features = false, features = ["client"]` and never compile the
  server stack (or vice versa).
- The codegen splits the same way. New `generate_client!` and
  `generate_server!` macros emit one half each — useful because cargo feature
  unification is global, so a crate that is a client for one service and a
  server for another selects per call site rather than per feature. For build
  scripts and library codegen, `Builder::client` / `Builder::server` and the
  `client` / `server` fields on `Options` do the same. The `prost` message
  types are always emitted regardless of which half you ask for.

### Changed

- **Breaking:** with `default-features = false`, the `client` and `server`
  modules no longer compile. They previously came in unconditionally; add
  `features = ["client"]` and/or `["server"]` to restore them. Crates on the
  default feature set are unaffected.
- **Breaking:** the `parse_grpc_content_type` and `has_te_trailers` helpers
  moved from the `server::content_type` module to a top-level `content_type`
  module.

## [0.2.0] - 2026-05-28

### Changed

Completely reworked the interface as documented. No 0.1 code will work, this should be considered a
fresh interface
