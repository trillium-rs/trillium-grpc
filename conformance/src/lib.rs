//! Connect/gRPC conformance harness for trillium-grpc.
//!
//! Implements `connectrpc.conformance.v1.ConformanceService` so the
//! `connectconformance` runner can drive a trillium-grpc server-under-test.

#[allow(clippy::all, dead_code)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/connectrpc.conformance.v1.rs"));
}

pub mod service;
