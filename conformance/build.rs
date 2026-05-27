//! Generates the `connectrpc.conformance.v1` module (ConformanceService trait +
//! Server/Client + prost message types) from the vendored conformance protos
//! into OUT_DIR. Included by `src/lib.rs`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    trillium_grpc_codegen::compile_protos(
        &[
            "proto/connectrpc/conformance/v1/service.proto",
            "proto/connectrpc/conformance/v1/server_compat.proto",
            "proto/connectrpc/conformance/v1/client_compat.proto",
            "proto/connectrpc/conformance/v1/config.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
