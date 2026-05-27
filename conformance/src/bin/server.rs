//! Conformance server-under-test harness.
//!
//! The `connectconformance` runner speaks to a server-under-test over
//! stdin/stdout: it writes one length-prefixed [`ServerCompatRequest`] to our
//! stdin describing how to listen, and expects one length-prefixed
//! [`ServerCompatResponse`] on our stdout reporting where we're listening. The
//! process then serves the `ConformanceService` until it receives SIGTERM.
//!
//! Each message is framed with a 4-byte big-endian length prefix (matching the
//! conformance protoDecoder/protoEncoder).
//!
//! We speak gRPC over HTTP/2, either cleartext (h2c) or over TLS when the runner
//! sets `use_tls` (it hands us a self-signed cert + key in `server_creds`; we
//! serve with ALPN `h2` and echo the cert back in `pem_cert`). The HTTP version
//! and message-receive-limit knobs are still ignored; the config YAML restricts
//! the runner to the flavors we support.
//!
//! [`ServerCompatRequest`]: trillium_grpc_conformance::pb::ServerCompatRequest
//! [`ServerCompatResponse`]: trillium_grpc_conformance::pb::ServerCompatResponse

use prost::Message;
use std::io::{self, Read, Write};
use trillium_grpc_conformance::{
    pb::{ConformanceServiceServer, ServerCompatRequest, ServerCompatResponse},
    service::ConformanceServiceImpl,
};
use trillium_rustls::RustlsAcceptor;

/// Read one 4-byte-big-endian-length-prefixed protobuf message from stdin.
fn read_delimited<M: Message + Default>(input: &mut impl Read) -> io::Result<M> {
    let mut len_buf = [0u8; 4];
    input.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    input.read_exact(&mut buf)?;
    M::decode(&buf[..]).map_err(io::Error::other)
}

/// Write one 4-byte-big-endian-length-prefixed protobuf message to stdout.
fn write_delimited(output: &mut impl Write, msg: &impl Message) -> io::Result<()> {
    let bytes = msg.encode_to_vec();
    output.write_all(&(bytes.len() as u32).to_be_bytes())?;
    output.write_all(&bytes)?;
    output.flush()
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    // Read the server config the runner hands us.
    let request: ServerCompatRequest = read_delimited(&mut io::stdin().lock())?;

    // Start a trillium HTTP/2 server on an ephemeral port — cleartext (h2c), or
    // over TLS when the runner asks for it. The default config registers
    // SIGTERM/SIGINT/SIGQUIT for graceful shutdown, which is exactly how the
    // runner stops us — so awaiting the handle blocks until shutdown.
    //
    // For TLS the runner supplies a self-signed cert + key in `server_creds`;
    // `RustlsAcceptor::from_single_cert` advertises ALPN `h2` (so trillium runs
    // its HTTP/2 driver over the TLS stream), and we echo the cert back so the
    // runner's clients trust it.
    let (handle, pem_cert) = if request.use_tls {
        let creds = request
            .server_creds
            .ok_or_else(|| io::Error::other("use_tls set but no server_creds provided"))?;
        let acceptor = RustlsAcceptor::from_single_cert(&creds.cert, &creds.key);
        let handle = trillium_tokio::config()
            .with_acceptor(acceptor)
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(ConformanceServiceServer::new(ConformanceServiceImpl));
        (handle, creds.cert)
    } else {
        let handle = trillium_tokio::config()
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(ConformanceServiceServer::new(ConformanceServiceImpl));
        (handle, Vec::new())
    };

    let info = handle.info().await;
    let port = info
        .tcp_socket_addr()
        .ok_or_else(|| io::Error::other("server did not bind a TCP socket"))?
        .port();

    write_delimited(
        &mut io::stdout().lock(),
        &ServerCompatResponse {
            host: "127.0.0.1".to_string(),
            port: port as u32,
            pem_cert,
        },
    )?;

    handle.await;
    Ok(())
}
