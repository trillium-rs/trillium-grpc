//! Sanity check: prove trillium speaks h2c via prior-knowledge to a known-good
//! client (curl). If this passes and the tonic test still fails, the bug is in
//! our tonic-side configuration, not in trillium.

use std::process::Command;
use trillium::Conn;

#[tokio::test(flavor = "multi_thread")]
async fn curl_can_speak_h2c_to_trillium() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = trillium_tokio::config()
        .with_host("127.0.0.1")
        .with_port(0)
        .spawn(|conn: Conn| async move {
            let version = format!("{:?}", conn.http_version());
            conn.ok(version)
        });

    let info = server.info().await;
    let port = info.tcp_socket_addr().unwrap().port();

    let output = Command::new("curl")
        .args([
            "-sS",
            "-v",
            "--http2-prior-knowledge",
            &format!("http://127.0.0.1:{port}/"),
        ])
        .output()
        .expect("curl failed to spawn");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    server.shut_down().await;

    println!("--- curl stdout ---\n{stdout}");
    println!("--- curl stderr ---\n{stderr}");

    assert!(
        output.status.success(),
        "curl exited non-zero: {:?}",
        output.status
    );
    assert_eq!(stdout, "Http2", "expected handler to report Http2 version");
}
