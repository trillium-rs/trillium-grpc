//! Codegen "golden file" test. Re-runs `generate_from_proto` on the same
//! fixture used by the runtime tests and asserts the output matches the file
//! committed at `tests/generated/greeter_v1.rs`. Set `UPDATE_GENERATED=1` to
//! refresh the committed file in-place.

use std::path::{Path, PathBuf};
use trillium_grpc::codegen::{Options, compile_protos, generate_from_proto};

const GENERATED_PATH: &str = "tests/generated/greeter_v1.rs";

#[test]
fn greeter_v1_matches_committed_output() {
    let opts = Options {
        include_paths: vec![PathBuf::from("tests/proto")],
        ..Options::default()
    };
    let generated = generate_from_proto(&[PathBuf::from("tests/proto/greeter.proto")], &opts)
        .expect("codegen succeeds");

    let actual = generated
        .files
        .get(Path::new("greeter.v1.rs"))
        .expect("greeter.v1.rs in output");

    if std::env::var("UPDATE_GENERATED").is_ok() {
        let path = Path::new(GENERATED_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, actual).unwrap();
        return;
    }

    let expected = std::fs::read_to_string(GENERATED_PATH).unwrap_or_else(|e| {
        panic!(
            "could not read {GENERATED_PATH}: {e}. Re-run with \
             UPDATE_GENERATED=1 to bootstrap.",
        );
    });

    if actual != &expected {
        panic!(
            "{GENERATED_PATH} is out of sync with codegen output. \
             Re-run with UPDATE_GENERATED=1 to update.",
        );
    }
}

/// The build-script entry point (`compile_protos`) should write `<package>.rs`
/// into OUT_DIR, byte-for-byte identical to what the CLI / golden path emits,
/// and emit a `rerun-if-changed` line for the source proto.
#[test]
fn compile_protos_writes_to_out_dir() {
    let out_dir =
        std::env::temp_dir().join(format!("trillium-grpc-codegen-{}", std::process::id()));
    std::fs::create_dir_all(&out_dir).unwrap();

    // Safety: trillium-grpc has no build script, so cargo does not set OUT_DIR
    // for the test binary, and no other test in this binary reads it.
    unsafe {
        std::env::set_var("OUT_DIR", &out_dir);
    }

    compile_protos(
        &[PathBuf::from("tests/proto/greeter.proto")],
        &[PathBuf::from("tests/proto")],
    )
    .expect("compile_protos succeeds");

    let written = std::fs::read_to_string(out_dir.join("greeter.v1.rs"))
        .expect("greeter.v1.rs written to OUT_DIR");

    // The build-script path must emit exactly what the library path does;
    // assert against generate_from_proto directly rather than the committed
    // golden, so this test stays independent of prettyplease formatting drift.
    let opts = Options {
        include_paths: vec![PathBuf::from("tests/proto")],
        ..Options::default()
    };
    let expected = generate_from_proto(&[PathBuf::from("tests/proto/greeter.proto")], &opts)
        .expect("codegen succeeds")
        .files
        .remove(Path::new("greeter.v1.rs"))
        .expect("greeter.v1.rs in output");

    assert_eq!(
        written, expected,
        "OUT_DIR output should match generate_from_proto output"
    );

    std::fs::remove_dir_all(&out_dir).ok();
}
