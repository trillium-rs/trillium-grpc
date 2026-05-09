//! Codegen "golden file" test. Re-runs `generate_from_proto` on the same
//! fixture used by the runtime tests and asserts the output matches the file
//! committed at `tests/generated/greeter_v1.rs`. Set `UPDATE_GENERATED=1` to
//! refresh the committed file in-place.

use std::path::{Path, PathBuf};
use trillium_grpc::codegen::{Options, generate_from_proto};

const GENERATED_PATH: &str = "tests/generated/greeter_v1.rs";

#[test]
fn greeter_v1_matches_committed_output() {
    let opts = Options {
        include_paths: vec![PathBuf::from("tests/proto")],
        ..Options::default()
    };
    let generated = generate_from_proto(
        &[PathBuf::from("tests/proto/greeter.proto")],
        &opts,
    )
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
