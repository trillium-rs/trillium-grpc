fn main() {
    println!("cargo:rerun-if-changed=tests/proto/greeter.proto");

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["tests/proto/greeter.proto"], &["tests/proto"])
        .expect("failed to compile greeter.proto");
}
