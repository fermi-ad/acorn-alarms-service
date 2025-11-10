fn main() {
    // Rebuild when any proto changes
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=proto/DAQ.proto");
    println!("cargo:rerun-if-changed=proto/dpm.proto");
    println!("cargo:rerun-if-changed=proto/controls/common/v1/device.proto");
    println!("cargo:rerun-if-changed=proto/controls/common/v1/status.proto");

    // IMPORTANT: compile DAQ + the common protos so the `common::*` modules exist
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "proto/DAQ.proto",
                "proto/dpm.proto",
                "proto/controls/common/v1/device.proto",
                "proto/controls/common/v1/status.proto",
            ],
            &["proto"], // import root; DAQ.proto should import "controls/common/v1/..."
        )
        .expect("Failed to compile protos with tonic-build");
}
