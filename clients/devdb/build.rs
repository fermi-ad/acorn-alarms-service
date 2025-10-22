fn main() {
    println!("cargo:rerun-if-changed=proto/DevDB.proto");
    tonic_build::configure()
        .compile_protos(&["proto/DevDB.proto"], &["proto"])
        .expect("Failed to compile DevDB proto");
}
