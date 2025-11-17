use build_support::setup_protoc_env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_protoc_env()?; // using shared helper

    let proto_root: &str = "../../interface-definitions/proto";

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile(
            &[
                &format!("{}/controls/service/DevDB/v1/DevDB.proto", proto_root),
                &format!("{}/controls/common/v1/device.proto", proto_root),
                &format!("{}/controls/common/v1/status.proto", proto_root),
            ],
            &[proto_root],
        )?;

    println!("cargo:rerun-if-changed={}", proto_root);
    Ok(())
}
