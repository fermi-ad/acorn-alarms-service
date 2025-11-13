fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe { std::env::set_var("PROTOC", protoc_path) };

    let proto_root = "../../interface-definitions/proto";

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile(
            &[
                &format!("{}/controls/service/DevDB/v1/devdb.proto", proto_root),
                &format!("{}/controls/common/v1/device.proto", proto_root),
                &format!("{}/controls/common/v1/status.proto", proto_root),
            ],
            &[proto_root],
        )?;

    println!("cargo:rerun-if-changed={}", proto_root);
    Ok(())
}
