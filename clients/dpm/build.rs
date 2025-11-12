fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../interface-definitions";
    let virtual_proto_dir = "../../interface-definitions/proto"; // where your protos actually are

    // Add both include paths so DAQ.proto can find "proto/controls/..."
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile(
            &[
                &format!("{}/proto/controls/service/DAQ/v1/DAQ.proto", proto_dir),
                &format!("{}/proto/controls/common/v1/device.proto", proto_dir),
                &format!("{}/proto/controls/common/v1/status.proto", proto_dir),
            ],
            &[proto_dir, virtual_proto_dir],
        )?;

    println!("cargo:rerun-if-changed={}", proto_dir);
    Ok(())
}
