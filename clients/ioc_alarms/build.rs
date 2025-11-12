fn main() -> Result<(), Box<dyn std::error::Error>> {
    const IOC_ALARMS_PROTO: &str =
        "../../interface-definitions/proto/controls/service/grpc-ioc-alarms/v1/ioc_alarms.proto";

    println!("cargo:rerun-if-changed={IOC_ALARMS_PROTO}");
    println!("cargo:rerun-if-changed=../../interface-definitions/proto");

    tonic_build::configure().build_server(false).compile(
        &[IOC_ALARMS_PROTO],
        &[
            "../../interface-definitions/proto/controls",
            "../../interface-definitions/proto/controls/service/grpc-ioc-alarms/v1",
        ],
    )?;

    Ok(())
}
