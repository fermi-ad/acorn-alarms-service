fn main() -> Result<(), Box<dyn std::error::Error>> {
    const IOC_ALARMS_PROTO: &str =
        "../../interface-definitions/services/ioc_alarms/proto/ioc_alarms.proto";

    println!("cargo:rerun-if-changed={IOC_ALARMS_PROTO}");
    println!("cargo:rerun-if-changed=../../interface-definitions/proto");

    tonic_build::configure()
        .build_server(false)
        .compile(
            &[IOC_ALARMS_PROTO],
            &[
                "../../interface-definitions",
                "../../interface-definitions/services/ioc_alarms/proto",
            ],
        )?;

    Ok(())
}
