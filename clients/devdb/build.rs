fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Proto locations in the submodule
    const DEVDB_PROTO: &str =
        "../../interface-definitions/services/devdb/proto/devdb.proto";

    // Rebuild triggers
    println!("cargo:rerun-if-changed={DEVDB_PROTO}");
    println!("cargo:rerun-if-changed=../../interface-definitions/proto");

    // Include roots:
    //  - interface-definitions/           (so imports starting with "proto/..." work)
    //  - services/devdb/proto/            (service-local includes, if any)
    tonic_build::configure()
        .build_server(false)
        .compile(
            &[DEVDB_PROTO],
            &[
                "../../interface-definitions",
                "../../interface-definitions/services/devdb/proto",
            ],
        )?;

    Ok(())
}
