fn main() -> Result<(), Box<dyn std::error::Error>> {
    const DEVDB_PROTO: &str =
        "../../interface-definitions/proto/controls/service/DevDB/v1/devdb.proto";

    println!("cargo:rerun-if-changed={DEVDB_PROTO}");
    println!("cargo:rerun-if-changed=../../interface-definitions/proto");

    tonic_build::configure().build_server(false).compile(
        &[DEVDB_PROTO],
        &[
            "../../interface-definitions/proto/controls",
            "../../interface-definitions/proto/controls/service/DevDB/v1",
        ],
    )?;

    Ok(())
}
