use std::error::Error;

use rust_grpc_lib::build_support::{Config, generate_protos};

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::new()
        .type_attribute(
            ".google.protobuf",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".common.alarm",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        );
    generate_protos(config)?;

    Ok(())
}
