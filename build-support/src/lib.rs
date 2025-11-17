use std::error::Error;
use std::path::PathBuf;

pub fn setup_protoc_env() -> Result<(), Box<dyn Error>> {
    let protoc_path: PathBuf = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", &protoc_path);

    let include_path: PathBuf = protoc_bin_vendored::include_path()?;
    std::env::set_var("PROTOC_INCLUDE", &include_path);

    Ok(())
}
