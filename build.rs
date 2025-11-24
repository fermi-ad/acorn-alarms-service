use std::env;
use std::path::PathBuf;

fn main() {
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("no protoc");
    unsafe { env::set_var("PROTOC", protoc_path) };

    let devdb = "interface-definitions/proto/controls/service/DevDB/v1/DevDB.proto";
    let daq = "interface-definitions/proto/controls/service/DAQ/v1/DAQ.proto";
    let ioc = "interface-definitions/proto/controls/service/grpc-ioc-alarms/v1/ioc_alarms.proto";

    tonic_prost_build::configure()
        .build_server(false)
        .file_descriptor_set_path(PathBuf::from(env::var("OUT_DIR").unwrap()).join("fds.bin"))
        .compile_protos(&[devdb, daq, ioc], &["interface-definitions/"])
        .unwrap();
}
