use std::env;
use std::path::PathBuf;

fn main() {
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("failed to find protoc");
    unsafe {
        env::set_var("PROTOC", protoc_path);
    }

    let devdb = "interface-definitions/proto/controls/service/DevDB/v1/DevDB.proto";
    let daq = "interface-definitions/proto/controls/service/DAQ/v1/DAQ.proto";
    let ioc_alarms =
        "interface-definitions/proto/controls/service/grpc-ioc-alarms/v1/ioc_alarms.proto";
    let alarm_struct = "interface-definitions/proto/controls/common/v1/alarm.proto";

    let alarm_commands =
        "interface-definitions/proto/controls/service/grpc-alarm-commands/v1/alarm_commands.proto";

    tonic_prost_build::configure()
        .build_server(true)
        .file_descriptor_set_path(PathBuf::from(env::var("OUT_DIR").unwrap()).join("fds.bin"))
        .type_attribute(
            ".google.protobuf",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_well_known_types(true)
        .type_attribute(
            ".services.devdb.InfoEntry.result",
            "#[allow(clippy::large_enum_variant)]",
        )
        .type_attribute(
            ".common.alarm",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(
            &[devdb, daq, ioc_alarms, alarm_struct, alarm_commands],
            &["interface-definitions/"],
        )
        .unwrap();
}
