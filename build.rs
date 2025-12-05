use std::env;
use std::path::PathBuf;

fn main() {
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("failed to find protoc");
    unsafe {
        env::set_var("PROTOC", protoc_path);
    }

    let proto_root = "interface-definitions/proto";

    let devdb = format!("{proto_root}/controls/service/DevDB/v1/DevDB.proto");
    let daq = format!("{proto_root}/controls/service/DAQ/v1/DAQ.proto");
    let ioc = format!("{proto_root}/controls/service/grpc-ioc-alarms/v1/ioc_alarms.proto");

    tonic_build::configure()
        .build_server(false)
        .file_descriptor_set_path(PathBuf::from(env::var("OUT_DIR").unwrap()).join("fds.bin"))
        // DevDB large enum
        .type_attribute(
            ".services.devdb.InfoEntry.result",
            "#[allow(clippy::large_enum_variant)]",
        )
        // DAQ enums
        .type_attribute(
            ".services.daq.Status",
            "#[allow(clippy::enum_variant_names)]",
        )
        .type_attribute(
            ".services.daq.Severity",
            "#[allow(clippy::enum_variant_names)]",
        )
        .type_attribute("Status", "#[allow(clippy::enum_variant_names)]")
        .type_attribute("Severity", "#[allow(clippy::enum_variant_names)]")
        // DeviceAlarmText is never constructed → allow dead code
        .type_attribute(".services.devdb.DeviceAlarmText", "#[allow(dead_code)]")
        .compile(
            &[devdb, daq, ioc],
            &[
                proto_root,
                "interface-definitions/proto/controls/common/v1",
                "interface-definitions/proto",
                "interface-definitions",
            ],
        )
        .unwrap();

    println!("cargo:rerun-if-changed=interface-definitions");
}
