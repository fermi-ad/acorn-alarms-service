use std::path::PathBuf;

fn main() {
    let base = PathBuf::from("interface-definitions");
    let proto_dir = base.join("proto");

    // Convert to string paths
    let include_base = base.to_str().unwrap();
    let include_proto = proto_dir.to_str().unwrap();

    tonic_build::configure()
        .build_server(false)
        .compile(
            &[
                "interface-definitions/proto/controls/service/DevDB/v1/DevDB.proto",
                "interface-definitions/proto/controls/service/DAQ/v1/DAQ.proto",
                "interface-definitions/proto/controls/service/grpc-ioc-alarms/v1/ioc_alarms.proto",
            ],
            &[
                include_base,   // resolves proto/controls/common/v1/*
                include_proto,  // resolves controls/service/*
            ],
        )
        .unwrap();
}
