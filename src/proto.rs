pub mod common {
    pub mod alarm {
        include!(concat!(env!("OUT_DIR"), "/common.alarm.rs"));
    }

    pub mod device {
        include!(concat!(env!("OUT_DIR"), "/common.device.rs"));
    }

    pub mod status {
        include!(concat!(env!("OUT_DIR"), "/common.status.rs"));
    }
}

pub mod services {
    pub mod daq {
        include!(concat!(env!("OUT_DIR"), "/services.daq.rs"));
    }

    pub mod devdb {
        include!(concat!(env!("OUT_DIR"), "/services.devdb.rs"));
    }

    pub mod ioc_alarms {
        include!(concat!(env!("OUT_DIR"), "/services.ioc_alarms.rs"));
    }
}

pub mod google {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}


// NOTE:
// This file is central module for all protobuf-generated code.
// tonic/prost generates many .rs files in OUT_DIR, but they are not part of
// the crate automatically. proto.rs exposes them by including each file and
// rebuilding the correct module tree (services.*, common.*, etc.).
//
// Without this file, modules like services.daq, services.devdb, and
// common.device/common.status would not be visible, causing errors such as
// “could not find `common` in super”. This structure is for consistent,
// shared protobuf namespaces across DevDB, DAQ, and IOC clients.
