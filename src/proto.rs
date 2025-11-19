pub mod common {
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
