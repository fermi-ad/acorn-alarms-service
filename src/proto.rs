pub mod common {
    pub mod alarm {
        tonic::include_proto!("common.alarm");
    }

    pub mod device {
        tonic::include_proto!("common.device");
    }

    pub mod status {
        tonic::include_proto!("common.status");
    }
}

pub mod google {
    pub mod protobuf {
        tonic::include_proto!("google.protobuf");
    }
}

pub mod services {
    pub mod devdb {
        tonic::include_proto!("services.devdb");
    }

    pub mod daq {
        tonic::include_proto!("services.daq");
    }

    pub mod ioc_alarms {
        tonic::include_proto!("services.ioc_alarms");
    }

    pub mod alarm_commands {
        tonic::include_proto!("services.alarm_commands");
    }
}
