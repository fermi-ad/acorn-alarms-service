#[derive(Debug, Clone)]
pub struct DeviceSummary {
    pub name: String,
    pub description: Option<String>,
    pub reading_units: Option<String>,
    pub setting_units: Option<String>,
    pub num_status_bits: usize,
    pub num_control_cmds: usize,
    pub error_message: Option<String>,
}
