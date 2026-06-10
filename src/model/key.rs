//! Alarm identity keys derived from device names and alarm sources.

use crate::proto::common::alarm::{Status, status::Source};

#[cfg(test)]
mod tests;

/// Uniquely identifies an alarm by its device name and data source.
///
/// Two alarms are considered the same alarm when they share both the same
/// normalized device name and the same [`Source`]. [`Key`] encodes that
/// identity in a single value that can be used as a map key or serialized for
/// transport.
///
/// # Device-name normalization
///
/// The [`Key::device`] field is always stored with leading and trailing
/// whitespace stripped and all characters uppercased. Both [`From<&Status>`]
/// and [`TryFrom<&str>`] apply this normalization automatically.
///
/// # String representation
///
/// [`std::fmt::Display`] serializes a [`Key`] as
/// `"DEVICE_NAME#SourceVariant"` such as `"M:BEAM#Analog"`. This is the
/// format used as the Kafka message key so consumers can identify which alarm a
/// message belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    /// Normalized device name.
    pub device: String,
    /// The alarm data source.
    pub source: Source,
}

impl From<&Status> for Key {
    /// Constructs a [`Key`] from a [`Status`], normalizing the device name.
    fn from(status: &Status) -> Self {
        Self {
            device: status.device.trim().to_uppercase(),
            source: status.source(),
        }
    }
}

impl TryFrom<&str> for Key {
    type Error = String;

    /// Constructs a [`Key`] from a `"DEVICE#Source"` string.
    ///
    /// The device portion, to the left of `'#'`, is trimmed and uppercased. The
    /// source portion, to the right of `'#'`, is matched case-insensitively.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.split_once('#') {
            Some((device, source_str)) => {
                let source = match source_str.trim().to_uppercase().as_str() {
                    "ANALOG" => Source::Analog,
                    "DIGITAL" => Source::Digital,
                    "EPICS" => Source::Epics,
                    _ => return Err(format!("{s} does not contain a known alarms source.")),
                };
                let device = device.trim().to_uppercase();
                if device.is_empty() {
                    Err(format!("{s} is missing a device name."))
                } else {
                    Ok(Self { device, source })
                }
            }
            None => Err(format!(
                "{s} does not contain the expected '#' delimiter for separating device name from alarm source."
            )),
        }
    }
}

impl std::fmt::Display for Key {
    /// Serializes the key as `"DEVICE#SourceVariant"`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{:?}", self.device, self.source)
    }
}
