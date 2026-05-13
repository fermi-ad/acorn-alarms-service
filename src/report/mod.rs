//! Alarm state tracking and Kafka publishing.
//!
//! This module is responsible for maintaining a live cache of active alarms
//! and publishing state changes to a Kafka topic so downstream consumers stay
//! in sync.  The central type is [`AlarmsReporter`].
//!
//! ## Operator actions are source-specific
//!
//! The `device` parameter accepted by [`set_bypass`](AlarmsReporter::set_bypass),
//! [`set_snooze`](AlarmsReporter::set_snooze),
//! [`set_active`](AlarmsReporter::set_active), and
//! [`set_acknowledged`](AlarmsReporter::set_acknowledged) must be encoded in
//! `DEVICE#Source` format — the same format used as the Kafka message key
//! (e.g. `"M:BEAM#Analog"`, `"Z:ACLTST#Digital"`).  Each action applies only
//! to the specific `(device, source)` pair identified by that string; other
//! sources for the same device are unaffected.

use std::collections::HashMap;

use chrono::Utc;
use rust_env_var_lib::env_var;
use rust_pubsub_lib::{Message, Publisher};
use tracing::{debug, error, warn};

use crate::proto::{
    common::alarm::{
        Status,
        status::{Severity, Source, State},
    },
    google::protobuf::Timestamp,
};

#[cfg(test)]
mod tests;

const CONTROLS_KAFKA_HOST: &str = "CONTROLS_KAFKA_HOST";
const DEFAULT_CONTROLS_HOST: &str = "kafka-cluster-kafka-bootstrap.kafka.svc.adkube.fnal.gov:9092";

const CONTROLS_ALARMS_TOPIC: &str = "CONTROLS_ALARMS_TOPIC";
const DEFAULT_CONTROLS_TOPIC: &str = "alarms";

/// Tracks the live alarm state for all devices and publishes state changes to
/// Kafka.
///
/// `AlarmsReporter` is the central coordinator between the three alarm input
/// paths (Redis stream, gRPC acknowledge, gRPC bypass/snooze) and the Kafka
/// topic that downstream consumers subscribe to.
///
/// # Cache
///
/// Only alarms that are **not** in the `Ok` state are kept in the internal
/// cache (`known_alarms`).  When an alarm transitions back to `Ok` it is
/// removed.  [`get_snapshot`](AlarmsReporter::get_snapshot) returns a point-in-time
/// copy of every currently active alarm.
///
/// **The cache is a mirror of confirmed Kafka state.**  An entry is added to
/// (or removed from) `known_alarms` only after the corresponding Kafka publish
/// succeeds.  If the publish fails the cache is left unchanged so that the
/// next incoming update can retry the transition.
///
/// ## Bypass state in the cache
///
/// A bypass or snooze is stored at the **same `Key`** as the real alarm entry
/// for that `(device, source)` pair, with `state: Bypassed`.  There is no
/// separate sentinel key — the bypass state is simply the current value at
/// `Key { device, source }`.  [`should_publish`](AlarmsReporter::should_publish)
/// suppresses incoming alarms for a `(device, source)` pair by checking
/// whether the existing cache entry for that exact key has `state == Bypassed`.
///
/// # Duplicate suppression
///
/// Before publishing, [`report`](AlarmsReporter::report) checks whether the
/// incoming alarm represents a meaningful change from the last known state.
/// Repeated identical updates and non-actionable state transitions are
/// silently dropped.
pub struct AlarmsReporter<P: Publisher> {
    controls_publisher: P,
    known_alarms: HashMap<Key, Status>,
}

impl<P: Publisher> AlarmsReporter<P> {
    /// Creates a new reporter, connecting to the Kafka broker and topic
    /// specified by the `CONTROLS_KAFKA_HOST` and `CONTROLS_ALARMS_TOPIC`
    /// environment variables (falling back to built-in defaults when those
    /// variables are absent).
    pub fn new() -> Self {
        Self {
            controls_publisher: get_publisher(),
            known_alarms: HashMap::new(),
        }
    }

    /// Returns a snapshot of every alarm that is currently in a non-`Ok` state.
    ///
    /// Each element is a clone of the full [`Status`] stored in the cache, so
    /// the caller receives an independent copy that will not be affected by
    /// subsequent calls to [`report`](AlarmsReporter::report).
    ///
    /// The order of the returned slice is unspecified.
    pub fn get_snapshot(&self) -> Vec<Status> {
        self.known_alarms.values().cloned().collect()
    }

    /// Bypasses a specific `DEVICE#Source`, suppressing future alarms for that
    /// source until [`set_active`](AlarmsReporter::set_active) is called.
    ///
    /// `device_source` must be in `"DEVICE#Source"` format
    /// (e.g. `"M:BEAM#Analog"`).  Only the identified `(device, source)` pair
    /// is affected; other sources for the same device continue to alarm
    /// normally.
    ///
    /// # Wire-format contract
    ///
    /// A bypass is represented in both the internal cache and on the Kafka
    /// wire as a `Status` entry at `Key { device, source }` with
    /// `state: Bypassed`.  When a bypass is set:
    ///
    /// 1. A `Status` with `state: Bypassed`, `severity: Unknown`, and
    ///    `wake: None` is published to Kafka under the key `"DEVICE#Source"`.
    /// 2. Only if the publish succeeds: any existing alarm entry for that
    ///    exact `(device, source)` pair is **replaced** by the bypass record
    ///    in the cache.
    ///
    /// If the publish fails the cache is left unchanged so the next attempt
    /// can retry.
    ///
    /// # Why publish to Kafka
    ///
    /// This service is one of many consumers watching the `alarms` Kafka topic.
    /// All other consumers (display clients, alarm loggers, etc.) learn about
    /// operator actions exclusively through Kafka; the gRPC command handler is
    /// the only writer.  The published record is therefore not a side effect —
    /// it is the primary notification mechanism for the rest of the system.
    ///
    /// # Wire shape
    ///
    /// `state: Bypassed`, `source: <actual source>`, `severity: Unknown`,
    /// `wake: None`.
    ///
    /// This is the inverse of [`set_active`](AlarmsReporter::set_active).
    pub fn set_bypass(&mut self, device_source: String, user: String) {
        let key = Key::from(device_source.as_str());

        self.apply_inactive_state(key, user, None);
    }

    /// Snoozes a specific `DEVICE#Source` until the given `wake` time.
    ///
    /// `device_source` must be in `"DEVICE#Source"` format
    /// (e.g. `"M:BEAM#Analog"`).  Only the identified `(device, source)` pair
    /// is affected; other sources for the same device continue to alarm
    /// normally.
    ///
    /// Snooze is a time-limited bypass.  The same contract as
    /// [`set_bypass`](AlarmsReporter::set_bypass) applies:
    ///
    /// 1. A `Status` with `state: Bypassed`, `severity: Unknown`, and
    ///    `wake: Some(timestamp)` is published to Kafka under the key
    ///    `"DEVICE#Source"`.
    /// 2. Only if the publish succeeds: any existing alarm entry for that
    ///    exact `(device, source)` pair is **replaced** by the snooze record
    ///    in the cache.
    ///
    /// If the publish fails the cache is left unchanged so the next attempt
    /// can retry.  Consumers that need to distinguish a permanent bypass from a
    /// snooze should check whether `wake` is `Some`.
    ///
    /// # Why publish to Kafka
    ///
    /// All other consumers learn about operator actions exclusively through
    /// Kafka; the gRPC command handler is the only writer.
    ///
    /// # Wire shape
    ///
    /// `state: Bypassed`, `source: <actual source>`, `severity: Unknown`,
    /// `wake: Some(timestamp)`.
    ///
    /// This is the inverse of [`set_active`](AlarmsReporter::set_active).
    pub fn set_snooze(&mut self, device_source: String, wake: Timestamp, user: String) {
        let key = Key::from(device_source.as_str());

        self.apply_inactive_state(key, user, Some(wake));
    }

    /// Removes a bypass or snooze from a specific `DEVICE#Source` and
    /// publishes an `Unbypassed` event for that source.
    ///
    /// `device_source` must be in `"DEVICE#Source"` format
    /// (e.g. `"M:BEAM#Analog"`).  Only the identified `(device, source)` pair
    /// is affected; bypasses on other sources for the same device are
    /// unaffected.
    ///
    /// # Wire-format contract
    ///
    /// The bypass record for a `(device, source)` pair is the cache entry at
    /// `Key { device, source }` with `state == Bypassed`.  This method
    /// publishes an `Unbypassed` event for that exact key so downstream
    /// consumers know the source has re-entered normal alarm monitoring, and
    /// only removes the entry from the cache if the publish succeeds.
    ///
    /// # Why publish to Kafka
    ///
    /// All other consumers learn about operator actions exclusively through
    /// Kafka; the gRPC command handler is the only writer.  Downstream
    /// consumers rely on this event to learn that the source has re-entered
    /// normal alarm monitoring.
    ///
    /// # Wire shape
    ///
    /// `state: Unbypassed`, `source: <actual source>`, `severity: Unknown`,
    /// `wake: None`.
    ///
    /// Events are published directly via [`handle_publish`], bypassing the
    /// [`should_publish`] duplicate-suppression logic (which would otherwise
    /// block the event because the entry is being removed, not updated).
    /// The entry is removed from the cache only after a successful publish.
    ///
    /// This is the inverse of [`set_bypass`](AlarmsReporter::set_bypass) and
    /// [`set_snooze`](AlarmsReporter::set_snooze).
    pub fn set_active(&mut self, device_source: String, user: String) {
        let key = Key::from(device_source.as_str());
        let now = chrono::Utc::now();
        let new_time = Some(Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        });

        // Peek at the bypass entry without removing it yet.
        // The entry is only removed from the cache after a successful Kafka
        // publish so that the cache remains a mirror of confirmed Kafka state.
        // Only proceed if the entry is actually Bypassed — set_active must not
        // clear a live Alarmed or Acknowledged entry.
        match self.known_alarms.get(&key) {
            Some(entry) if entry.state() == State::Bypassed => {
                let mut unbypassed = entry.clone();
                unbypassed.state = State::Unbypassed as i32;
                unbypassed.time = new_time;
                unbypassed.user = user;
                unbypassed.wake = None;
                match serde_json::to_string(&unbypassed) {
                    Ok(body) => {
                        if self.handle_publish(alarm_to_message(&key, body)) {
                            // Publish confirmed — now remove the entry from the cache.
                            self.known_alarms.remove(&key);
                        }
                    }
                    Err(err) => {
                        error!(
                            "Failed to serialize unbypassed status for {}: {}",
                            unbypassed.device, err
                        );
                    }
                }
            }
            Some(entry) => {
                warn!(
                    device = %key.device,
                    source = ?key.source,
                    state = ?entry.state(),
                    "Activate called but device-source is not currently bypassed"
                );
            }
            None => {
                warn!(device = %key.device, source = ?key.source, "Activate called but device-source has no active bypass");
            }
        }
    }

    /// Records an acknowledgement for a specific `DEVICE#Source` and publishes
    /// an `Acknowledged` event for that source.
    ///
    /// `device_source` must be in `"DEVICE#Source"` format
    /// (e.g. `"M:BEAM#Analog"`).  Only the identified `(device, source)` pair
    /// is affected; other sources for the same device are unaffected.
    ///
    /// The existing cache entry for the `(device, source)` pair is cloned,
    /// mutated (`state`, `time`, `user`), and published to Kafka.  Only if
    /// the publish succeeds is the mutated clone written back into the cache,
    /// so that `known_alarms` always mirrors confirmed Kafka state.
    /// `severity`, `epics_type`, and all other fields are preserved.
    ///
    /// # Bypass guard
    ///
    /// If the entry for the `(device, source)` pair is currently `Bypassed`
    /// (or `Snoozed`), the acknowledgement is silently ignored.  An
    /// acknowledgement must not pull a source out of bypass — only
    /// [`set_active`](AlarmsReporter::set_active) may do that.
    ///
    /// This mirrors the structure of [`set_bypass`](AlarmsReporter::set_bypass):
    /// the domain logic lives on the reporter rather than in the gRPC handler.
    pub fn set_acknowledged(&mut self, device_source: String, user: String) {
        let key = Key::from(device_source.as_str());

        let now = Utc::now();
        let new_time = Some(Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        });

        // Clone and mutate the single matching entry, then publish.
        // The cache is only updated after a successful Kafka publish.
        // Entries in the Bypassed state are skipped — an acknowledgement must
        // not pull a source out of bypass.
        if let Some(prev) = self.known_alarms.get(&key) {
            if prev.state() == State::Bypassed {
                warn!(
                    device = %key.device,
                    source = ?key.source,
                    "Acknowledge rejected: device-source is currently bypassed"
                );
                return;
            }
            let mut updated = prev.clone();
            updated.state = State::Acknowledged as i32;
            updated.time = new_time;
            updated.user = user;

            let message = match serde_json::to_string(&updated) {
                Ok(body) => alarm_to_message(&key, body),
                Err(err) => {
                    error!(
                        "Failed to serialize acknowledged status for {}: {}",
                        key.device, err
                    );
                    return;
                }
            };
            if self.handle_publish(message) {
                self.known_alarms.insert(key, updated);
            }
        } else {
            warn!(device = %key.device, source = ?key.source, "Acknowledge called but device-source has no active alarm");
        }
    }

    /// Processes an incoming alarm from any input path (Redis stream, etc.) and
    /// publishes it to Kafka if it represents a meaningful state change.
    ///
    /// The cache is updated **only after** a successful Kafka publish so that
    /// `known_alarms` mirrors confirmed Kafka state rather than attempted state.
    pub fn report(&mut self, alarm: Status) {
        if self.should_publish(&alarm) {
            let message_body = match serde_json::to_string(&alarm) {
                Ok(body) => body,
                Err(err) => {
                    error!(
                        "Failed to serialize alarm object for {}#{:?}\n{}",
                        alarm.device,
                        alarm.source(),
                        err
                    );
                    return;
                }
            };

            debug!(target = "kafka", payload = %message_body, "Kafka payload");

            let key = Key::from(&alarm);
            let message: Message = alarm_to_message(&key, message_body);

            // Only update the cache after a successful Kafka publish so that
            // known_alarms mirrors confirmed Kafka state.
            if self.handle_publish(message) {
                if alarm.state() == State::Ok {
                    self.known_alarms.remove(&key);
                } else {
                    self.known_alarms.insert(key, alarm);
                }
            }
        }
    }

    /// Shared logic for [`set_bypass`] and [`set_snooze`].
    ///
    /// Stores the bypass/snooze state at the same `Key` as the real alarm
    /// entry for the `(device, source)` pair:
    ///
    /// 1. Build a `Status` with `state: Bypassed` (and the actual `source`)
    ///    and publish it to Kafka so all other consumers learn about the
    ///    operator action.
    /// 2. Only if the publish succeeds: **replace** any existing entry for
    ///    `key` in the cache with the bypass record.
    ///
    /// If the publish fails the cache is left unchanged so the next attempt
    /// can retry.  Only the single `(device, source)` entry is touched —
    /// other sources for the same device are unaffected.
    ///
    /// [`should_publish`](AlarmsReporter::should_publish) suppresses incoming
    /// alarms for a `(device, source)` pair by checking whether the existing
    /// cache entry for that exact key has `state == Bypassed`.  Because this
    /// method always writes that entry on success, a single key lookup in
    /// `should_publish` is sufficient to suppress subsequent alarms on that
    /// specific source.
    fn apply_inactive_state(&mut self, key: Key, user: String, wake: Option<Timestamp>) {
        let now = Utc::now();
        let new_time = Some(Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        });
        // Build the bypass record — needed for serialization before we know
        // whether the publish will succeed.
        let record = Status {
            device: key.device.clone(),
            severity: Severity::Unknown as i32,
            state: State::Bypassed as i32,
            source: key.source as i32,
            acknowledgeable: false,
            time: new_time,
            epics_type: String::default(),
            user,
            wake,
        };

        let message_body = match serde_json::to_string(&record) {
            Ok(msg) => msg,
            Err(e) => {
                error!(
                    "Failed to serialize bypassed status for {}: {}",
                    record.device, e
                );
                return;
            }
        };
        let message = alarm_to_message(&key, message_body);

        // Only update the cache after a successful Kafka publish so that
        // known_alarms mirrors confirmed Kafka state.
        if self.handle_publish(message) {
            // Replace any existing entry for this (device, source) pair with
            // the bypass record.  Other sources for the same device are
            // unaffected.
            self.known_alarms.insert(key, record);
        }
    }

    fn transition_allowed(prev: State, next: State) -> bool {
        matches!(
            (prev, next),
            (State::Ok, State::Alarmed)
                | (State::Alarmed, State::Acknowledged)
                | (State::Alarmed, State::Ok)
                | (State::Acknowledged, State::Ok)
                | (State::Acknowledged, State::Alarmed)
        )
    }

    /// Determines whether an incoming alarm should be published to Kafka.
    ///
    /// # Bypass suppression
    ///
    /// Before checking for state changes, this method looks up the existing
    /// cache entry for the exact `Key { device, source }` of the incoming
    /// alarm.  If that entry exists with `state == Bypassed`, the alarm is
    /// suppressed unconditionally.  This works because
    /// [`apply_inactive_state`](AlarmsReporter::apply_inactive_state) stores
    /// the bypass state at the same key as the real alarm entry — making the
    /// presence of a `Bypassed` entry the complete, source-specific bypass
    /// signal.  No device-wide scan is needed.
    fn should_publish(&self, alarm: &Status) -> bool {
        let key = Key::from(alarm);

        let prev = self.known_alarms.get(&key);

        // Check for a source-specific bypass before evaluating state changes.
        if let Some(prev_status) = prev
            && prev_status.state() == State::Bypassed
        {
            debug!(
                target = "alarm_transition",
                device = %key.device,
                source = ?key.source,
                "Skipping alarm due to source-specific bypass"
            );
            return false;
        }

        let next_state = alarm.state();
        let next_severity = alarm.severity();

        let changed = match prev {
            None => true,
            Some(prev_status) => {
                Self::transition_allowed(prev_status.state(), next_state)
                    || prev_status.severity() != next_severity
            }
        };
        if !changed {
            debug!(
                target = "alarm_transition",
                device = %key.device,
                source = ?key.source,
                previous = ?prev.map(|s| (s.state(), s.severity())),
                current = ?(next_state, next_severity),
                "Duplicate or non-actionable transition skipped"
            );
        } else {
            debug!(
                target = "alarm_transition",
                device = %key.device,
                source = ?key.source,
                previous = ?prev.map(|s| (s.state(), s.severity())),
                current = ?(next_state, next_severity),
                "Alarm state transition detected"
            );
        }

        changed
    }

    fn handle_publish(&mut self, message: Message) -> bool {
        let key = message.key.clone();
        self.controls_publisher
            .publish(message)
            .inspect_err(|err| {
                error!(
                    target = "kafka",
                    error = ?err,
                    key = ?key,
                    "Kafka publish failed"
                )
            })
            .is_ok()
    }
}

/// Uniquely identifies an alarm by its device name and data source.
///
/// Two alarms are considered the same alarm when they share both the same
/// (normalized) device name and the same [`Source`].  `Key` encodes that
/// identity in a single value that can be used as a [`HashMap`] key.
///
/// # Device-name normalization
///
/// The `device` field is always stored with leading/trailing whitespace
/// stripped and all characters uppercased.  Both [`From<&Status>`] and
/// [`From<&str>`] apply this normalization automatically, so callers never
/// need to do it manually.
///
/// # String representation
///
/// [`Display`](std::fmt::Display) serializes a `Key` as
/// `"DEVICE_NAME#SourceVariant"` (e.g. `"M:BEAM#Analog"`).  This is the
/// format used as the Kafka message key so consumers can identify which alarm
/// a message belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    /// Normalized device name (trimmed + uppercased).
    device: String,
    /// The alarm data source (Analog, Digital, Epics, …).
    source: Source,
}

/// Constructs a [`Key`] from a [`Status`], normalizing the device name in the
/// process.
///
/// The device name is trimmed of surrounding whitespace and uppercased so that
/// `"m:beam "`, `"M:BEAM"`, and `" M:Beam"` all produce the same key.
impl From<&Status> for Key {
    fn from(status: &Status) -> Self {
        Self {
            device: status.device.trim().to_uppercase(),
            source: status.source(),
        }
    }
}

/// Constructs a [`Key`] from a `"DEVICE#Source"` string, as used by the
/// Kafka message key format and the gRPC `device` field for operator actions.
///
/// The device portion (left of `'#'`) is trimmed and uppercased.  The source
/// portion (right of `'#'`) is matched case-insensitively:
///
/// | String    | [`Source`] variant |
/// |-----------|--------------------|
/// | `"Analog"` | [`Source::Analog`] |
/// | `"Digital"` | [`Source::Digital`] |
/// | `"Epics"` | [`Source::Epics`] |
/// | anything else | [`Source::Unknown`] |
///
/// If the string contains no `'#'` separator the entire string is treated as
/// the device name and `source` defaults to [`Source::Unknown`].
impl From<&str> for Key {
    fn from(s: &str) -> Self {
        match s.split_once('#') {
            Some((device, source_str)) => Self {
                device: device.trim().to_uppercase(),
                source: match source_str.trim().to_uppercase().as_str() {
                    "ANALOG" => Source::Analog,
                    "DIGITAL" => Source::Digital,
                    "EPICS" => Source::Epics,
                    _ => Source::Unknown,
                },
            },
            None => Self {
                device: s.trim().to_uppercase(),
                source: Source::Unknown,
            },
        }
    }
}

/// Serializes the key as `"DEVICE#SourceVariant"` — the format used as the
/// Kafka message key.
impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{:?}", self.device, self.source)
    }
}

fn get_publisher<P: Publisher>() -> P {
    let host = env_var::get(CONTROLS_KAFKA_HOST).or_else(|| DEFAULT_CONTROLS_HOST.to_string());

    let topic = env_var::get(CONTROLS_ALARMS_TOPIC).or_else(|| DEFAULT_CONTROLS_TOPIC.to_string());

    P::new(host, topic)
}

/// Wraps a serialized alarm payload in a [`Message`] ready for publishing.
///
/// The message key is the [`Key`] string representation of the alarm
/// (`"DEVICE#SourceVariant"`), which lets downstream consumers identify which
/// alarm a Kafka message belongs to without deserializing the body.
fn alarm_to_message(key: &Key, message_body: String) -> Message {
    Message {
        key: Some(key.to_string()),
        value: message_body,
    }
}
