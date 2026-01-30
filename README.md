# acorn-alarms-service

A service that combines the alarms pathways for EPICS and ACNET, and provides a unified UI for viewing alarms.

## Operation

### Environment variables

The following environment variables may be set to configure the associated system properties.

- `CONTROLS_ALARMS_TOPIC` -> Configures the name of the topic in the Controls Kafka instance that alarms will be published to
- `CONTROLS_KAFKA_HOST` -> Configures the address of the Controls Kafka instance
- `DEV_DB_ADDR` -> Configures the address of the device database gRPC service
- `KAFKA_CONNECTION_SECONDS` -> Configures the amount of time, in seconds, to wait for a response from the Kafka server
- `PIP_II_ALARMS_TOPIC` -> Configures the name of the topic in the PIP-II Kafka instance that alarms will be published to
- `PIP_II_KAFKA_HOST` -> Configures the address of the PIP-II Kafka instance

## Design

### Startup

- Determine the set of all device alarms
  - Request all Devices having analog and/or digital alarms from an ACNET Device gRPC service
  - Request all Devices (i.e. PV's) having value alarms from an EPICS Device gRPC service
- If necessary, read the initial alarm state of every device alarm (TBD - we may get this for free by setting the listeners below)
- Read the most recent information from Kafka for all device alarms
  - Bypass state (EPICS only)
  - Snooze stop time (ACNET and EPICS)
  - Acknowledge state (ACNET and EPICS)
- Begin listening to all device alarms via DPM/Data-Cache for changes to alarm state
  - Devices should be divided into manageable subsets and listened to via multiple DRF requests (rather than one huge one)

### Alarm Listening

- Report changes of alarm states by adding records to a Kafka service
- Monitor Kafka for changes from clients (ACORN or Phoebus)
- Periodically re-request the set of all device alarms from Device gRPC service(s)
  - Cancel and re-issue alarm listening requests if device alarm set has changed

### Device/Kafka Alarm States

| ACORN          | Description
|----------------|------------
| `OK`           | There is not an alarm
| `ALARMED`      | There is an alarm
| `BYPASSED`     | There may be an alarm but we are ignoring it
| `ACKNOWLEDGED` | There is an alarm but user has acknowledged it
| `LATCHED`      | There was an alarm but no user acknowledged it

- Device is source of truth for alarm (ACNET and EPICS)
- Device is source of truth for bypass (ACNET)
- Kafka is source of truth for bypass (EPICS)
- Kafka is source of truth for acknowledge (ACNET and EPICS)
- Complete alarm state information can always be read from Kafka

### Server State Transitions

```mermaid
stateDiagram-v2
  OK --> BYPASSED: Bypass
  OK --> ALARMED: Device alarm

  BYPASSED --> OK: Unbypass (Device ok)
  BYPASSED --> ALARMED: Unbypass (Device alarm)

  ALARMED --> BYPASSED: Bypass
  ALARMED --> OK: Device ok (Not latching)
  ALARMED --> LATCHED: Device ok (Latching)
  ALARMED --> ACKNOWLEDGED: Acknowledge

  ACKNOWLEDGED --> BYPASSED: Bypass
  ACKNOWLEDGED --> OK: Device ok

  LATCHED --> BYPASSED: Bypass
  LATCHED --> OK: Acknowledge
  LATCHED --> ALARMED: Device alarm

  style OK            fill:#F0FFF0
  style LATCHED       fill:#F0FFF0
  style ALARMED       fill:#FFF0F0
  style ACKNOWLEDGED  fill:#FFF0F0
```

### Kafka Schema

A single Kafka topic shall be maintained for recording alarm state information. Keys shall be unique (non-duplicating records) and shall record the most recent state of alarms for a given device.

The Key for a record shall be the device name, and the payload shall be a JSON string.

Given the respective rules of operation for ACNET and EPICS:
- An ACNET device record shall only have either a single "analog" element and/or a single "digital" element.
- An EPICS device record shall only have a single "epics" element.

#### JSON Schema for Values
```
{
  "analog"  : { <JSON> }  //  ACNET analog alarm (can also have digital)
  "digital" : { <JSON> }  //  ACNET digital alarm (can also have analog)
  "epics"   : { <JSON> }  //  EPICS alarm (type specified by "type" below)(aka EPICS 'status')
  "state"   : "ok" | "alarmed" | "bypassed" | "latched" | "acknowledged"
  "time"    : <uint64 nanos since epoch 1/1/70>
  "type"    : "hihi" | "high" | "low" | "lolo" | ...  //  optional see EPICS alarm status definitions
  "user"    : <name of user who changed state>        //  optional
  "wake"    : <uint64 nanos since epoch 1/1/70>       //  optional
}
```
[EPICS alarm status definitions]( https://docs.epics-controls.org/projects/base/en/7.0.10/alarm_h.html)

#### Example Kafka Records
```
G:AMANDA  { "analog":{ "state":"acknowledged", "time":1234567890, "user":"dave" }, "digital":{ "state":"alarmed", "time":1234500000 }}

PIP2IT:pHB650_CRYO_TX103:TempK  { "epics":{ "state":"alarmed", "time":1234567890, "type": "hihi" }}

Z:ACLTST  { "digital":{ "state":"bypassed", "time":1234567890, "user":"dave", "wake":1234599999 }}
```

### User Actions

- Acknowledge Alarm
- Bypass/Unbypass Alarm
- Manage Alarm List(s)
