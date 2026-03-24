# acorn-alarms-service

A service that combines the alarms pathways for EPICS and ACNET, and provides a unified UI for viewing alarms.

## Operation

### Environment variables

The following environment variables may be set to configure the associated system properties.

- `CONTROLS_ALARMS_TOPIC` -> Configures the name of the topic in the Controls Kafka instance that alarms will be published to
- `CONTROLS_KAFKA_HOST` -> Configures the address of the Controls Kafka instance
- `DEV_DB_ADDR` -> Configures the address of the device database gRPC service
- `DPM_ADDR` -> Configures the address of the DPM gRPC service
- `EPICS_DEV_DB_ADDR` -> Configures the address of the EPICS device database gRPC service
- `KAFKA_CONNECTION_SECONDS` -> Configures the amount of time, in seconds, to wait for a response from the Kafka server

## Development

The following packages must be present on the host machine when building this application:
- `cmake`
- `libcurl4-openssl-dev`
- `librdkafka-dev`
- `libsasl2-dev`
- `zlib`

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

| State          | Description
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
  ACKNOWLEDGED --> ALARMED: Device alarm (Severity increased)

  LATCHED --> BYPASSED: Bypass
  LATCHED --> OK: Acknowledge
  LATCHED --> ALARMED: Device alarm

  style OK            fill:#F0FFF0
  style LATCHED       fill:#F0FFF0
  style ALARMED       fill:#FFF0F0
  style ACKNOWLEDGED  fill:#FFF0F0
```

### Kafka Schema

A single Kafka topic shall be maintained for recording alarm state information. Keys shall be unique (non-duplicating messages) and shall record the most recent state for a given device alarm.

#### Key

The Key for a message shall be the device name, plus a suffix consisting of a '#' separator character and one of the possible Source values:
- "Analog"
- "Digital"
- "Epics"

See [Example Kafka Records](#example-kafka-records) below.

#### Value

The Value for a message shall be a JSON string.

#### JSON Schema for Values

Refer also to https://github.com/fermi-ad/interface-definitions/blob/main/proto/controls/common/v1/alarm.proto
```
{
  "Device"          : <name of device without Source suffix>    //  see Key above
  "Source"          : "Analog" | "Digital" | "Epics"
  "State"           : "Ok" | "Alarmed" | "Bypassed" | "Latched" | "Acknowledged"
  "Severity"        : "Low" | "High"                            //  (optional)
  "Acknowledgeable" : "True" | "False"                          //  (optional)
  "Time"            : { "seconds":<int64>, "nanos":<int32> }    //  per gRPC Timestamp
  "Detail"          : <uint32 meaning depends on Source field>  //  (optional)
  "User"            : <name of user who changed state>          //  (optional)
  "Wake"            : { "seconds":<int64>, "nanos":<int32> }    //  per gRPC Timestamp (optional)
}
```

#### Example Kafka Records

| Key | Value
|-----|------
| G:AMANDA#Analog | {<br>&emsp;"Device" : "G:AMANDA",<br>&emsp;"Source" : "Analog",<br>&emsp;"State" : "Bypassed",<br>&emsp;"Time" : { "seconds" : 1234, "nanos" : 5678 },<br>&emsp;"User" : "Dave",<br>&emsp;"Wake" : { "seconds" : 2345, "nanos" : 6789 }<br>}
| PIP2IT:pHB650_CRYO_TX103:TempK#Epics | {<br>&emsp;"Device" : "PIP2IT:pHB650_CRYO_TX103:TempK",<br>&emsp;"Source" : "Epics",<br>&emsp;"State" : "Alarmed",<br>&emsp;"Severity" : "Low",<br>&emsp;"Acknowledgeable" : "False",<br>&emsp;"Time" : { "seconds" : 1234, "nanos" : 5678 },<br>&emsp;"Detail" : 3<br>}
| Z:ACLTST#Digital | {<br>&emsp;"Device" : "Z:ACLTST",<br>&emsp;"Source" : "Digital",<br>&emsp;"State" : "Alarmed",<br>&emsp;"Severity" : "High",<br>&emsp;"Acknowledgeable" : "True",<br>&emsp;"Time" : { "seconds" : 1234, "nanos" : 5678 }<br>&emsp;"Detail" : 1234<br>}

### Alarm Configuration Info (Device DB)

There is information describing an alarm that is useful to users, but does not change frequently and is not part of a state transition message, but can be read from the respective Device DB's.  These data include:
  - Alarm Description
  - Guidance Message
  - Severity (ACNET)
  - Latchable (Acknowledgeable)

This information can change, but likely only infrequently.  A feasible update strategy for this information might be to query the Device DB for config data about an alarm whever a state change for that alarm is processed, with some throttle to prevent updates being too frequent (perhaps minimum 10 seconds between updates per device).

### Public Interface (GraphQL)

#### User Actions

- Acknowledge Alarm
- Bypass/Unbypass Alarm
- Manage Alarm List(s)
