# Alarm Server

## Startup

On startup the server will request all the 'alarmable' devices from a service. The response should have:

- Digital or analog alarm blocks (or both)
- Within the alarm blocks:
  - Frequency of polling
  - Tries needed

## Alarm and Reading comparison

The server will request readings for devices/PVs with alarm blocks from DPM at the defined polling rate.

## Alarm State and Lifecycle

The server will have a formal definition of alarm states. The defined alarm states are:

- INDICATED
  - A device's reading has exceeded alarm limits, but not all conditions have been met
- RAISED
  - All conditions for the device to be alarm have been met.
- ACKNOWLEDGED
  - A boolean that determines wether or not a user has confirmed that they are acting on the alarm
  - A device can be still be RAISED regardless of the ACKNOWLEDGE status
- BYPASSED
  - A device will not be RAISED even if all conditions have been met

The server will use the `tried needed` to determine when a device should be raised and when should it should no longer be raised.

## Client Interface

The server will have a client gRPC interface. The interface will allow the client to subscribe to present alarms

## Historical Storage

Any device that has come into alarm should be pushed to long term storage. Data that should be stored are:

- Time device came into alarm
- Time device came out of alarm
- Reading when device was raised
- Alarm Limit at the time the device was raised

## Updates

The server will subscribe to changes to alarms blocks and update the alarm blocks stored in memory.
