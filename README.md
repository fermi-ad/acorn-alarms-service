# acorn-alarms-service

A service that combines the alarms pathways for EPICS and ACNET, and provides a unified UI for viewing alarms.

## Composition

* Alarm Viewer - a browser-hosted UI application that displays alarms to users
* Alarm Server - a program that is the concentration point for all alarms
* Alarm Log - a utility that records a time-ordered list of alarm events

## Problem Analysis Notes

Likely to be moved to the Projects or Wiki tab at some point

### Concepts

Defining a common terminology for referring to analogous concepts between ACNET and EPICS

| Generic   | ACNET         | EPICS
|-----------|---------------|------
| Node      | Node          | IOC
| Device    | Device        | PV
| Property  | Property      | Attribute Structure
| Attribute | Attribute     | Field of Attribute Structure
| Value     | Reading Value | VAL Field

### ALARM

- An ALARM notifies users when Conditions are met
  - Conditions may be comparisons of Device data to limits
  - Conditions may be explicit boolean data from Devices
  - Conditions may be the states of other associated ALARMs
  - Conditions may be complex (require multiple triggers over time or constant trigger over time)
  
- An ALARM is a persistent collection of states:
  - BYPASSED (boolean)
    - True: Alarm cannot be Raised (see STATUS:Raised)
    - False: Alarm can be Raised
  - ACKNOWLEDGED (boolean)
    - True: Alarm has been seen by a User and shall no longer be Raised
    - False: Alarm has not yet been seen by a User and shall be Raised
  - STATUS (enumeration)
    -  Quiet: Conditions are not met
    -  Indicated: Conditions are met but Alarm is BYPASSED or ACKNOWLEDGED
    -  Raised: Conditions are met and Alarm is not BYPASSED nor ACKNOWLEDGED

### Alarms Log

The Alarms Log will be a timestamped list of Alarms Events

### Alarms Event

An Alarms Event shall describe the state change of an ALARM

| Event Attribute | Description
|-----------------|------------
| Timestamp       | Date and Time of event
| New State       | New state of alarm (Quiet, Indicated or Raised)
| Action          | Bypassed changed, Acknowledged changed, Conditions met
| Actor           | User (for bypass and acknowledge) or Device

### Interfaces

#### Alarm Server to Alarm Source (DPM)

#### Alarm Server to Alarm Log

#### Alarm Server to Alarm Viewer


