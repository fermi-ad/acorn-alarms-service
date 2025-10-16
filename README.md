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

- An ALARM notifies users when Criteria are met
  - Criteria are conditions of Device data
- An ALARM may contain Devices (with Criteria)
- An ALARM may contain other ALARMS
  
- An ALARM is a persistent collection of states:
  - BYPASSED (boolean)
    - True: Alarm cannot be Raised (see STATUS:Raised)
    - False: Alarm can be Raised
  - ACKNOWLEDGED (boolean)
    - True: Alarm has been seen by a User and shall no longer be Raised
    - False: Alarm has not yet been seen by a User and shall be Raised
  - STATUS (enumeration)
    -  Quiet: Criteria are not met
    -  Indicated: Criteria are met but Alarm is BYPASSED or ACKNOWLEDGED
    -  Raised: Criteria are met and Alarm is not BYPASSED nor ACKNOWLEDGED
