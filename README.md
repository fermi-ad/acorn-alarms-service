# acorn-alarms-service

A service that combines the alarms pathways for EPICS and ACNET, and provides a unified UI for viewing alarms.

## Composition

* Alarm Viewer - a browser-hosted UI application that displays alarms to users
* Alarm Server - a program that is the concentration point for all alarms
* Alarm Log - a utility that records a time-ordered list of alarm events
