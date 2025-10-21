# About

This list all the requirements that fall into the 'alarms' umbrella. The goal is to capture all requirements and define what service will handle each requirement.

The table shows all requirements and service responsible for each requirement in the ACNET, EPICS, and ACORN architetures.

| Requirement                 | ACORN                  | ACNET                     | EPICS |
| --------------------------- | ---------------------- | ------------------------- | ----- |
| Report RAISED alarms        | Alarm Server           | Aelous                    |       |
| Query alarm log             | Alarm Server           | Aelous?                   |       |
| Compare readings & limits   | Alarm Server           | Front End                 |       |
| Acknowledge an alarm        | Alarm Server / States? | Alarm Screen App / States |       |
| Alarm Lists                 | AppDB Service          | App DB                    |       |
| Alarm display configuration | AppDB Service?         | App DB?                   |       |
| Alarm Groups                | DevDB Service          | Device DB                 |       |
| Bypass/Enable alarm         | DevDB Service?         | DevDB/FE                  |       |
| Snooze alarm                | ??                     | Monitor OAC?              |       |
| Change alarm limits         | DevDB Service?         | DevDB?                    |       |
