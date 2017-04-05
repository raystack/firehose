#### Introduction

Filter expressions are allowed to filter EsbMessages just after reading from Kafka and before sending to Sink.

##### Rules to write expressions:

* All the expressions are like a piece of Java code.
* Follow rules for every data type, as like writing a Java code.
* Attach `_` at last of the outer field names.

**Example**

Sample driver location message:

```
===================KEY==========================
driver_id: "COJRXpCPIYrIASdJ4W8gbqzeTt1PGl"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE

================= MESSAGE=======================
driver_id: "COJRXpCPIYrIASdJ4W8gbqzeTt1PGl"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE
app_version: "kqChbcqL0zYevPalGQEAdIrjZbwfKV"
driver_location {
  latitude: 0.6487193703651428
  longitude: 0.791822075843811
  altitude_in_meters: 0.9949166178703308
  accuracy_in_meters: 0.39277541637420654
  speed_in_meters_per_second: 0.28804516792297363
}
gcm_key: "LefFCyvIVkJVgOL6d4uKBlMxlpyus1"
```

***Key based filter expressions examples:***

* `driverId_ == 'COJRXpCPIYrIASdJ4W8gbqzeTt1PGl'`  (`''` for `string` comparison)
* `vehicleType_ == 2` (Give Enum sequence for enum values to be compared)
* `eventTimestamp_.seconds == 186178` (access nested elements by `.`)
* `driverId_ == 'COJRXpCPIYrIASdJ4W8gbqzeTt1PGl' && vehicleType_ == 2` (multiple conditions example 1)
* `vehicleType_ == 2 || eventTimestamp_.seconds == 186178` (multiple conditions example 2)

***Message based filter expressions examples:***

* `gcmKey_ == 'LefFCyvIVkJVgOL6d4uKBlMxlpyus1'`
* `driverId_ == 'COJRXpCPIYrIASdJ4W8gbqzeTt1PGl' && driverLocation_.longitude > 0.791822075843811`
* `driverLocation_.altitudeInMeters > 0.9949166178703308`
