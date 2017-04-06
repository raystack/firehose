#### Introduction

Filter expressions are allowed to filter EsbMessages just after reading from Kafka and before sending to Sink.

##### Rules to write expressions:

* All the expressions are like a piece of Java code.
* Follow rules for every data type, as like writing a Java code.
* Access nested fields by `.` and `()`, i.e., `driverLocationLogMessage.getVehicleType()`

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

* `driverLocationLogKey.getDriverId() == 'COJRXpCPIYrIASdJ4W8gbqzeTt1PGl'`
* `driverLocationLogKey.getVehicleType == 'BIKE'` (Give Enum values to be compared)
* `driverLocationLogKey.getEventTimestamp().getSeconds() == 186178`
* `driverLocationLogKey.getDriverId() == 'COJRXpCPIYrIASdJ4W8gbqzeTt1PGl' && driverLocationLogKey.getVehicleType == 'BIKE'` (multiple conditions example 1)
* `driverLocationLogKey.getVehicleType == 'BIKE' || driverLocationLogKey.getEventTimestamp().getSeconds() == 186178` (multiple conditions example 2)

***Message based filter expressions examples:***

* `driverLocationLogMessage.getGcmKey() == 'LefFCyvIVkJVgOL6d4uKBlMxlpyus1'`
* `driverLocationLogMessage.getDriverId() == 'COJRXpCPIYrIASdJ4W8gbqzeTt1PGl' && driverLocationLogMessage.getDriverLocation().getLatitude() > 0.6487193703651428`
* `driverLocationLogMessage.getDriverLocation().getAltitudeInMeters > 0.9949166178703308`

**Note: Use `SINK=log` for testing the applied filtering** 
