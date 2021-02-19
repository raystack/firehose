##FILTER EXPRESSIONS
#### Introduction

Filter expressions are allowed to filter messages just after reading from Kafka and before sending to Sink.

##### Rules to write expressions:

* All the expressions are like a piece of Java code.
* Follow rules for every data type, as like writing a Java code.
* Access nested fields by `.` and `()`, i.e., `sampleLogMessage.getVehicleType()`

**Example**

Sample proto message:

```
===================KEY==========================
driver_id: "abcde12345"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE

================= MESSAGE=======================
driver_id: "abcde12345"
vehicle_type: BIKE
event_timestamp {
  seconds: 186178
  nanos: 323080
}
driver_status: UNAVAILABLE
app_version: "1.0.0"
driver_location {
  latitude: 0.6487193703651428
  longitude: 0.791822075843811
  altitude_in_meters: 0.9949166178703308
  accuracy_in_meters: 0.39277541637420654
  speed_in_meters_per_second: 0.28804516792297363
}
gcm_key: "abc123"
```

***Key based filter expressions examples:***

* `sampleLogKey.getDriverId()=="abcde12345"`
* `sampleLogKey.getVehicleType()=="BIKE"`
* `sampleLogKey.getEventTimestamp().getSeconds()==186178`
* `sampleLogKey.getDriverId()=="abcde12345"&&sampleLogKey.getVehicleType=="BIKE"` (multiple conditions example 1)
* `sampleLogKey.getVehicleType()=="BIKE"||sampleLogKey.getEventTimestamp().getSeconds()==186178` (multiple conditions example 2)

***Message based filter expressions examples:***

* `sampleLogMessage.getGcmKey()=="abc123"`
* `sampleLogMessage.getDriverId()=="abcde12345"&&sampleLogMessage.getDriverLocation().getLatitude()>0.6487193703651428`
* `sampleLogMessage.getDriverLocation().getAltitudeInMeters>0.9949166178703308`

**Note: Use `sink.type=log` for testing the applied filtering** 



##CONTRIBUTION GUIDELINES
####BECOME A COMMITOR & CONTRIBUTE

We are always interested in adding new contributors. What we look for is a series of contributions, good taste, and an ongoing interest in the project.

* Committers will have write access to the Firehose repositories.

* There is no strict protocol for becoming a committer or PMC member. Candidates for new committers are typically people that are active contributors and community members.

* Candidates for new committers can also be suggested by current committers or PMC members.

* If you would like to become a committer, you should start contributing to Firehose in any of the ways mentioned. You might also want to talk to other committers and ask for their advice and guidance.


####WHAT CAN YOU DO?
* You can report a bug or suggest a feature enhancement on our [Jira Board](link to create a new issue). If you have a question or are simply not sure if it is really an issue or not, please [contact us](E-mail) first before you create a new JIRA ticket.

* You can modify the code
    * Add any new feature
    * Add a new Sink
    * Improve Health and Monitoring Metrics
    * Improve Logging

* You can help with documenting new features or improve existing documentation.

* You can also review and accept other contributions if you are a Commitor.


####CONTRIBUTING GUIDELINES
Please follow these practices for you change to get merged fast and smoothly:

* Contributions can only be accepted if they contain appropriate testing (Unit and Integration Tests).

* If you are introducing a completely new feature or making any major changes in an existing one, we recommend to start with an RFC and get consensus on the basic design first.

* Make sure your local build is running with all the tests and checkstyle passing.

* If your change is related to user-facing protocols / configurations, you need to make the corresponding change in the documentation as well.
    * You may need to contact us to get the permission first if it is your first time to edit the documentation.
    * Docs live in the code repo under `docs` so that changes to that can be done in the same PR as changes to the code.
    
## Firehose VS Kafka-Connect
#### Kafka-Connect
PROS
* Can consume from Kafka as well as many other sources
* Easier to integrate if working with confluent-platform
* Provides a bunch of transformation options as part of Single Message Transformations(SMTs)

CONS
* When using in distributed mode across multiple nodes, need to install all the connectors across all the workers within your kafka connect cluster
* Available connectors may have some limitations. Its usually rare to find all the required features in a single connector and so is to find a documentation for the same
* Separation of commercial and open-source features is very poor
* For monitoring and many other features confluent control center asks for an enterprise subscription
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#### Firehose
PROS
* Easier to install and use different sinks by tweaking only a couple of configurations
* Comes with tons of exclusive features for each sink. e.g. JSON body template, Parameterised header/request, OAuth2 for HTTP sink
* Value based filtering is much easier to implement as compared to Kafka-Connect. Requires no additional plugins/schema-registry to be installed
* Provides a comprehensible Abstract sink contract which makes it Easier to add a new sink in Firehose
* Don't need to think about converters and serializers, Firehose comes with an inbuilt serialization/deserialization
* Comes with Firehose health dashboard (Grafana) for effortless monitoring free of cost

CONS
* Can consume from Kafka but not from any other data source
* Supports only protobuf format as of now
* Doesn't support Kafka Sink yet