"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[520],{3905:function(e,t,o){o.d(t,{Zo:function(){return h},kt:function(){return u}});var a=o(7294);function r(e,t,o){return t in e?Object.defineProperty(e,t,{value:o,enumerable:!0,configurable:!0,writable:!0}):e[t]=o,e}function n(e,t){var o=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),o.push.apply(o,a)}return o}function i(e){for(var t=1;t<arguments.length;t++){var o=null!=arguments[t]?arguments[t]:{};t%2?n(Object(o),!0).forEach((function(t){r(e,t,o[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(o)):n(Object(o)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(o,t))}))}return e}function s(e,t){if(null==e)return{};var o,a,r=function(e,t){if(null==e)return{};var o,a,r={},n=Object.keys(e);for(a=0;a<n.length;a++)o=n[a],t.indexOf(o)>=0||(r[o]=e[o]);return r}(e,t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(a=0;a<n.length;a++)o=n[a],t.indexOf(o)>=0||Object.prototype.propertyIsEnumerable.call(e,o)&&(r[o]=e[o])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),o=t;return e&&(o="function"==typeof e?e(t):i(i({},t),e)),o},h=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},p=a.forwardRef((function(e,t){var o=e.components,r=e.mdxType,n=e.originalType,l=e.parentName,h=s(e,["components","mdxType","originalType","parentName"]),p=c(o),u=r,f=p["".concat(l,".").concat(u)]||p[u]||d[u]||n;return o?a.createElement(f,i(i({ref:t},h),{},{components:o})):a.createElement(f,i({ref:t},h))}));function u(e,t){var o=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var n=o.length,i=new Array(n);i[0]=p;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var c=2;c<n;c++)i[c]=o[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,o)}p.displayName="MDXCreateElement"},979:function(e,t,o){o.r(t),o.d(t,{assets:function(){return h},contentTitle:function(){return l},default:function(){return u},frontMatter:function(){return s},metadata:function(){return c},toc:function(){return d}});var a=o(7462),r=o(3366),n=(o(7294),o(3905)),i=["components"],s={},l="FAQs",c={unversionedId:"reference/core-faqs",id:"reference/core-faqs",title:"FAQs",description:"What problems does Firehose solve?",source:"@site/docs/reference/core-faqs.md",sourceDirName:"reference",slug:"/reference/core-faqs",permalink:"/firehose/reference/core-faqs",draft:!1,editUrl:"https://github.com/raystack/firehose/edit/master/docs/docs/reference/core-faqs.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Metrics",permalink:"/firehose/reference/metrics"},next:{title:"Frequently Asked Questions",permalink:"/firehose/reference/faq"}},h={},d=[{value:"What problems does Firehose solve?",id:"what-problems-does-firehose-solve",level:2},{value:"Which Java versions does Firehose work with?",id:"which-java-versions-does-firehose-work-with",level:2},{value:"How does the execution work?",id:"how-does-the-execution-work",level:2},{value:"Can I do any transformations(for example filter) before sending the data to sink?",id:"can-i-do-any-transformationsfor-example-filter-before-sending-the-data-to-sink",level:2},{value:"How to optimize parallelism based on input rate of Kafka messages?",id:"how-to-optimize-parallelism-based-on-input-rate-of-kafka-messages",level:2},{value:"What is the retry mechanism in Firehose? What kind of retry strategies are supported ?",id:"what-is-the-retry-mechanism-in-firehose-what-kind-of-retry-strategies-are-supported-",level:2},{value:"Which Kafka Client configs are available ?",id:"which-kafka-client-configs-are-available-",level:2},{value:"What all data formats are supported ?",id:"what-all-data-formats-are-supported-",level:2},{value:"Is there any code snippet which shows how i can produce sample message in supported data format ?",id:"is-there-any-code-snippet-which-shows-how-i-can-produce-sample-message-in-supported-data-format-",level:2},{value:"Can we select particular fields from the incoming message ?",id:"can-we-select-particular-fields-from-the-incoming-message-",level:2},{value:"How can I handle consumer lag ?",id:"how-can-i-handle-consumer-lag-",level:2},{value:"What is Stencil in context of Firehose ?",id:"what-is-stencil-in-context-of-firehose-",level:2},{value:"How do I configure Protobuf needed to consume ?",id:"how-do-i-configure-protobuf-needed-to-consume-",level:2},{value:"Can we select particular fields from the input message ?",id:"can-we-select-particular-fields-from-the-input-message-",level:2},{value:"Why Protobuf ? Can it support other formats like JSON and Avro ?",id:"why-protobuf--can-it-support-other-formats-like-json-and-avro-",level:2},{value:"Will I have any data loss if my Firehose fails ?",id:"will-i-have-any-data-loss-if-my-firehose-fails-",level:2},{value:"How does Firehose handle failed messages ?",id:"how-does-firehose-handle-failed-messages-",level:2},{value:"How does commits for Kafka consumer works ?",id:"how-does-commits-for-kafka-consumer-works-",level:2},{value:"What all metrics are available to monitor the Kafka consumer?",id:"what-all-metrics-are-available-to-monitor-the-kafka-consumer",level:2},{value:"What happens if my Firehose gets restarted?",id:"what-happens-if-my-firehose-gets-restarted",level:2},{value:"How to configure the filter for a proto field based on some data?",id:"how-to-configure-the-filter-for-a-proto-field-based-on-some-data",level:2},{value:"Can I perform basic arithmetic operations in filters?",id:"can-i-perform-basic-arithmetic-operations-in-filters",level:2},{value:"Does log sink work for any complex data type e.g. array?",id:"does-log-sink-work-for-any-complex-data-type-eg-array",level:2},{value:"What are the use-cases of log sink?",id:"what-are-the-use-cases-of-log-sink",level:2}],p={toc:d};function u(e){var t=e.components,o=(0,r.Z)(e,i);return(0,n.kt)("wrapper",(0,a.Z)({},p,o,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"faqs"},"FAQs"),(0,n.kt)("h2",{id:"what-problems-does-firehose-solve"},"What problems does Firehose solve?"),(0,n.kt)("p",null,"Every micro-service needs its own sink to be developed for such common operations as streaming data from Kafka to data lakes or other endpoints, along with real-time filtering, parsing, and monitoring of the sink."),(0,n.kt)("p",null,"With Firehose, you don't need to write sink code for every such microservice, or manage resources to sink data from Kafka server to your database/service endpoint. Having provided all the configuration parameters of the sink, Firehose will create, manage and monitor one for you. It also automatically scales to match the throughput of your data and requires no ongoing administration."),(0,n.kt)("h2",{id:"which-java-versions-does-firehose-work-with"},"Which Java versions does Firehose work with?"),(0,n.kt)("p",null,"Firehose has been built and tested to work with Java SE Development Kit 1.8."),(0,n.kt)("h2",{id:"how-does-the-execution-work"},"How does the execution work?"),(0,n.kt)("p",null,"Firehose has the capability to run parallelly on threads. Each thread does the following:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Get messages from Kafka"),(0,n.kt)("li",{parentName:"ul"},"Filter the messages ","(","optional",")"),(0,n.kt)("li",{parentName:"ul"},"Push these messages to sink"),(0,n.kt)("li",{parentName:"ul"},"All the existing sink types follow the same contract/lifecycle defined in ",(0,n.kt)("inlineCode",{parentName:"li"},"AbstractSink.java"),". It consists of two stages:",(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},"Prepare: Transformation over-filtered messages\u2019 list to prepare the sink-specific insert/update client requests."),(0,n.kt)("li",{parentName:"ul"},"Execute: Requests created in the Prepare stage are executed at this step and a list of failed messages is returned ","(","if any",")"," for retry."))),(0,n.kt)("li",{parentName:"ul"},"In case push fails and DLQ is:",(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},"enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic"),(0,n.kt)("li",{parentName:"ul"},"disabled: Firehose keeps on retrying until it receives a success code"))),(0,n.kt)("li",{parentName:"ul"},"Captures telemetry and success/failure events and send them to Telegraf"),(0,n.kt)("li",{parentName:"ul"},"Repeat the process")),(0,n.kt)("h2",{id:"can-i-do-any-transformationsfor-example-filter-before-sending-the-data-to-sink"},"Can I do any transformations","(","for example filter",")"," before sending the data to sink?"),(0,n.kt)("p",null,"Yes, Firehose provides JEXL based filters based on the fields in key or message of the Kafka record. Read the ",(0,n.kt)("a",{parentName:"p",href:"/firehose/concepts/filters"},"Filters")," section for further details."),(0,n.kt)("h2",{id:"how-to-optimize-parallelism-based-on-input-rate-of-kafka-messages"},"How to optimize parallelism based on input rate of Kafka messages?"),(0,n.kt)("p",null,"You can increase the workers in the Firehose which will effectively multiply the number of records being processed by Firehose. However, please be mindful of the fact that your sink also needs to be able to process this higher volume of data being pushed to it. Because if it is not, then this will only compound the problem of increasing lag."),(0,n.kt)("p",null,"Adding some sort of a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink."),(0,n.kt)("h2",{id:"what-is-the-retry-mechanism-in-firehose-what-kind-of-retry-strategies-are-supported-"},"What is the retry mechanism in Firehose? What kind of retry strategies are supported ?"),(0,n.kt)("p",null,"In case push fails and DLQ ","(","Dead Letter Queue",")"," is:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic"),(0,n.kt)("li",{parentName:"ul"},"disabled: Firehose keeps on retrying until it receives a success code")),(0,n.kt)("h2",{id:"which-kafka-client-configs-are-available-"},"Which Kafka Client configs are available ?"),(0,n.kt)("p",null,"Firehose provides various Kafka client configurations. Refer ",(0,n.kt)("a",{parentName:"p",href:"../advance/generic"},"Generic Configurations")," section for details on configuration related to Kafka Consumer."),(0,n.kt)("h2",{id:"what-all-data-formats-are-supported-"},"What all data formats are supported ?"),(0,n.kt)("p",null,"Elasticsearch , Bigquery and MongoDB sink support both JSON and Protobuf as the input schema. For other sinks, we currently support only Protobuf. Support for JSON and Avro is planned and incorporated in our roadmap. Please refer to our Roadmap section for more details."),(0,n.kt)("p",null,"Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a Protobuf schema."),(0,n.kt)("p",null,"Follow the instructions in ",(0,n.kt)("a",{parentName:"p",href:"https://developers.google.com/protocol-buffers/docs/javatutorial"},"this article")," on how to create, compile and serialize a Protobuf object to send it to a binary OutputStream. Refer ",(0,n.kt)("a",{parentName:"p",href:"https://developers.google.com/protocol-buffers/docs/proto3"},"this guide")," for detailed Protobuf syntax and rules to create a ",(0,n.kt)("inlineCode",{parentName:"p"},".proto")," file"),(0,n.kt)("h2",{id:"is-there-any-code-snippet-which-shows-how-i-can-produce-sample-message-in-supported-data-format-"},"Is there any code snippet which shows how i can produce sample message in supported data format ?"),(0,n.kt)("p",null,"Following is an example to demonstrate how to create a Protobuf message and then produce it to a Kafka cluster. Firstly, create a ",(0,n.kt)("inlineCode",{parentName:"p"},".proto")," file containing all the required field names and their corresponding integer tags. Save it in a new file named ",(0,n.kt)("inlineCode",{parentName:"p"},"person.proto")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-text"},'syntax = "proto2";\n\npackage tutorial;\n\noption java_multiple_files = true;\noption java_package = "com.example.tutorial.protos";\noption java_outer_classname = "PersonProtos";\n\nmessage Person {\n  optional string name = 1;\n  optional int32 id = 2;\n  optional string email = 3;\n\n  enum PhoneType {\n    MOBILE = 0;\n    HOME = 1;\n    WORK = 2;\n  }\n\n  message PhoneNumber {\n    optional string number = 1;\n    optional PhoneType type = 2 [default = HOME];\n  }\n\n  repeated PhoneNumber phones = 4;\n}\n')),(0,n.kt)("p",null,"Next, compile your ",(0,n.kt)("inlineCode",{parentName:"p"},".proto")," file using Protobuf compiler i.e. ",(0,n.kt)("inlineCode",{parentName:"p"},"protoc"),".This will generate Person ,PersonOrBuilder and PersonProtos Java source files. Specify the source directory ","(","where your application's source code lives \u2013 the current directory is used if you don't provide a value",")",", the destination directory ","(","where you want the generated code to go; often the same as ",(0,n.kt)("inlineCode",{parentName:"p"},"$SRC_DIR"),")",", and the path to your ",(0,n.kt)("inlineCode",{parentName:"p"},".proto")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-text"},"protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/person.proto\n")),(0,n.kt)("p",null,"Lastly, add the following lines in your Java code to generate a POJO ","(","Plain Old Java Object",")"," of the Person proto class and serialize it to a byte array, using the ",(0,n.kt)("inlineCode",{parentName:"p"},"toByteArray()")," method of the ",(0,n.kt)("a",{parentName:"p",href:"https://www.javadoc.io/static/com.google.protobuf/protobuf-java/3.5.1/com/google/protobuf/GeneratedMessageV3.html"},"com.google.protobuf.GeneratedMessageV3 ")," class. The byte array is then sent to the Kafka cluster by the producer."),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-java"},'KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);\n\nPerson john = Person.newBuilder()\n                .setId(87182872)\n                .setName("John Doe")\n                .setEmail("jdoe@example.com")\n                .addPhones(\n                        Person.PhoneNumber.newBuilder()\n                                .setNumber("555-4321")\n                                .setType(Person.PhoneType.HOME))\n                .build();\n\nproducer.send(new ProducerRecord<byte[], byte[]>(topicName, john.toByteArray()));\n')),(0,n.kt)("p",null,"Refer ",(0,n.kt)("a",{parentName:"p",href:"https://developers.google.com/protocol-buffers"},"https://developers.google.com/protocol-buffers")," for more info on how to create protobufs."),(0,n.kt)("h2",{id:"can-we-select-particular-fields-from-the-incoming-message-"},"Can we select particular fields from the incoming message ?"),(0,n.kt)("p",null,"Firehose will send all the fields of the incoming messages to the specified sink. But you can configure your sink destination/ database to consume only the required fields."),(0,n.kt)("h2",{id:"how-can-i-handle-consumer-lag-"},"How can I handle consumer lag ?"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"When it comes to decreasing the topic lag, it often helps to have the environment variable - ",(0,n.kt)("a",{parentName:"li",href:"/firehose/advance/generic#source_kafka_consumer_config_max_poll_records"},(0,n.kt)("inlineCode",{parentName:"a"},"SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS"))," to be increased from the default of 500 to something higher which will tell the Kafka Consumer to consume more messages in a single poll."),(0,n.kt)("li",{parentName:"ul"},"Additionally, you can increase the workers in the Firehose which will effectively multiply the number of records being processed by Firehose."),(0,n.kt)("li",{parentName:"ul"},"Alternatively, if your underlying sink is not able to handle increased ","(","or default",")"," volume of data being pushed to it, adding some sort of a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink.")),(0,n.kt)("h2",{id:"what-is-stencil-in-context-of-firehose-"},"What is Stencil in context of Firehose ?"),(0,n.kt)("p",null,"Stencil API is a dynamic schema registry for hosting and managing versions of Protobuf descriptors. The schema handling i.e., find the mapped schema for the topic, downloading the descriptors, and dynamically being notified of/updating with the latest schema is abstracted through the Stencil library."),(0,n.kt)("p",null,"The Stencil Client is a proprietary library that provides an abstraction layer, for schema handling. Schema Caching, dynamic schema updates are features of the stencil client library."),(0,n.kt)("p",null,"Refer ",(0,n.kt)("a",{parentName:"p",href:"https://raystack.gitbook.io/stencil/"},"this article")," for further information of the features, configuration and deployment instructions of the Stencil API. Source code of Stencil Server and Client API can be found in its ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/raystack/stencil"},"Github repository"),"."),(0,n.kt)("h2",{id:"how-do-i-configure-protobuf-needed-to-consume-"},"How do I configure Protobuf needed to consume ?"),(0,n.kt)("p",null,"Generated Protobuf Descriptors are hosted behind an Stencil server artifactory/HTTP endpoint. This endpoint URL and the ProtoDescriptor class that the Firehose deployment should use to deserialize raw data with is configured in Firehose in the environment variables",(0,n.kt)("inlineCode",{parentName:"p"},"SCHEMA_REGISTRY_STENCIL_URLS"),"and",(0,n.kt)("inlineCode",{parentName:"p"},"INPUT_SCHEMA_PROTO_CLASS")," respectively ."),(0,n.kt)("p",null,"The Proto Descriptor Set of the Kafka messages must be uploaded to the Stencil server. Refer ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/raystack/stencil/tree/master/server#readme"},"this guide")," on how to setup and configure the Stencil server."),(0,n.kt)("h2",{id:"can-we-select-particular-fields-from-the-input-message-"},"Can we select particular fields from the input message ?"),(0,n.kt)("p",null,"No, all fields from the input key/message will be sent by Firehose to the Sink. But you can configure your service endpoint or database to consume only those fields which are required."),(0,n.kt)("h2",{id:"why-protobuf--can-it-support-other-formats-like-json-and-avro-"},"Why Protobuf ? Can it support other formats like JSON and Avro ?"),(0,n.kt)("p",null,"Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a Protobuf schema. Protobuf is much more lightweight that other schema formats like JSON, since it encodes the keys in the message to integers."),(0,n.kt)("p",null,"Elasticsearch, Bigquery and MongoDB sink support both JSON and Protobuf as the input schema."),(0,n.kt)("p",null,"For other sinks, we currently support only Protobuf. Support for JSON and Avro is planned and incorporated in our roadmap. Please refer to our Roadmap section for more details."),(0,n.kt)("h2",{id:"will-i-have-any-data-loss-if-my-firehose-fails-"},"Will I have any data loss if my Firehose fails ?"),(0,n.kt)("p",null,"After a batch of messages is sent successfully, Firehose commits the offset before the consumer polls another batch from Kafka. Thus, failed messages are not committed."),(0,n.kt)("p",null,"So, when Firehose is restarted, the Kafka Consumer automatically starts pulling messages from the last committed offset of the consumer group. So, no data loss occurs when an instance of Firehose fails."),(0,n.kt)("h2",{id:"how-does-firehose-handle-failed-messages-"},"How does Firehose handle failed messages ?"),(0,n.kt)("p",null,"In case push fails and DLQ ","(","Dead Letter Queue",")"," is:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic"),(0,n.kt)("li",{parentName:"ul"},"disabled: Firehose keeps on retrying until it receives a success code")),(0,n.kt)("h2",{id:"how-does-commits-for-kafka-consumer-works-"},"How does commits for Kafka consumer works ?"),(0,n.kt)("p",null,"After the messages are pulled successfully, Firehose commits the offset to the Kafka cluster."),(0,n.kt)("p",null,"If",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_ASYNC_COMMIT_ENABLE")," is set to ",(0,n.kt)("inlineCode",{parentName:"p"},"true"),"then the KafkaConsumer commits the offset asynchronously and logs to the metric ",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL")," incrementing the counter ",(0,n.kt)("inlineCode",{parentName:"p"},"FAILURE_TAG")," or ",(0,n.kt)("inlineCode",{parentName:"p"},"SUCCESS_TAG")," depending on whether the commit was a success / failure."),(0,n.kt)("p",null,"If",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_ASYNC_COMMIT_ENABLE")," is set to ",(0,n.kt)("inlineCode",{parentName:"p"},"false"),"then the KafkaConsumer commits the offset synchronously and execution is blocked until the commit either succeeds or throws an exception."),(0,n.kt)("h2",{id:"what-all-metrics-are-available-to-monitor-the-kafka-consumer"},"What all metrics are available to monitor the Kafka consumer?"),(0,n.kt)("p",null,"Firehose exposes critical metrics to monitor the health of your delivery streams and take any necessary actions. Refer the ",(0,n.kt)("a",{parentName:"p",href:"/firehose/reference/metrics"},"Metrics")," section for further details on each metric."),(0,n.kt)("h2",{id:"what-happens-if-my-firehose-gets-restarted"},"What happens if my Firehose gets restarted?"),(0,n.kt)("p",null,"When Firehose is restarted, the Kafka Consumer automatically starts pulling messages from the last committed offset of the consumer group specified by the variable ",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_CONSUMER_GROUP_ID")),(0,n.kt)("h2",{id:"how-to-configure-the-filter-for-a-proto-field-based-on-some-data"},"How to configure the filter for a proto field based on some data?"),(0,n.kt)("p",null,"The environment variables ",(0,n.kt)("inlineCode",{parentName:"p"},"FILTER_DATA_SOURCE")," , ",(0,n.kt)("inlineCode",{parentName:"p"},"FILTER_JEXL_EXPRESSION")," and ",(0,n.kt)("inlineCode",{parentName:"p"},"FILTER_SCHEMA_PROTO_CLASS")," need to be set for filters to work. The required filters need to be written in JEXL expression format. Refer ",(0,n.kt)("a",{parentName:"p",href:"/firehose/guides/json-based-filters"},"Using Filters")," section for more details."),(0,n.kt)("h2",{id:"can-i-perform-basic-arithmetic-operations-in-filters"},"Can I perform basic arithmetic operations in filters?"),(0,n.kt)("p",null,"Yes, you can combine multiple fields of the key/message protobuf in a single JEXL expression and perform any arithmetic or logical operations between them. e.g - ",(0,n.kt)("inlineCode",{parentName:"p"},"sampleKey.getTime().getSeconds() * 1000 + sampleKey.getTime().getMillis() > 22809")),(0,n.kt)("h2",{id:"does-log-sink-work-for-any-complex-data-type-eg-array"},"Does log sink work for any complex data type e.g. array?"),(0,n.kt)("p",null,"Log Sink uses Logback and SL4J lobrary for logging to standard output. Thus, it'll be able to log any complex data type by printing the String returned by the toString","(",")"," method of the object. Log sink will also work for arrays and be able to print all the array elements in comma-separated format, e.g. ",(0,n.kt)("inlineCode",{parentName:"p"},"[4, 3, 8]")),(0,n.kt)("h2",{id:"what-are-the-use-cases-of-log-sink"},"What are the use-cases of log sink?"),(0,n.kt)("p",null,"Firehose provides a log sink to make it easy to consume messages in ",(0,n.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Standard_streams#Standard_output_%28stdout%29"},"standard output"),". Log sink can be used for debugging purposes and experimenting with various filters. It can also be used to test the latency and overall performance of the Firehose."))}u.isMDXComponent=!0}}]);