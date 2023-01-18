"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[955],{3905:function(e,t,l){l.d(t,{Zo:function(){return p},kt:function(){return c}});var a=l(7294);function n(e,t,l){return t in e?Object.defineProperty(e,t,{value:l,enumerable:!0,configurable:!0,writable:!0}):e[t]=l,e}function i(e,t){var l=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),l.push.apply(l,a)}return l}function r(e){for(var t=1;t<arguments.length;t++){var l=null!=arguments[t]?arguments[t]:{};t%2?i(Object(l),!0).forEach((function(t){n(e,t,l[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(l)):i(Object(l)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(l,t))}))}return e}function o(e,t){if(null==e)return{};var l,a,n=function(e,t){if(null==e)return{};var l,a,n={},i=Object.keys(e);for(a=0;a<i.length;a++)l=i[a],t.indexOf(l)>=0||(n[l]=e[l]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)l=i[a],t.indexOf(l)>=0||Object.prototype.propertyIsEnumerable.call(e,l)&&(n[l]=e[l])}return n}var u=a.createContext({}),_=function(e){var t=a.useContext(u),l=t;return e&&(l="function"==typeof e?e(t):r(r({},t),e)),l},p=function(e){var t=_(e.components);return a.createElement(u.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},k=a.forwardRef((function(e,t){var l=e.components,n=e.mdxType,i=e.originalType,u=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),k=_(l),c=n,s=k["".concat(u,".").concat(c)]||k[c]||m[c]||i;return l?a.createElement(s,r(r({ref:t},p),{},{components:l})):a.createElement(s,r({ref:t},p))}));function c(e,t){var l=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=l.length,r=new Array(i);r[0]=k;var o={};for(var u in t)hasOwnProperty.call(t,u)&&(o[u]=t[u]);o.originalType=e,o.mdxType="string"==typeof e?e:n,r[1]=o;for(var _=2;_<i;_++)r[_]=l[_];return a.createElement.apply(null,r)}return a.createElement.apply(null,l)}k.displayName="MDXCreateElement"},3756:function(e,t,l){l.r(t),l.d(t,{assets:function(){return p},contentTitle:function(){return u},default:function(){return c},frontMatter:function(){return o},metadata:function(){return _},toc:function(){return m}});var a=l(7462),n=l(3366),i=(l(7294),l(3905)),r=["components"],o={},u="Generic",_={unversionedId:"advance/generic",id:"advance/generic",title:"Generic",description:"All sinks in Firehose requires the following variables to be set",source:"@site/docs/advance/generic.md",sourceDirName:"advance",slug:"/advance/generic",permalink:"/firehose/advance/generic",draft:!1,editUrl:"https://github.com/odpf/firehose/edit/master/docs/docs/advance/generic.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Monitoring",permalink:"/firehose/concepts/monitoring"},next:{title:"Errors",permalink:"/firehose/advance/errors"}},p={},m=[{value:"<code>INPUT_SCHEMA_DATA_TYPE</code>",id:"input_schema_data_type",level:3},{value:"<code>KAFKA_RECORD_PARSER_MODE</code>",id:"kafka_record_parser_mode",level:3},{value:"<code>SINK_TYPE</code>",id:"sink_type",level:3},{value:"<code>INPUT_SCHEMA_PROTO_CLASS</code>",id:"input_schema_proto_class",level:3},{value:"<code>INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING</code>",id:"input_schema_proto_to_column_mapping",level:3},{value:"<code>METRIC_STATSD_HOST</code>",id:"metric_statsd_host",level:3},{value:"\0<code>METRIC_STATSD_PORT</code>",id:"metric_statsd_port",level:3},{value:"<code>METRIC_STATSD_TAGS</code>",id:"metric_statsd_tags",level:3},{value:"<code>APPLICATION_THREAD_CLEANUP_DELAY</code>",id:"application_thread_cleanup_delay",level:3},{value:"<code>APPLICATION_THREAD_COUNT</code>",id:"application_thread_count",level:3},{value:"<code>TRACE_JAEGAR_ENABLE</code>",id:"trace_jaegar_enable",level:3},{value:"<code>LOG_LEVEL</code>",id:"log_level",level:3},{value:"<code>INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE</code>",id:"input_schema_proto_allow_unknown_fields_enable",level:3},{value:"Kafka Consumer",id:"kafka-consumer",level:2},{value:"<code>SOURCE_KAFKA_BROKERS</code>",id:"source_kafka_brokers",level:3},{value:"<code>SOURCE_KAFKA_TOPIC</code>",id:"source_kafka_topic",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS</code>",id:"source_kafka_consumer_config_max_poll_records",level:3},{value:"<code>SOURCE_KAFKA_ASYNC_COMMIT_ENABLE</code>",id:"source_kafka_async_commit_enable",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS</code>",id:"source_kafka_consumer_config_session_timeout_ms",level:3},{value:"<code>SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE</code>",id:"source_kafka_commit_only_current_partitions_enable",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE</code>",id:"source_kafka_consumer_config_auto_commit_enable",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_GROUP_ID</code>",id:"source_kafka_consumer_group_id",level:3},{value:"<code>SOURCE_KAFKA_POLL_TIMEOUT_MS</code>",id:"source_kafka_poll_timeout_ms",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS</code>",id:"source_kafka_consumer_config_metadata_max_age_ms",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_MODE</code>",id:"source_kafka_consumer_mode",level:3},{value:"<code>SOURCE_KAFKA_CONSUMER_CONFIG_PARTITION_ASSIGNMENT_STRATEGY</code>",id:"source_kafka_consumer_config_partition_assignment_strategy",level:3},{value:"Stencil Client",id:"stencil-client",level:2},{value:"<code>SCHEMA_REGISTRY_STENCIL_ENABLE</code>",id:"schema_registry_stencil_enable",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_URLS</code>",id:"schema_registry_stencil_urls",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS</code>",id:"schema_registry_stencil_fetch_timeout_ms",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES</code>",id:"schema_registry_stencil_fetch_retries",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS</code>",id:"schema_registry_stencil_fetch_backoff_min_ms",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN</code>",id:"schema_registry_stencil_fetch_auth_bearer_token",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH</code>",id:"schema_registry_stencil_cache_auto_refresh",level:3},{value:"<code>SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS</code>",id:"schema_registry_stencil_cache_ttl_ms",level:3}],k={toc:m};function c(e){var t=e.components,l=(0,n.Z)(e,r);return(0,i.kt)("wrapper",(0,a.Z)({},k,l,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"generic"},"Generic"),(0,i.kt)("p",null,"All sinks in Firehose requires the following variables to be set"),(0,i.kt)("h3",{id:"input_schema_data_type"},(0,i.kt)("inlineCode",{parentName:"h3"},"INPUT_SCHEMA_DATA_TYPE")),(0,i.kt)("p",null,"Defines the input message schema."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"json")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"protobuf"))),(0,i.kt)("h3",{id:"kafka_record_parser_mode"},(0,i.kt)("inlineCode",{parentName:"h3"},"KAFKA_RECORD_PARSER_MODE")),(0,i.kt)("p",null,"Decides whether to parse key or message ","(","as per your input proto",")"," from incoming data."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"message")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required")),(0,i.kt)("li",{parentName:"ul"},"Default value",(0,i.kt)("inlineCode",{parentName:"li"},": message"))),(0,i.kt)("h3",{id:"sink_type"},(0,i.kt)("inlineCode",{parentName:"h3"},"SINK_TYPE")),(0,i.kt)("p",null,"Defines the Firehose sink type."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"log")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required"))),(0,i.kt)("h3",{id:"input_schema_proto_class"},(0,i.kt)("inlineCode",{parentName:"h3"},"INPUT_SCHEMA_PROTO_CLASS")),(0,i.kt)("p",null,"Defines the fully qualified name of the input proto class."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"com.tests.TestMessage")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required"))),(0,i.kt)("h3",{id:"input_schema_proto_to_column_mapping"},(0,i.kt)("inlineCode",{parentName:"h3"},"INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING")),(0,i.kt)("p",null,"Defines the mapping of the Proto fields to header/query fields in JSON format."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},'{"1":"order_number","2":"event_timestamp","3":"driver_id"}')),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional"))),(0,i.kt)("h3",{id:"metric_statsd_host"},(0,i.kt)("inlineCode",{parentName:"h3"},"METRIC_STATSD_HOST")),(0,i.kt)("p",null,"URL of the StatsD host ","(","Telegraf service",")"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"localhost")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value",(0,i.kt)("inlineCode",{parentName:"li"},": localhost"))),(0,i.kt)("h3",{id:"metric_statsd_port"},"\0",(0,i.kt)("inlineCode",{parentName:"h3"},"METRIC_STATSD_PORT")),(0,i.kt)("p",null,"Port of the StatsD host ","(","Telegraf service",")"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"8125")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value",(0,i.kt)("inlineCode",{parentName:"li"},": 8125"))),(0,i.kt)("h3",{id:"metric_statsd_tags"},(0,i.kt)("inlineCode",{parentName:"h3"},"METRIC_STATSD_TAGS")),(0,i.kt)("p",null,"Global tags for StatsD metrics. Tags must be comma-separated."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"team=data-engineering,app=firehose")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional"))),(0,i.kt)("h3",{id:"application_thread_cleanup_delay"},(0,i.kt)("inlineCode",{parentName:"h3"},"APPLICATION_THREAD_CLEANUP_DELAY")),(0,i.kt)("p",null,"Defines the time duration in milliseconds after which to cleanup the thread."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"400")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"2000"))),(0,i.kt)("h3",{id:"application_thread_count"},(0,i.kt)("inlineCode",{parentName:"h3"},"APPLICATION_THREAD_COUNT")),(0,i.kt)("p",null,"Number of parallel threads to run for Firehose."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"2")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"1"))),(0,i.kt)("h3",{id:"trace_jaegar_enable"},(0,i.kt)("inlineCode",{parentName:"h3"},"TRACE_JAEGAR_ENABLE")),(0,i.kt)("p",null,"Defines whether to enable Jaegar tracing or not"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"false"))),(0,i.kt)("h3",{id:"log_level"},(0,i.kt)("inlineCode",{parentName:"h3"},"LOG_LEVEL")),(0,i.kt)("p",null,"Defines the log level , i.e. debug/info/error."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"debug")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"info"))),(0,i.kt)("h3",{id:"input_schema_proto_allow_unknown_fields_enable"},(0,i.kt)("inlineCode",{parentName:"h3"},"INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE")),(0,i.kt)("p",null,"Proto can have unknown fields as input"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true"))),(0,i.kt)("h2",{id:"kafka-consumer"},"Kafka Consumer"),(0,i.kt)("h3",{id:"source_kafka_brokers"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_BROKERS")),(0,i.kt)("p",null,"Defines the bootstrap server of Kafka brokers to consume from."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"localhost:9092")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required"))),(0,i.kt)("h3",{id:"source_kafka_topic"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_TOPIC")),(0,i.kt)("p",null,"Defines the list of Kafka topics to consume from."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"test-topic")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required"))),(0,i.kt)("h3",{id:"source_kafka_consumer_config_max_poll_records"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS")),(0,i.kt)("p",null,"Defines the batch size of Kafka messages"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"705")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"500"))),(0,i.kt)("h3",{id:"source_kafka_async_commit_enable"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_ASYNC_COMMIT_ENABLE")),(0,i.kt)("p",null,"Defines whether to enable async commit for Kafka consumer"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"false")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true"))),(0,i.kt)("h3",{id:"source_kafka_consumer_config_session_timeout_ms"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS")),(0,i.kt)("p",null,"Defines the duration of session timeout in milliseconds"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"700")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"10000"))),(0,i.kt)("h3",{id:"source_kafka_commit_only_current_partitions_enable"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE")),(0,i.kt)("p",null,"Defines whether to commit only current partitions"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"false")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true"))),(0,i.kt)("h3",{id:"source_kafka_consumer_config_auto_commit_enable"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE")),(0,i.kt)("p",null,"Defines whether to enable auto commit for Kafka consumer"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"705")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"500"))),(0,i.kt)("h3",{id:"source_kafka_consumer_group_id"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_GROUP_ID")),(0,i.kt)("p",null,"Defines the Kafka consumer group ID for your Firehose deployment."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"sample-group-id")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required"))),(0,i.kt)("h3",{id:"source_kafka_poll_timeout_ms"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_POLL_TIMEOUT_MS")),(0,i.kt)("p",null,"Defines the duration of poll timeout for Kafka messages in milliseconds"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"80000")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"required")),(0,i.kt)("li",{parentName:"ul"},"Default: ",(0,i.kt)("inlineCode",{parentName:"li"},"9223372036854775807"))),(0,i.kt)("h3",{id:"source_kafka_consumer_config_metadata_max_age_ms"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS")),(0,i.kt)("p",null,"Defines the maximum age of config metadata in milliseconds"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"700")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"500"))),(0,i.kt)("h3",{id:"source_kafka_consumer_mode"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_MODE")),(0,i.kt)("p",null,"Mode can ASYNC or SYNC"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"SYNC")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"SYNC"))),(0,i.kt)("h3",{id:"source_kafka_consumer_config_partition_assignment_strategy"},(0,i.kt)("inlineCode",{parentName:"h3"},"SOURCE_KAFKA_CONSUMER_CONFIG_PARTITION_ASSIGNMENT_STRATEGY")),(0,i.kt)("p",null,"Defines the class of the partition assignor to use for the rebalancing strategy. "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"org.apache.kafka.clients.consumer.StickyAssignor")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"org.apache.kafka.clients.consumer.CooperativeStickyAssignor"))),(0,i.kt)("h2",{id:"stencil-client"},"Stencil Client"),(0,i.kt)("p",null,"Stencil, the Protobuf schema registry used by Firehose need the following variables to be set for the Stencil client."),(0,i.kt)("h3",{id:"schema_registry_stencil_enable"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_ENABLE")),(0,i.kt)("p",null,"Defines whether to enable Stencil Schema registry"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"false"))),(0,i.kt)("h3",{id:"schema_registry_stencil_urls"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_URLS")),(0,i.kt)("p",null,"Defines the URL of the Proto Descriptor set file in the Stencil Server"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"http://localhost:8000/v1/namespaces/quickstart/descriptors/example/versions/latest")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional"))),(0,i.kt)("h3",{id:"schema_registry_stencil_fetch_timeout_ms"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")),(0,i.kt)("p",null,"Defines the timeout in milliseconds to fetch the Proto Descriptor set file from the Stencil Server."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"4000")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"10000"))),(0,i.kt)("h3",{id:"schema_registry_stencil_fetch_retries"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")),(0,i.kt)("p",null,"Defines the number of times to retry to fetch the Proto Descriptor set file from the Stencil Server."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"4")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"3"))),(0,i.kt)("h3",{id:"schema_registry_stencil_fetch_backoff_min_ms"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")),(0,i.kt)("p",null,"Defines the minimum time in milliseconds after which to back off from fetching the Proto Descriptor set file from the Stencil Server."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"70000")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"60000"))),(0,i.kt)("h3",{id:"schema_registry_stencil_fetch_auth_bearer_token"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN")),(0,i.kt)("p",null,"Defines the token for authentication to connect to Stencil Server"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"tcDpw34J8d1")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional"))),(0,i.kt)("h3",{id:"schema_registry_stencil_cache_auto_refresh"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")),(0,i.kt)("p",null,"Defines whether to enable auto-refresh of Stencil cache."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"true")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"false"))),(0,i.kt)("h3",{id:"schema_registry_stencil_cache_ttl_ms"},(0,i.kt)("inlineCode",{parentName:"h3"},"SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")),(0,i.kt)("p",null,"Defines the minimum time in milliseconds after which to refresh the Stencil cache."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example value: ",(0,i.kt)("inlineCode",{parentName:"li"},"900000")),(0,i.kt)("li",{parentName:"ul"},"Type: ",(0,i.kt)("inlineCode",{parentName:"li"},"optional")),(0,i.kt)("li",{parentName:"ul"},"Default value: ",(0,i.kt)("inlineCode",{parentName:"li"},"900000"))))}c.isMDXComponent=!0}}]);