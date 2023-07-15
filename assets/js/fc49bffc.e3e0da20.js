"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[643],{3905:function(e,t,a){a.d(t,{Zo:function(){return d},kt:function(){return p}});var l=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function n(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);t&&(l=l.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,l)}return a}function r(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?n(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):n(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,l,o=function(e,t){if(null==e)return{};var a,l,o={},n=Object.keys(e);for(l=0;l<n.length;l++)a=n[l],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(l=0;l<n.length;l++)a=n[l],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var s=l.createContext({}),c=function(e){var t=l.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):r(r({},t),e)),a},d=function(e){var t=c(e.components);return l.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return l.createElement(l.Fragment,{},t)}},m=l.forwardRef((function(e,t){var a=e.components,o=e.mdxType,n=e.originalType,s=e.parentName,d=i(e,["components","mdxType","originalType","parentName"]),m=c(a),p=o,h=m["".concat(s,".").concat(p)]||m[p]||u[p]||n;return a?l.createElement(h,r(r({ref:t},d),{},{components:a})):l.createElement(h,r({ref:t},d))}));function p(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var n=a.length,r=new Array(n);r[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:o,r[1]=i;for(var c=2;c<n;c++)r[c]=a[c];return l.createElement.apply(null,r)}return l.createElement.apply(null,a)}m.displayName="MDXCreateElement"},2723:function(e,t,a){a.r(t),a.d(t,{assets:function(){return d},contentTitle:function(){return s},default:function(){return p},frontMatter:function(){return i},metadata:function(){return c},toc:function(){return u}});var l=a(7462),o=a(3366),n=(a(7294),a(3905)),r=["components"],i={},s="Metrics",c={unversionedId:"reference/metrics",id:"reference/metrics",title:"Metrics",description:"Service-level Indicators \\(SLIs\\) are the measurements used to calculate the performance for the goal. It is a direct measurement of a service\u2019s behaviour and helps us and the users to evaluate whether our system has been running within SLO. The metrics captured as part of SLI for Firehose are described below.",source:"@site/docs/reference/metrics.md",sourceDirName:"reference",slug:"/reference/metrics",permalink:"/firehose/reference/metrics",draft:!1,editUrl:"https://github.com/raystack/firehose/edit/master/docs/docs/reference/metrics.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Sink Pool",permalink:"/firehose/advance/sink-pool"},next:{title:"FAQs",permalink:"/firehose/reference/core-faqs"}},d={},u=[{value:"Table of Contents",id:"table-of-contents",level:2},{value:"Type Details",id:"type-details",level:2},{value:"<code>Sink</code>",id:"sink",level:3},{value:"<code>Team</code>",id:"team",level:3},{value:"<code>Proto Schema</code>",id:"proto-schema",level:3},{value:"<code>Stream</code>",id:"stream",level:3},{value:"Overview",id:"overview",level:2},{value:"<code>ConsumerLag: MaxLag</code>",id:"consumerlag-maxlag",level:3},{value:"<code>Total Message Received</code>",id:"total-message-received",level:3},{value:"<code>Message Sent Successfully</code>",id:"message-sent-successfully",level:3},{value:"<code>Message Sent Failed</code>",id:"message-sent-failed",level:3},{value:"<code>Message Dropped</code>",id:"message-dropped",level:3},{value:"<code>Batch size Distribution</code>",id:"batch-size-distribution",level:3},{value:"<code>Time spent in Firehose</code>",id:"time-spent-in-firehose",level:3},{value:"<code>Time spent in pipeline</code>",id:"time-spent-in-pipeline",level:3},{value:"<code>Sink Response Time</code>",id:"sink-response-time",level:3},{value:"Pods Health",id:"pods-health",level:2},{value:"<code>JVM Lifetime</code>",id:"jvm-lifetime",level:3},{value:"<code>Cpu Load</code>",id:"cpu-load",level:3},{value:"<code>Cpu Time</code>",id:"cpu-time",level:3},{value:"Kafka Consumer Details",id:"kafka-consumer-details",level:2},{value:"<code>Assigned partitions</code>",id:"assigned-partitions",level:3},{value:"<code>Consumer Number of Request/second</code>",id:"consumer-number-of-requestsecond",level:3},{value:"<code>Records Consumed Rate/second</code>",id:"records-consumed-ratesecond",level:3},{value:"<code>Bytes Consumed Rate/second</code>",id:"bytes-consumed-ratesecond",level:3},{value:"<code>Fetch Rate/second</code>",id:"fetch-ratesecond",level:3},{value:"<code>Max Fetch Latency</code>",id:"max-fetch-latency",level:3},{value:"<code>Average Fetch Latency</code>",id:"average-fetch-latency",level:3},{value:"<code>Average Fetch Size</code>",id:"average-fetch-size",level:3},{value:"<code>Max Fetch Size</code>",id:"max-fetch-size",level:3},{value:"<code>Commit Rate/second</code>",id:"commit-ratesecond",level:3},{value:"<code>Consumer Active Connections Count</code>",id:"consumer-active-connections-count",level:3},{value:"<code>New Connections Creation Rate/second</code>",id:"new-connections-creation-ratesecond",level:3},{value:"<code>Connections Close Rate/second</code>",id:"connections-close-ratesecond",level:3},{value:"<code>Consumer Outgoing Byte Rate/Sec</code>",id:"consumer-outgoing-byte-ratesec",level:3},{value:"<code>Avg time between poll</code>",id:"avg-time-between-poll",level:3},{value:"<code>Max time between poll</code>",id:"max-time-between-poll",level:3},{value:"<code>Sync rate</code>",id:"sync-rate",level:3},{value:"<code>Consumer Network IO rate /second</code>",id:"consumer-network-io-rate-second",level:3},{value:"<code>Rebalance Rate /hour</code>",id:"rebalance-rate-hour",level:3},{value:"<code>Average Commit latency</code>",id:"average-commit-latency",level:3},{value:"<code>Max Commit latency</code>",id:"max-commit-latency",level:3},{value:"<code>Avg Rebalance latency</code>",id:"avg-rebalance-latency",level:3},{value:"<code>Max Rebalance latency</code>",id:"max-rebalance-latency",level:3},{value:"Error",id:"error",level:2},{value:"<code>Fatal Error</code>",id:"fatal-error",level:3},{value:"<code>Nonfatal Error</code>",id:"nonfatal-error",level:3},{value:"Memory",id:"memory",level:2},{value:"<code>Heap Memory Usage</code>",id:"heap-memory-usage",level:3},{value:"<code>Non-Heap Memory Usage</code>",id:"non-heap-memory-usage",level:3},{value:"<code>GC: Memory Pool Collection Usage</code>",id:"gc-memory-pool-collection-usage",level:3},{value:"<code>GC: Memory Pool Peak Usage</code>",id:"gc-memory-pool-peak-usage",level:3},{value:"<code>GC: Memory Pool Usage</code>",id:"gc-memory-pool-usage",level:3},{value:"Garbage Collection",id:"garbage-collection",level:2},{value:"<code>GC Collection Count</code>",id:"gc-collection-count",level:3},{value:"<code>GC Collection Time</code>",id:"gc-collection-time",level:3},{value:"<code>Thread Count</code>",id:"thread-count",level:3},{value:"<code>Class Count</code>",id:"class-count",level:3},{value:"<code>Code Cache Memory after GC</code>",id:"code-cache-memory-after-gc",level:3},{value:"<code>Compressed Class Space after GC</code>",id:"compressed-class-space-after-gc",level:3},{value:"<code>Metaspace after GC</code>",id:"metaspace-after-gc",level:3},{value:"<code>Par Eden Space after GC</code>",id:"par-eden-space-after-gc",level:3},{value:"<code>Par Survivor Space after GC</code>",id:"par-survivor-space-after-gc",level:3},{value:"<code>Tenured Space after GC</code>",id:"tenured-space-after-gc",level:3},{value:"Retry",id:"retry",level:2},{value:"<code>Average Retry Requests</code>",id:"average-retry-requests",level:3},{value:"<code>Back Off time</code>",id:"back-off-time",level:3},{value:"HTTP Sink",id:"http-sink",level:2},{value:"<code>2XX Response Count</code>",id:"2xx-response-count",level:3},{value:"<code>4XX Response Count</code>",id:"4xx-response-count",level:3},{value:"<code>5XX Response Count</code>",id:"5xx-response-count",level:3},{value:"<code>No Response Count</code>",id:"no-response-count",level:3},{value:"Filter",id:"filter",level:2},{value:"<code>Filter Type</code>",id:"filter-type",level:3},{value:"<code>Total Messages filtered</code>",id:"total-messages-filtered",level:3},{value:"Blob Sink",id:"blob-sink",level:2},{value:"<code>Local File Open Total</code>",id:"local-file-open-total",level:3},{value:"<code>Local File Closed Total</code>",id:"local-file-closed-total",level:3},{value:"<code>Local File Closing Time</code>",id:"local-file-closing-time",level:3},{value:"<code>Local File Records Total</code>",id:"local-file-records-total",level:3},{value:"<code>Local File Size Bytes</code>",id:"local-file-size-bytes",level:3},{value:"<code>File Uploaded Total</code>",id:"file-uploaded-total",level:3},{value:"<code>File Upload Time</code>",id:"file-upload-time",level:3},{value:"<code>File Upload Size Bytes</code>",id:"file-upload-size-bytes",level:3},{value:"<code>File Upload Records Total</code>",id:"file-upload-records-total",level:3}],m={toc:u};function p(e){var t=e.components,a=(0,o.Z)(e,r);return(0,n.kt)("wrapper",(0,l.Z)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"metrics"},"Metrics"),(0,n.kt)("p",null,"Service-level Indicators ","(","SLIs",")"," are the measurements used to calculate the performance for the goal. It is a direct measurement of a service\u2019s behaviour and helps us and the users to evaluate whether our system has been running within SLO. The metrics captured as part of SLI for Firehose are described below."),(0,n.kt)("h2",{id:"table-of-contents"},"Table of Contents"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#type-details"},"Type Details")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#overview"},"Overview")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#pods-health"},"Pods Health")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#kafka-consumer-details"},"Kafka Consumer Details")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#error"},"Error")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#memory"},"Memory")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#error"},"Error")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#garbage-collection"},"Garbage Collection")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#retry"},"Retry")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#http-sink"},"HTTP Sink")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#filter"},"Filter")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/firehose/reference/metrics#blob-sink"},"Blob Sink")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://github.com/raystack/depot/blob/main/docs/reference/metrics.md#bigquery-sink"},"Bigquery Sink"))),(0,n.kt)("h2",{id:"type-details"},"Type Details"),(0,n.kt)("p",null,"Collection of all the generic configurations in a Firehose."),(0,n.kt)("h3",{id:"sink"},(0,n.kt)("inlineCode",{parentName:"h3"},"Sink")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The type of sink of the Firehose. It could be 'log', 'HTTP', 'DB', 'redis', 'influx' or 'Elasticsearch'")),(0,n.kt)("h3",{id:"team"},(0,n.kt)("inlineCode",{parentName:"h3"},"Team")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Team who has the ownership for the given Firehose.")),(0,n.kt)("h3",{id:"proto-schema"},(0,n.kt)("inlineCode",{parentName:"h3"},"Proto Schema")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The proto class used for creating the Firehose")),(0,n.kt)("h3",{id:"stream"},(0,n.kt)("inlineCode",{parentName:"h3"},"Stream")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The stream where the input topic is read from")),(0,n.kt)("h2",{id:"overview"},"Overview"),(0,n.kt)("p",null,"Some of the most important metrics related to Firehose that gives you an overview of the current state of it."),(0,n.kt)("h3",{id:"consumerlag-maxlag"},(0,n.kt)("inlineCode",{parentName:"h3"},"ConsumerLag: MaxLag")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The maximum lag in terms of number of records for any partition in this window. An increasing value over time is your best indication that the consumer group is not keeping up with the producers.")),(0,n.kt)("h3",{id:"total-message-received"},(0,n.kt)("inlineCode",{parentName:"h3"},"Total Message Received")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Sum of all messages received from Kafka per pod.")),(0,n.kt)("h3",{id:"message-sent-successfully"},(0,n.kt)("inlineCode",{parentName:"h3"},"Message Sent Successfully")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Messages sent successfully to the sink per batch per pod.")),(0,n.kt)("h3",{id:"message-sent-failed"},(0,n.kt)("inlineCode",{parentName:"h3"},"Message Sent Failed")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Messages failed to be pushed into the sink per batch per pod. In case of HTTP sink, if status code is not in retry codes configured, the records will be dropped.")),(0,n.kt)("h3",{id:"message-dropped"},(0,n.kt)("inlineCode",{parentName:"h3"},"Message Dropped")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"In case of HTTP sink, when status code is not in retry codes configured, the records are dropped. This metric captures the dropped messages count.")),(0,n.kt)("h3",{id:"batch-size-distribution"},(0,n.kt)("inlineCode",{parentName:"h3"},"Batch size Distribution")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"99p of batch size distribution for pulled and pushed messages per pod.")),(0,n.kt)("h3",{id:"time-spent-in-firehose"},(0,n.kt)("inlineCode",{parentName:"h3"},"Time spent in Firehose")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Latency introduced by Firehose ","(","time before sending to sink - time when reading from Kafka",")",". Note: It could be high if the response time of the sink is higher as subsequent batches could be delayed.")),(0,n.kt)("h3",{id:"time-spent-in-pipeline"},(0,n.kt)("inlineCode",{parentName:"h3"},"Time spent in pipeline")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Time difference between Kafka ingestion and sending to sink ","(","Time before sending to sink - Time of Kafka ingestion",")")),(0,n.kt)("h3",{id:"sink-response-time"},(0,n.kt)("inlineCode",{parentName:"h3"},"Sink Response Time")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Different percentile of the response time of the sink.")),(0,n.kt)("h2",{id:"pods-health"},"Pods Health"),(0,n.kt)("p",null,"Since Firehose runs on Kube, this gives a nice health details of each pods."),(0,n.kt)("h3",{id:"jvm-lifetime"},(0,n.kt)("inlineCode",{parentName:"h3"},"JVM Lifetime")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"JVM Uptime of each pod.")),(0,n.kt)("h3",{id:"cpu-load"},(0,n.kt)("inlineCode",{parentName:"h3"},"Cpu Load")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},'Returns the "recent cpu usage" for the Java Virtual Machine process. This value is a double in the ',"[","0.0,1.0","]"," interval. A value of 0.0 means that none of the CPUs were running threads from the JVM process during the recent period of time observed, while a value of 1.0 means that all CPUs were actively running threads from the JVM 100% of the time during the recent period being observed. Threads from the JVM include the application threads as well as the JVM internal threads. All values betweens 0.0 and 1.0 are possible depending of the activities going on in the JVM process and the whole system. If the Java Virtual Machine recent CPU usage is not available, the method returns a negative value.")),(0,n.kt)("h3",{id:"cpu-time"},(0,n.kt)("inlineCode",{parentName:"h3"},"Cpu Time")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Returns the CPU time used by the process on which the Java virtual machine is running. The returned value is of nanoseconds precision but not necessarily nanoseconds accuracy.")),(0,n.kt)("h2",{id:"kafka-consumer-details"},"Kafka Consumer Details"),(0,n.kt)("p",null,"Listing some of the Kafka consumer metrics here."),(0,n.kt)("h3",{id:"assigned-partitions"},(0,n.kt)("inlineCode",{parentName:"h3"},"Assigned partitions")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Consumer Group Metrics: The number of partitions currently assigned to this consumer ","(","per pod",")",".")),(0,n.kt)("h3",{id:"consumer-number-of-requestsecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"Consumer Number of Request/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Global Request Metrics: The average number of requests sent per second per pod.")),(0,n.kt)("h3",{id:"records-consumed-ratesecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"Records Consumed Rate/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Topic-level Fetch Metrics: The average number of records consumed per second for a specific topic per pod.")),(0,n.kt)("h3",{id:"bytes-consumed-ratesecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"Bytes Consumed Rate/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Topic-level Fetch Metrics: The average number of bytes consumed per second per pod.")),(0,n.kt)("h3",{id:"fetch-ratesecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"Fetch Rate/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Fetch Metrics: The number of fetch requests per second per pod.")),(0,n.kt)("h3",{id:"max-fetch-latency"},(0,n.kt)("inlineCode",{parentName:"h3"},"Max Fetch Latency")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Fetch Metrics: The max time taken for a fetch request per pod.")),(0,n.kt)("h3",{id:"average-fetch-latency"},(0,n.kt)("inlineCode",{parentName:"h3"},"Average Fetch Latency")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Fetch Metrics: The average time taken for a fetch request per pod.")),(0,n.kt)("h3",{id:"average-fetch-size"},(0,n.kt)("inlineCode",{parentName:"h3"},"Average Fetch Size")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Fetch Metrics: The average number of bytes fetched per request per pod.")),(0,n.kt)("h3",{id:"max-fetch-size"},(0,n.kt)("inlineCode",{parentName:"h3"},"Max Fetch Size")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Fetch Metrics: The max number of bytes fetched per request per pod.")),(0,n.kt)("h3",{id:"commit-ratesecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"Commit Rate/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Consumer Group Metrics: The number of commit calls per second per pod.")),(0,n.kt)("h3",{id:"consumer-active-connections-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"Consumer Active Connections Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Global Connection Metrics: The current number of active connections per pod.")),(0,n.kt)("h3",{id:"new-connections-creation-ratesecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"New Connections Creation Rate/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Global Connection Metrics: New connections established per second in the window per pod.")),(0,n.kt)("h3",{id:"connections-close-ratesecond"},(0,n.kt)("inlineCode",{parentName:"h3"},"Connections Close Rate/second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Global Connection Metrics: Connections closed per second in the window per pod.")),(0,n.kt)("h3",{id:"consumer-outgoing-byte-ratesec"},(0,n.kt)("inlineCode",{parentName:"h3"},"Consumer Outgoing Byte Rate/Sec")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Global Request Metrics: The average number of outgoing bytes sent per second to all servers per pod.")),(0,n.kt)("h3",{id:"avg-time-between-poll"},(0,n.kt)("inlineCode",{parentName:"h3"},"Avg time between poll")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Average time spent between poll per pod.")),(0,n.kt)("h3",{id:"max-time-between-poll"},(0,n.kt)("inlineCode",{parentName:"h3"},"Max time between poll")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Max time spent between poll per pod.")),(0,n.kt)("h3",{id:"sync-rate"},(0,n.kt)("inlineCode",{parentName:"h3"},"Sync rate")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Consumer Group Metrics: The number of group syncs per second per pod. Group synchronization is the second and last phase of the rebalance protocol. Similar to join-rate, a large value indicates group instability.")),(0,n.kt)("h3",{id:"consumer-network-io-rate-second"},(0,n.kt)("inlineCode",{parentName:"h3"},"Consumer Network IO rate /second")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The average number of network operations ","(","reads or writes",")"," on all connections per second per pod")),(0,n.kt)("h3",{id:"rebalance-rate-hour"},(0,n.kt)("inlineCode",{parentName:"h3"},"Rebalance Rate /hour")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Rate of rebalance the consumer.")),(0,n.kt)("h3",{id:"average-commit-latency"},(0,n.kt)("inlineCode",{parentName:"h3"},"Average Commit latency")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Consumer Group Metrics: The average time taken for a commit request per pod")),(0,n.kt)("h3",{id:"max-commit-latency"},(0,n.kt)("inlineCode",{parentName:"h3"},"Max Commit latency")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Consumer Group Metrics: The max time taken for a commit request per pod.")),(0,n.kt)("h3",{id:"avg-rebalance-latency"},(0,n.kt)("inlineCode",{parentName:"h3"},"Avg Rebalance latency")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Average Rebalance Latency for the consumer per pod.")),(0,n.kt)("h3",{id:"max-rebalance-latency"},(0,n.kt)("inlineCode",{parentName:"h3"},"Max Rebalance latency")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Max Rebalance Latency for the consumer per pod.")),(0,n.kt)("h2",{id:"error"},"Error"),(0,n.kt)("p",null,"This gives you a nice insight about the critical and noncritical exceptions happened in the Firehose."),(0,n.kt)("h3",{id:"fatal-error"},(0,n.kt)("inlineCode",{parentName:"h3"},"Fatal Error")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Count of all the exception raised by the pods which can restart the Firehose.")),(0,n.kt)("h3",{id:"nonfatal-error"},(0,n.kt)("inlineCode",{parentName:"h3"},"Nonfatal Error")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Count of all the exception raised by the Firehose which will not restart the Firehose and Firehose will keep retrying.")),(0,n.kt)("h2",{id:"memory"},"Memory"),(0,n.kt)("p",null,"Details on memory used by the Firehose for different tasks."),(0,n.kt)("h3",{id:"heap-memory-usage"},(0,n.kt)("inlineCode",{parentName:"h3"},"Heap Memory Usage")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Details of heap memory usage:"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-text"},"Max: The amount of memory that can be used for memory management\nUsed: The amount of memory currently in use\n")))),(0,n.kt)("h3",{id:"non-heap-memory-usage"},(0,n.kt)("inlineCode",{parentName:"h3"},"Non-Heap Memory Usage")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Details of non-heap memory usage:"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-text"},"Max: The amount of memory that can be used for memory management\nUsed: The amount of memory currently in use\n")))),(0,n.kt)("h3",{id:"gc-memory-pool-collection-usage"},(0,n.kt)("inlineCode",{parentName:"h3"},"GC: Memory Pool Collection Usage")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"For a garbage-collected memory pool, the amount of used memory includes the memory occupied by all objects in the pool including both reachable and unreachable objects. This is for all the names in the type: MemoryPool.")),(0,n.kt)("h3",{id:"gc-memory-pool-peak-usage"},(0,n.kt)("inlineCode",{parentName:"h3"},"GC: Memory Pool Peak Usage")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Peak usage of GC memory usage.")),(0,n.kt)("h3",{id:"gc-memory-pool-usage"},(0,n.kt)("inlineCode",{parentName:"h3"},"GC: Memory Pool Usage")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Total usage of GC memory usage.")),(0,n.kt)("h2",{id:"garbage-collection"},"Garbage Collection"),(0,n.kt)("p",null,"All JVM Garbage Collection Details."),(0,n.kt)("h3",{id:"gc-collection-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"GC Collection Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The total number of collections that have occurred per pod. Rather than showing the absolute value we are showing the difference to see the rate of change more easily.")),(0,n.kt)("h3",{id:"gc-collection-time"},(0,n.kt)("inlineCode",{parentName:"h3"},"GC Collection Time")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The approximate accumulated collection elapsed time in milliseconds per pod. Rather than showing the absolute value we are showing the difference to see the rate of change more easily.")),(0,n.kt)("h3",{id:"thread-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"Thread Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"daemonThreadCount: Returns the current number of live daemon threads per pod peakThreadCount: Returns the peak live thread count since the Java virtual machine started or peak was reset per pod threadCount: Returns the current number of live threads including both daemon and non-daemon threads per pod.")),(0,n.kt)("h3",{id:"class-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"Class Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"loadedClass: Displays number of classes that are currently loaded in the Java virtual machine per pod unloadedClass: Displays the total number of classes unloaded since the Java virtual machine has started execution.")),(0,n.kt)("h3",{id:"code-cache-memory-after-gc"},(0,n.kt)("inlineCode",{parentName:"h3"},"Code Cache Memory after GC")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The code cache memory usage in the memory pools at the end of a GC per pod.")),(0,n.kt)("h3",{id:"compressed-class-space-after-gc"},(0,n.kt)("inlineCode",{parentName:"h3"},"Compressed Class Space after GC")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The compressed class space memory usage in the memory pools at the end of a GC per pod.")),(0,n.kt)("h3",{id:"metaspace-after-gc"},(0,n.kt)("inlineCode",{parentName:"h3"},"Metaspace after GC")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The metaspace memory usage in the memory pools at the end of a GC per pod.")),(0,n.kt)("h3",{id:"par-eden-space-after-gc"},(0,n.kt)("inlineCode",{parentName:"h3"},"Par Eden Space after GC")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The eden space memory usage in the memory pools at the end of a GC per pod.")),(0,n.kt)("h3",{id:"par-survivor-space-after-gc"},(0,n.kt)("inlineCode",{parentName:"h3"},"Par Survivor Space after GC")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"The survivor space memory usage in the memory pools at the end of a GC per pod.")),(0,n.kt)("h3",{id:"tenured-space-after-gc"},(0,n.kt)("inlineCode",{parentName:"h3"},"Tenured Space after GC")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"The tenured space memory usage in the memory pools at the end of a GC per pod."),(0,n.kt)("p",{parentName:"li"},(0,n.kt)("strong",{parentName:"p"},(0,n.kt)("inlineCode",{parentName:"strong"},"File Descriptor")))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Number of file descriptor per pod"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-text"},"Open: Current open file descriptors\nMax: Based on config max allowed\n")))),(0,n.kt)("h2",{id:"retry"},"Retry"),(0,n.kt)("p",null,"If you have configured retries this will give you some insight about the retries."),(0,n.kt)("h3",{id:"average-retry-requests"},(0,n.kt)("inlineCode",{parentName:"h3"},"Average Retry Requests")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Request retries per min per pod.")),(0,n.kt)("h3",{id:"back-off-time"},(0,n.kt)("inlineCode",{parentName:"h3"},"Back Off time")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Time spent per pod backing off.")),(0,n.kt)("h2",{id:"http-sink"},"HTTP Sink"),(0,n.kt)("p",null,"HTTP Sink response code details."),(0,n.kt)("h3",{id:"2xx-response-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"2XX Response Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Total number of 2xx response received by Firehose from the HTTP service,")),(0,n.kt)("h3",{id:"4xx-response-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"4XX Response Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Total number of 4xx response received by Firehose from the HTTP service.")),(0,n.kt)("h3",{id:"5xx-response-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"5XX Response Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Total number of 5xx response received by Firehose from the HTTP service.")),(0,n.kt)("h3",{id:"no-response-count"},(0,n.kt)("inlineCode",{parentName:"h3"},"No Response Count")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Total number of No response received by Firehose from the HTTP service.")),(0,n.kt)("h2",{id:"filter"},"Filter"),(0,n.kt)("p",null,"Since Firehose supports filtration based on some data, these metrics give some information related to that."),(0,n.kt)("h3",{id:"filter-type"},(0,n.kt)("inlineCode",{parentName:"h3"},"Filter Type")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},'Type of filter in the Firehose. It will be one of the "none", "key", "message".')),(0,n.kt)("h3",{id:"total-messages-filtered"},(0,n.kt)("inlineCode",{parentName:"h3"},"Total Messages filtered")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Sum of all the messages filtered because of the filter condition per pod.")),(0,n.kt)("h2",{id:"blob-sink"},"Blob Sink"),(0,n.kt)("h3",{id:"local-file-open-total"},(0,n.kt)("inlineCode",{parentName:"h3"},"Local File Open Total")),(0,n.kt)("p",null,"A gauge, total number of local file that is currently being opened."),(0,n.kt)("h3",{id:"local-file-closed-total"},(0,n.kt)("inlineCode",{parentName:"h3"},"Local File Closed Total")),(0,n.kt)("p",null,"Total number of local file that being closed and ready to be uploaded, excluding local file that being closed prematurely due to consumer restart."),(0,n.kt)("h3",{id:"local-file-closing-time"},(0,n.kt)("inlineCode",{parentName:"h3"},"Local File Closing Time")),(0,n.kt)("p",null,"Duration of local file closing time."),(0,n.kt)("h3",{id:"local-file-records-total"},(0,n.kt)("inlineCode",{parentName:"h3"},"Local File Records Total")),(0,n.kt)("p",null,"Total number of records that written to all files that have been closed and ready to be uploaded."),(0,n.kt)("h3",{id:"local-file-size-bytes"},(0,n.kt)("inlineCode",{parentName:"h3"},"Local File Size Bytes")),(0,n.kt)("p",null,"Size of file in bytes."),(0,n.kt)("h3",{id:"file-uploaded-total"},(0,n.kt)("inlineCode",{parentName:"h3"},"File Uploaded Total")),(0,n.kt)("p",null,"Total number file that successfully being uploaded."),(0,n.kt)("h3",{id:"file-upload-time"},(0,n.kt)("inlineCode",{parentName:"h3"},"File Upload Time")),(0,n.kt)("p",null,"Duration of file upload."),(0,n.kt)("h3",{id:"file-upload-size-bytes"},(0,n.kt)("inlineCode",{parentName:"h3"},"File Upload Size Bytes")),(0,n.kt)("p",null,"Total Size of the uploaded file in bytes."),(0,n.kt)("h3",{id:"file-upload-records-total"},(0,n.kt)("inlineCode",{parentName:"h3"},"File Upload Records Total")),(0,n.kt)("p",null,"Total number records inside files that successfully being uploaded to blob storage."))}p.isMDXComponent=!0}}]);