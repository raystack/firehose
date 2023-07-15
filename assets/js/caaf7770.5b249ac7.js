"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[119],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return m}});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),d=c(r),m=o,f=d["".concat(s,".").concat(m)]||d[m]||p[m]||a;return r?n.createElement(f,i(i({ref:t},u),{},{components:r})):n.createElement(f,i({ref:t},u))}));function m(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,i=new Array(a);i[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var c=2;c<a;c++)i[c]=r[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},5667:function(e,t,r){r.r(t),r.d(t,{assets:function(){return u},contentTitle:function(){return s},default:function(){return m},frontMatter:function(){return l},metadata:function(){return c},toc:function(){return p}});var n=r(7462),o=r(3366),a=(r(7294),r(3905)),i=["components"],l={},s="Deployment",c={unversionedId:"guides/deployment",id:"guides/deployment",title:"Deployment",description:"Firehose can deployed locally, inside a Docker container or in a Kubernetes cluster. The following external services must be installed and launched before deploying Firehose on any platform -",source:"@site/docs/guides/deployment.md",sourceDirName:"guides",slug:"/guides/deployment",permalink:"/firehose/guides/deployment",draft:!1,editUrl:"https://github.com/raystack/firehose/edit/master/docs/docs/guides/deployment.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"JEXL-based Filters",permalink:"/firehose/guides/jexl-based-filters"},next:{title:"Troubleshooting",permalink:"/firehose/guides/manage"}},u={},p=[{value:"Deploy on Docker",id:"deploy-on-docker",level:2},{value:"Deploy on Kubernetes",id:"deploy-on-kubernetes",level:2},{value:"Deploy locally",id:"deploy-locally",level:2}],d={toc:p};function m(e){var t=e.components,r=(0,o.Z)(e,i);return(0,a.kt)("wrapper",(0,n.Z)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"deployment"},"Deployment"),(0,a.kt)("p",null,"Firehose can deployed locally, inside a Docker container or in a Kubernetes cluster. The following external services must be installed and launched before deploying Firehose on any platform -"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Apache Kafka Server 2.3+"),(0,a.kt)("li",{parentName:"ul"},"Stencil Server as schema registry"),(0,a.kt)("li",{parentName:"ul"},"Telegraf as the StatsD host"),(0,a.kt)("li",{parentName:"ul"},"InfluxDB for storing metrics"),(0,a.kt)("li",{parentName:"ul"},"Grafana for metrics visualization"),(0,a.kt)("li",{parentName:"ul"},"destination Sink server")),(0,a.kt)("p",null,"Refer the ",(0,a.kt)("a",{parentName:"p",href:"/firehose/contribute/development"},"Development Guide")," section on how to set up and configure the above services. For instructions on how to set up visualization of Firehose metrics , refer the ",(0,a.kt)("a",{parentName:"p",href:"/firehose/concepts/monitoring#setting-up-grafana-with-firehose"},"Monitoring "),"section."),(0,a.kt)("h2",{id:"deploy-on-docker"},"Deploy on Docker"),(0,a.kt)("p",null,"Use the Docker hub to download Firehose ",(0,a.kt)("a",{parentName:"p",href:"https://hub.docker.com/r/raystack/firehose/"},"docker image"),". You need to have Docker installed in your system. Follow",(0,a.kt)("a",{parentName:"p",href:"https://www.docker.com/products/docker-desktop"}," this guide")," on how to install and set up Docker in your system."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-text"},"# Download docker image from docker hub\n$ docker pull raystack/firehose\n\n# Run the following docker command for a simple log sink.\n$ docker run -e SOURCE_KAFKA_BROKERS=127.0.0.1:6667 -e SOURCE_KAFKA_CONSUMER_GROUP_ID=kafka-consumer-group-id -e SOURCE_KAFKA_TOPIC=sample-topic -e SINK_TYPE=log -e SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e INPUT_SCHEMA_PROTO_CLASS=com.github.firehose.sampleLogProto.SampleLogMessage -e SCHEMA_REGISTRY_STENCIL_ENABLE=true -e SCHEMA_REGISTRY_STENCIL_URLS=http://localhost:9000/artifactory/proto-descriptors/latest/raystack/firehose:latest\n")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Note:")," Make sure your protos ","(",".jar file",")"," are located in ",(0,a.kt)("inlineCode",{parentName:"p"},"work-dir"),", this is required for Filter functionality to work."),(0,a.kt)("h2",{id:"deploy-on-kubernetes"},"Deploy on Kubernetes"),(0,a.kt)("p",null,"Kubernetes is an open-source container-orchestration system for automating computer application deployment, scaling, and management. Follow ",(0,a.kt)("a",{parentName:"p",href:"https://kubernetes.io/docs/setup/"},"this guide")," on how to set up and configure a Kubernetes cluster."),(0,a.kt)("p",null,"Then create a Firehose deployment using the Helm chart available ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/raystack/charts/tree/main/stable/firehose"},"here"),". The Helm chart Deployment also includes Telegraf container which works as a metrics aggregator and pushes StatsD metrics to InfluxDB. Make sure to configure all Kafka, sink and filter parameters in ",(0,a.kt)("inlineCode",{parentName:"p"},"values.yaml")," file before deploying the Helm chart."),(0,a.kt)("p",null,"Description and default values for each parameter can be found",(0,a.kt)("a",{parentName:"p",href:"https://github.com/raystack/charts/tree/main/stable/firehose#values"}," here"),"."),(0,a.kt)("h2",{id:"deploy-locally"},"Deploy locally"),(0,a.kt)("p",null,"Firehose needs Java SE Development Kit 8 to be installed and configured in ",(0,a.kt)("inlineCode",{parentName:"p"},"JAVA_HOME")," environment variable."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-text"},"# Clone the repo\n$ git clone https://github.com/raystack/firehose.git\n\n# Build the jar\n$ ./gradlew clean build\n\n# Configure env variables\n$ cat env/local.properties\n\n# Run the Firehose\n$ ./gradlew runConsumer\n")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Note:")," Sample configuration for other sinks along with some advanced configurations can be found ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/raystack/firehose/blob/main/docs/reference/configuration.md"},"here")))}m.isMDXComponent=!0}}]);