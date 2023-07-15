"use strict";(self.webpackChunkfirehose=self.webpackChunkfirehose||[]).push([[601],{3905:function(e,n,t){t.d(n,{Zo:function(){return c},kt:function(){return m}});var i=t(7294);function l(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function a(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);n&&(i=i.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,i)}return t}function r(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?a(Object(t),!0).forEach((function(n){l(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):a(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function o(e,n){if(null==e)return{};var t,i,l=function(e,n){if(null==e)return{};var t,i,l={},a=Object.keys(e);for(i=0;i<a.length;i++)t=a[i],n.indexOf(t)>=0||(l[t]=e[t]);return l}(e,n);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(i=0;i<a.length;i++)t=a[i],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(l[t]=e[t])}return l}var u=i.createContext({}),p=function(e){var n=i.useContext(u),t=n;return e&&(t="function"==typeof e?e(n):r(r({},n),e)),t},c=function(e){var n=p(e.components);return i.createElement(u.Provider,{value:n},e.children)},d={inlineCode:"code",wrapper:function(e){var n=e.children;return i.createElement(i.Fragment,{},n)}},s=i.forwardRef((function(e,n){var t=e.components,l=e.mdxType,a=e.originalType,u=e.parentName,c=o(e,["components","mdxType","originalType","parentName"]),s=p(t),m=l,_=s["".concat(u,".").concat(m)]||s[m]||d[m]||a;return t?i.createElement(_,r(r({ref:n},c),{},{components:t})):i.createElement(_,r({ref:n},c))}));function m(e,n){var t=arguments,l=n&&n.mdxType;if("string"==typeof e||l){var a=t.length,r=new Array(a);r[0]=s;var o={};for(var u in n)hasOwnProperty.call(n,u)&&(o[u]=n[u]);o.originalType=e,o.mdxType="string"==typeof e?e:l,r[1]=o;for(var p=2;p<a;p++)r[p]=t[p];return i.createElement.apply(null,r)}return i.createElement.apply(null,t)}s.displayName="MDXCreateElement"},664:function(e,n,t){t.r(n),t.d(n,{assets:function(){return c},contentTitle:function(){return u},default:function(){return m},frontMatter:function(){return o},metadata:function(){return p},toc:function(){return d}});var i=t(7462),l=t(3366),a=(t(7294),t(3905)),r=["components"],o={},u="JDBC",p={unversionedId:"sinks/jdbc-sink",id:"sinks/jdbc-sink",title:"JDBC",description:"A JDBC sink Firehose \\(SINK_TYPE=jdbc\\) requires the following variables to be set along with Generic ones",source:"@site/docs/sinks/jdbc-sink.md",sourceDirName:"sinks",slug:"/sinks/jdbc-sink",permalink:"/firehose/sinks/jdbc-sink",draft:!1,editUrl:"https://github.com/raystack/firehose/edit/master/docs/docs/sinks/jdbc-sink.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"GRPC",permalink:"/firehose/sinks/grpc-sink"},next:{title:"BigQuery",permalink:"/firehose/sinks/bigquery-sink"}},c={},d=[{value:"<code>SINK_JDBC_URL</code>",id:"sink_jdbc_url",level:3},{value:"<code>SINK_JDBC_TABLE_NAME</code>",id:"sink_jdbc_table_name",level:3},{value:"<code>SINK_JDBC_USERNAME</code>",id:"sink_jdbc_username",level:3},{value:"<code>SINK_JDBC_PASSWORD</code>",id:"sink_jdbc_password",level:3},{value:"<code>INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING</code>",id:"input_schema_proto_to_column_mapping",level:3},{value:"<code>SINK_JDBC_UNIQUE_KEYS</code>",id:"sink_jdbc_unique_keys",level:3},{value:"<code>SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS</code>",id:"sink_jdbc_connection_pool_timeout_ms",level:3},{value:"<code>SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS</code>",id:"sink_jdbc_connection_pool_idle_timeout_ms",level:3},{value:"<code>SINK_JDBC_CONNECTION_POOL_MIN_IDLE</code>",id:"sink_jdbc_connection_pool_min_idle",level:3},{value:"<code>SINK_JDBC_CONNECTION_POOL_MAX_SIZE</code>",id:"sink_jdbc_connection_pool_max_size",level:3}],s={toc:d};function m(e){var n=e.components,t=(0,l.Z)(e,r);return(0,a.kt)("wrapper",(0,i.Z)({},s,t,{components:n,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"jdbc"},"JDBC"),(0,a.kt)("p",null,"A JDBC sink Firehose ","(",(0,a.kt)("inlineCode",{parentName:"p"},"SINK_TYPE"),"=",(0,a.kt)("inlineCode",{parentName:"p"},"jdbc"),")"," requires the following variables to be set along with Generic ones"),(0,a.kt)("h3",{id:"sink_jdbc_url"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_URL")),(0,a.kt)("p",null,"Deifnes the PostgresDB URL, it's usually the hostname followed by port."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"jdbc:postgresql://localhost:5432/postgres")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_jdbc_table_name"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_TABLE_NAME")),(0,a.kt)("p",null,"Defines the name of the table in which the data should be dumped."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"public.customers")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_jdbc_username"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_USERNAME")),(0,a.kt)("p",null,"Defines the username to connect to DB."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"root")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_jdbc_password"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_PASSWORD")),(0,a.kt)("p",null,"Defines the password to connect to DB."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"root")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"input_schema_proto_to_column_mapping"},(0,a.kt)("inlineCode",{parentName:"h3"},"INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING")),(0,a.kt)("p",null,"Defines the mapping of fields in DB and the corresponding proto index from where the value will be extracted. This is a JSON field."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},'{"6":"customer_id","1":"service_type","5":"event_timestamp"}')," Proto field value with index 1 will be stored in a column named service_type in DB and so on"),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"sink_jdbc_unique_keys"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_UNIQUE_KEYS")),(0,a.kt)("p",null,"Defines a comma-separated column names having a unique constraint on the table."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"customer_id")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional"))),(0,a.kt)("h3",{id:"sink_jdbc_connection_pool_timeout_ms"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS")),(0,a.kt)("p",null,"Defines a database connection timeout in milliseconds."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"1000")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"1000"))),(0,a.kt)("h3",{id:"sink_jdbc_connection_pool_idle_timeout_ms"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS")),(0,a.kt)("p",null,"Defines a database connection pool idle connection timeout in milliseconds."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"60000")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"60000"))),(0,a.kt)("h3",{id:"sink_jdbc_connection_pool_min_idle"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_CONNECTION_POOL_MIN_IDLE")),(0,a.kt)("p",null,"Defines the minimum number of idle connections in the pool to maintain."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"0")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"0"))),(0,a.kt)("h3",{id:"sink_jdbc_connection_pool_max_size"},(0,a.kt)("inlineCode",{parentName:"h3"},"SINK_JDBC_CONNECTION_POOL_MAX_SIZE")),(0,a.kt)("p",null,"Defines the maximum size for the database connection pool."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"10")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required")),(0,a.kt)("li",{parentName:"ul"},"Default value: ",(0,a.kt)("inlineCode",{parentName:"li"},"10"))))}m.isMDXComponent=!0}}]);