# Overview

The following topics will describe how to manage Firehose throughout its lifecycle. 

### Creating Firehose

Firehose is designed to work with different sinks. Each deployment of firehose can only have one sink. Currently supported sinks by Firehose are Log, HTTP, JDBC, InfluxDB, Prometheus, GRPC, Elastic Search and Redis. 

{% page-ref page="create\_firehose.md" %}

### Deploying Firehose

Firehose can run inside VMs or containers in a fully managed runtime environment like Kubernetes. This section contains guides, best practices and advices related to deploying Firehose in production.

{% page-ref page="deployment.md" %}

### Monitoring Firehose with exposed metrics 

Always know what’s going on with your deployment with built-in [monitoring](https://github.com/odpf/firehose/blob/main/docs/assets/firehose-grafana-dashboard.json) of throughput, response times, errors and more. This section contains guides, best practices and advices related to managing Firehose in production.

{% page-ref page="monitoring.md" %}

### Troubleshooting Firehose

Firehose scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops. This section contains guides, best practices and advices related to troubleshooting issues with Firehose in production.

{% page-ref page="manage.md" %}



