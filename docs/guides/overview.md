# Overview

The following topics will describe how to manage Firehose throughout its lifecycle.

## Creating Firehose

Firehose is designed to work with different sinks. Each deployment of Firehose can only have one sink. Currently supported sinks by Firehose are Log, HTTP, JDBC, InfluxDB, Prometheus, GRPC, Elastic Search and Redis.

{% page-ref page="create\_firehose.md" %}

## Using Filters in Firehose

 Use the Filter feature provided in Firehose which allows you to apply any filters on the fields present in the data set and helps you narrow it down to your use case-specific data.

{% page-ref page="filters.md" %}

## Deploying Firehose

Firehose can run inside VMs or containers in a fully managed runtime environment like Kubernetes. This section contains guides, best practices and advises related to deploying Firehose in production.

{% page-ref page="deployment.md" %}

## Troubleshooting Firehose

Firehose scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops. This section contains guides, best practices and advises related to troubleshooting issues with Firehose in production.

{% page-ref page="manage.md" %}

