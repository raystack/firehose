# Overview

The following topics will describe key concepts of Firehose.

## Architecture

Firehose is a fully managed service that allows you to consume messages to any sink from kafka streams in realtime at scale. This section explains the overall architecture of Firehose and describes its various components.

{% page-ref page="architecture.md" %}

## Monitoring Firehose with exposed metrics

Always know what’s going on with your deployment with built-in [monitoring](https://github.com/odpf/firehose/blob/main/docs/assets/firehose-grafana-dashboard.json) of throughput, response times, errors and more. This section contains guides, best practices and advises related to managing Firehose in production.

{% page-ref page="monitoring.md" %}

## Filters

Use the Filter feature provided in Firehose which allows you to apply any filters on the fields present in the key or message of the event data set and helps you narrow it down to your use case-specific data.

{% page-ref page="filters.md" %}

## Templating

Firehose provides various templating features

{% page-ref page="templating.md" %}

