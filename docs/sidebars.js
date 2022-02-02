module.exports = {
  docsSidebar: [
    'introduction',
    {
      type: "category",
      label: "Guides",
      items: [
        "guides/overview",
        "guides/filters/README",
        "guides/filters/json-based-filters",
        "guides/filters/jexl-based-filters",
        "guides/deployment",
        "guides/manage",
      ],
    },
    {
      type: "category",
      label: "Concepts",
      items: [
        "concepts/README",
        "concepts/architecture",
        "concepts/filters",
        "concepts/monitoring",
        "concepts/templating",
      ],
    },
    {
      type: "category",
      label: "Sink",
      items: [
        "sink/bigquery",
      ],
    },
    {
      type: "category",
      label: "Reference",
      items: [
        "reference/configuration/README",
        "reference/configuration/generic-1",
        "reference/configuration/kafka-consumer-1",
        "reference/configuration/filters",
        "reference/configuration/stencil-client",
        "reference/configuration/retries",
        "reference/configuration/elasticsearch-sink",
        "reference/configuration/grpc-sink",
        "reference/configuration/http-sink",
        "reference/configuration/mongo-sink",
        "reference/configuration/influxdb-sink",
        "reference/configuration/jdbc-sink",
        "reference/configuration/prometheus-sink",
        "reference/configuration/redis-sink",
        "reference/configuration/blob-sink",
        "reference/configuration/bigquery-sink",
      ],
    },
    {
      type: "category",
      label: "Contribute",
      items: [
        "contribute/contribution",
        "contribute/development",
      ],
    },
    'roadmap'
  ],
};