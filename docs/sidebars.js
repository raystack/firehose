module.exports = {
  docsSidebar: [
    'introduction',
    {
      type: "category",
      label: "Guides",
      items: [
        "guides/create_firehose",
        "guides/json-based-filters",
        "guides/jexl-based-filters",
        "guides/deployment",
        "guides/manage",
      ],
    },
    {
      type: "category",
      label: "Concepts",
      items: [
        "concepts/overview",
        "concepts/architecture",
        "concepts/filters",
        "concepts/templating",
        "concepts/consumer",
        "concepts/decorators",
        "concepts/offsets",
        "concepts/monitoring",
      ],
    },
    {
      type: "category",
      label: "Sinks",
      items: [
        "sinks/overview",
        "sinks/filters",
        "sinks/retries",
        "sinks/elasticsearch-sink",
        "sinks/grpc-sink",
        "sinks/http-sink",
        "sinks/mongo-sink",
        "sinks/influxdb-sink",
        "sinks/jdbc-sink",
        "sinks/prometheus-sink",
        "sinks/redis-sink",
        "sinks/blob-sink",
        "sinks/bigquery-sink",
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
        "reference/configuration",
        "reference/configurations",
        "reference/core-faqs",
        "reference/faq",
        "reference/glossary",
        "reference/metrics",
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
    'roadmap',
  ],
};