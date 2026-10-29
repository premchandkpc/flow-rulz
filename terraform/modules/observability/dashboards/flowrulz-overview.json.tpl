{
  "dashboard": {
    "title": "FlowRulz Overview",
    "tags": ["flowrulz", "overview"],
    "timezone": "utc",
    "panels": [
      {
        "id": 1,
        "title": "API Request Rate",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(rate(http_requests_total{job=\"flowrulz-api\"}[5m])) by (method, path, code)",
            "legendFormat": "{{method}} {{path}} {{code}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "reqps",
            "label": "Requests/sec"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 2,
        "title": "API Latency (p50, p95, p99)",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{job=\"flowrulz-api\"}[5m])) by (le))",
            "legendFormat": "p50",
            "refId": "A"
          },
          {
            "expr": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job=\"flowrulz-api\"}[5m])) by (le))",
            "legendFormat": "p95",
            "refId": "B"
          },
          {
            "expr": "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{job=\"flowrulz-api\"}[5m])) by (le))",
            "legendFormat": "p99",
            "refId": "C"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "s",
            "label": "Latency"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 3,
        "title": "Worker Event Processing Rate",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(rate(flowrulz_events_processed_total[5m])) by (status)",
            "legendFormat": "{{status}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "eps",
            "label": "Events/sec"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 4,
        "title": "Worker Queue Lag",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "flowrulz_worker_queue_lag",
            "legendFormat": "Queue Lag",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "short",
            "label": "Lag"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 5,
        "title": "NATS JetStream Consumer Lag",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "nats_jetstream_consumer_pending_messages",
            "legendFormat": "{{consumer}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "short",
            "label": "Pending Messages"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 6,
        "title": "NATS JetStream Stream Size",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "nats_jetstream_stream_messages",
            "legendFormat": "{{stream}} - Messages",
            "refId": "A"
          },
          {
            "expr": "nats_jetstream_stream_bytes",
            "legendFormat": "{{stream}} - Bytes",
            "refId": "B"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "short",
            "label": "Count/Bytes"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 7,
        "title": "Database Connections",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "pg_stat_database_numbackends",
            "legendFormat": "Active Connections",
            "refId": "A"
          },
          {
            "expr": "pg_settings_max_connections",
            "legendFormat": "Max Connections",
            "refId": "B"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "short",
            "label": "Connections"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 8,
        "title": "Database Query Duration",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, sum(rate(pg_stat_statements_duration_seconds_bucket[5m])) by (le, query))",
            "legendFormat": "p95 - {{query}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "s",
            "label": "Duration"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 9,
        "title": "Redis Memory Usage",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "redis_memory_used_bytes",
            "legendFormat": "Used Memory",
            "refId": "A"
          },
          {
            "expr": "redis_memory_max_bytes",
            "legendFormat": "Max Memory",
            "refId": "B"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "bytes",
            "label": "Memory"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 10,
        "title": "Redis Commands/sec",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(rate(redis_commands_total[5m])) by (command)",
            "legendFormat": "{{command}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "ops",
            "label": "Commands/sec"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 11,
        "title": "Pod CPU Usage",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(rate(container_cpu_usage_seconds_total{namespace=\"${cluster_name}-${environment}\",container!=\"\"}[5m])) by (pod, container)",
            "legendFormat": "{{pod}} - {{container}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "cores",
            "label": "CPU Cores"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 12,
        "title": "Pod Memory Usage",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(container_memory_working_set_bytes{namespace=\"${cluster_name}-${environment}\",container!=\"\"}) by (pod, container)",
            "legendFormat": "{{pod}} - {{container}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "bytes",
            "label": "Memory"
          },
          {
            "format": "short"
          }
        ]
      },
      {
        "id": 13,
        "title": "Pod Restart Count",
        "type": "stat",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(kube_pod_container_status_restarts_total{namespace=\"${cluster_name}-${environment}\"}) by (pod)",
            "legendFormat": "{{pod}}",
            "refId": "A"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "short"
          }
        }
      },
      {
        "id": 14,
        "title": "HPA Status",
        "type": "table",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "kube_horizontalpodautoscaler_status_desired_replicas{namespace=\"${cluster_name}-${environment}\"}",
            "legendFormat": "Desired - {{horizontalpodautoscaler}}",
            "refId": "A"
          },
          {
            "expr": "kube_horizontalpodautoscaler_status_current_replicas{namespace=\"${cluster_name}-${environment}\"}",
            "legendFormat": "Current - {{horizontalpodautoscaler}}",
            "refId": "B"
          }
        ]
      },
      {
        "id": 15,
        "title": "Error Rate by Type",
        "type": "graph",
        "datasource": "Prometheus",
        "targets": [
          {
            "expr": "sum(rate(flowrulz_errors_total[5m])) by (type, component)",
            "legendFormat": "{{component}} - {{type}}",
            "refId": "A"
          }
        ],
        "xaxis": {
          "mode": "time"
        },
        "yaxes": [
          {
            "format": "eps",
            "label": "Errors/sec"
          },
          {
            "format": "short"
          }
        ]
      }
    ],
    "time": {
      "from": "now-1h",
      "to": "now"
    },
    "refresh": "30s",
    "version": 1
  },
  "overwrite": true
}