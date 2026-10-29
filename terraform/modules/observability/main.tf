# Observability Module
# Deploys Prometheus, Grafana, Loki, and Promtail on EKS

# Kubernetes namespace for monitoring
resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
    labels = {
      name = "monitoring"
    }
  }
}

# Kubernetes namespace for logging
resource "kubernetes_namespace" "logging" {
  metadata {
    name = "logging"
    labels = {
      name = "logging"
    }
  }
}

# Prometheus Stack (kube-prometheus-stack)
resource "helm_release" "prometheus" {
  count = var.enable_prometheus ? 1 : 0

  name       = "prometheus"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = var.prometheus_chart_version
  namespace  = kubernetes_namespace.monitoring.metadata[0].name

  values = [
    templatefile("${path.module}/prometheus-values.yaml.tpl", {
      retention              = var.prometheus_retention
      storage_size           = var.prometheus_storage_size
      storage_class          = var.prometheus_storage_class
      enable_grafana         = var.enable_grafana
      grafana_admin_password = var.grafana_admin_password
      grafana_domain         = var.grafana_domain
      cluster_name           = var.cluster_name
      environment            = var.environment
    })
  ]
}

# Loki Stack
resource "helm_release" "loki" {
  count = var.enable_loki ? 1 : 0

  name       = "loki"
  repository = "https://grafana.github.io/helm-charts"
  chart      = "loki-stack"
  version    = var.loki_chart_version
  namespace  = kubernetes_namespace.logging.metadata[0].name

  values = [
    templatefile("${path.module}/loki-values.yaml.tpl", {
      retention     = var.loki_retention
      storage_size  = var.loki_storage_size
      storage_class = var.loki_storage_class
      cluster_name  = var.cluster_name
      environment   = var.environment
    })
  ]
}

# ServiceMonitor for FlowRulz API
# Requires the ServiceMonitor/PodMonitor CRDs registered by the Prometheus
# stack, so it only exists when that stack is installed.
resource "kubernetes_manifest" "api_servicemonitor" {
  count = var.enable_prometheus ? 1 : 0

  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "flowrulz-api"
      namespace = var.app_namespace
      labels = {
        release = "prometheus"
      }
    }
    spec = {
      selector = {
        matchLabels = {
          app = "flowrulz-api"
        }
      }
      endpoints = [
        {
          port     = "metrics"
          path     = "/metrics"
          interval = "30s"
        }
      ]
      namespaceSelector = {
        matchNames = [var.app_namespace]
      }
    }
  }

  depends_on = [helm_release.prometheus]
}

# ServiceMonitor for FlowRulz Worker
resource "kubernetes_manifest" "worker_servicemonitor" {
  count = var.enable_prometheus ? 1 : 0

  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "flowrulz-worker"
      namespace = var.app_namespace
      labels = {
        release = "prometheus"
      }
    }
    spec = {
      selector = {
        matchLabels = {
          app = "flowrulz-worker"
        }
      }
      endpoints = [
        {
          port     = "metrics"
          path     = "/metrics"
          interval = "30s"
        }
      ]
      namespaceSelector = {
        matchNames = [var.app_namespace]
      }
    }
  }

  depends_on = [helm_release.prometheus]
}

# PodMonitor for NATS (if deployed in cluster)
resource "kubernetes_manifest" "nats_podmonitor" {
  count = var.enable_prometheus && var.enable_nats_monitoring ? 1 : 0

  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "PodMonitor"
    metadata = {
      name      = "nats"
      namespace = "nats"
      labels = {
        release = "prometheus"
      }
    }
    spec = {
      selector = {
        matchLabels = {
          app = "nats"
        }
      }
      podMetricsEndpoints = [
        {
          port     = "metrics"
          path     = "/metrics"
          interval = "30s"
        }
      ]
      namespaceSelector = {
        matchNames = ["nats"]
      }
    }
  }

  depends_on = [helm_release.prometheus]
}

# PrometheusRule for FlowRulz alerts (PrometheusRule CRD from the stack)
resource "kubernetes_manifest" "flowrulz_alerts" {
  count = var.enable_prometheus ? 1 : 0

  depends_on = [helm_release.prometheus]

  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "PrometheusRule"
    metadata = {
      name      = "flowrulz-alerts"
      namespace = kubernetes_namespace.monitoring.metadata[0].name
      labels = {
        release = "prometheus"
      }
    }
    spec = {
      groups = [
        {
          name = "flowrulz.rules"
          rules = [
            {
              alert = "FlowRulzAPIDown"
              expr  = "up{job=\"flowrulz-api\"} == 0"
              for   = "2m"
              labels = {
                severity = "critical"
              }
              annotations = {
                summary     = "FlowRulz API is down"
                description = "FlowRulz API has been down for more than 2 minutes."
              }
            },
            {
              alert = "FlowRulzWorkerDown"
              expr  = "up{job=\"flowrulz-worker\"} == 0"
              for   = "2m"
              labels = {
                severity = "critical"
              }
              annotations = {
                summary     = "FlowRulz Worker is down"
                description = "FlowRulz Worker has been down for more than 2 minutes."
              }
            },
            {
              alert = "FlowRulzAPIHighErrorRate"
              expr  = "rate(http_requests_total{job=\"flowrulz-api\",code=~\"5..\"}[5m]) > 0.05"
              for   = "5m"
              labels = {
                severity = "warning"
              }
              annotations = {
                summary     = "FlowRulz API high error rate"
                description = "FlowRulz API error rate is above 5% for 5 minutes."
              }
            },
            {
              alert = "FlowRulzWorkerHighQueueLag"
              expr  = "flowrulz_worker_queue_lag > 1000"
              for   = "5m"
              labels = {
                severity = "warning"
              }
              annotations = {
                summary     = "FlowRulz Worker high queue lag"
                description = "FlowRulz Worker queue lag is above 1000 for 5 minutes."
              }
            },
            {
              alert = "FlowRulzNATSJetStreamLag"
              expr  = "nats_jetstream_consumer_pending_messages > 10000"
              for   = "5m"
              labels = {
                severity = "warning"
              }
              annotations = {
                summary     = "NATS JetStream consumer lag"
                description = "NATS JetStream consumer has more than 10000 pending messages."
              }
            },
            {
              alert = "FlowRulzDatabaseConnectionsHigh"
              expr  = "pg_stat_database_numbackends / pg_settings_max_connections > 0.8"
              for   = "5m"
              labels = {
                severity = "warning"
              }
              annotations = {
                summary     = "Database connections high"
                description = "Database connections are above 80% of max connections."
              }
            },
            {
              alert = "FlowRulzRedisMemoryHigh"
              expr  = "redis_memory_used_bytes / redis_memory_max_bytes > 0.85"
              for   = "5m"
              labels = {
                severity = "warning"
              }
              annotations = {
                summary     = "Redis memory usage high"
                description = "Redis memory usage is above 85%."
              }
            }
          ]
        }
      ]
    }
  }
}

# Grafana Dashboard ConfigMap for FlowRulz
resource "kubernetes_config_map" "grafana_dashboard" {
  count = var.enable_grafana ? 1 : 0

  metadata {
    name      = "flowrulz-dashboard"
    namespace = kubernetes_namespace.monitoring.metadata[0].name
    labels = {
      grafana_dashboard = "1"
    }
  }

  data = {
    "flowrulz-overview.json" = templatefile("${path.module}/dashboards/flowrulz-overview.json.tpl", {
      cluster_name = var.cluster_name
      environment  = var.environment
    })
  }
}