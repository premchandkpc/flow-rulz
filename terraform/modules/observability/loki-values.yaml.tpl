# Loki Stack Values Template

loki:
  enabled: true
  # Loki configuration
  config: |
    auth_enabled: false
    server:
      http_listen_port: 3100
      grpc_listen_port: 9096
    common:
      path_prefix: /loki
      storage:
        filesystem:
          chunks_directory: /loki/chunks
          rules_directory: /loki/rules
      replication_factor: 1
      ring:
        instance_addr: 127.0.0.1
        kvstore:
          store: inmemory
    schema_config:
      configs:
        - from: 2020-10-24
          store: boltdb-shipper
          object_store: filesystem
          schema: v11
          index:
            prefix: index_
            period: 24h
    limits_config:
      reject_old_samples: true
      reject_old_samples_max_age: ${retention}
      ingestion_rate_mb: 20
      ingestion_burst_size_mb: 40
      per_stream_rate_limit: 10MB
      per_stream_rate_limit_burst: 20MB
    chunk_store_config:
      max_look_back_period: ${retention}
    table_manager:
      retention_deletes_enabled: true
      retention_period: ${retention}

  # Persistence
  persistence:
    enabled: true
    storageClassName: "${storage_class}"
    accessModes: ["ReadWriteOnce"]
    size: "${storage_size}"

  # Resources
  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2000m"
      memory: "4Gi"

  # Service
  service:
    type: ClusterIP
    port: 3100

  # Ingress
  ingress:
    enabled: false

# Promtail Configuration
promtail:
  enabled: true
  config:
    logLevel: info
    serverPort: 3101
    clients:
      - url: http://loki.monitoring.svc.cluster.local:3100/loki/api/v1/push
    positions:
      filename: /tmp/positions.yaml
    scrapeConfigs:
      - jobName: kubernetes-pods
        kubernetesSDConfigs:
          - role: pod
        relabelConfigs:
          - sourceLabels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
            action: keep
            regex: true
          - sourceLabels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
            action: replace
            targetLabel: __metrics_path__
            regex: (.+)
          - sourceLabels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
            action: replace
            regex: ([^:]+)(?::\d+)?;(\d+)
            replacement: $1:$2
            targetLabel: __address__
          - action: labelmap
            regex: __meta_kubernetes_pod_label_(.+)
          - sourceLabels: [__meta_kubernetes_namespace]
            action: replace
            targetLabel: kubernetes_namespace
          - sourceLabels: [__meta_kubernetes_pod_name]
            action: replace
            targetLabel: kubernetes_pod_name
          - sourceLabels: [__meta_kubernetes_pod_container_name]
            action: replace
            targetLabel: kubernetes_container_name
        pipelineStages:
          - docker: {}
          - cri: {}

  # Resources
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"

# Grafana (if enabled in Prometheus stack, this is supplementary)
grafana:
  enabled: false
  # Grafana is deployed via kube-prometheus-stack