# Prometheus Stack Values Template

# Prometheus Configuration
prometheus:
  prometheusSpec:
    # Retention period
    retention: "${retention}"
    # Storage
    storageSpec:
      volumeClaimTemplate:
        spec:
          storageClassName: "${storage_class}"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "${storage_size}"
    # Resources
    resources:
      requests:
        cpu: "500m"
        memory: "2Gi"
      limits:
        cpu: "2000m"
        memory: "4Gi"
    # Additional scrape configs
    additionalScrapeConfigs: []
    # Enable admin API
    enableAdminAPI: true
    # External labels
    externalLabels:
      cluster: "${cluster_name}"
      environment: "${environment}"

# Alertmanager Configuration
alertmanager:
  alertmanagerSpec:
    storage:
      volumeClaimTemplate:
        spec:
          storageClassName: "${storage_class}"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
    replicas: 2
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"

# Grafana Configuration
grafana:
  enabled: ${enable_grafana}
  adminUser: "admin"
  adminPassword: "${grafana_admin_password}"
  persistence:
    enabled: true
    storageClassName: "${storage_class}"
    accessModes: ["ReadWriteOnce"]
    size: "10Gi"
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"
  ingress:
    enabled: true
    ingressClassName: "alb"
    annotations:
      alb.ingress.kubernetes.io/scheme: "internet-facing"
      alb.ingress.kubernetes.io/target-type: "ip"
      alb.ingress.kubernetes.io/certificate-arn: ""
      alb.ingress.kubernetes.io/listen-ports: '[{"HTTP": 80}, {"HTTPS": 443}]'
      alb.ingress.kubernetes.io/ssl-redirect: "true"
    hosts:
      - "${grafana_domain}"
    path: /
  grafana.ini:
    server:
      domain: "${grafana_domain}"
      root_url: "https://${grafana_domain}"
    security:
      cookie_secure: true
      cookie_samesite: "strict"
    auth:
      disable_login_form: false
      disable_signout_menu: false
    users:
      viewers_can_edit: false
  # Custom dashboards are not embedded here: they are provisioned by the
  # flowrulz-dashboard ConfigMap, which the Grafana dashboards sidecar picks
  # up. The previous file() call was evaluated inside templatefile(), where
  # path.module is not available.

# Prometheus Operator
prometheusOperator:
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "512Mi"

# kube-state-metrics
kubeStateMetrics:
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "256Mi"

# node-exporter
nodeExporter:
  resources:
    requests:
      cpu: "100m"
      memory: "128Mi"
    limits:
      cpu: "500m"
      memory: "256Mi"

# kubelet
kubelet:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: "30s"

# CoreDNS
coreDns:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: "30s"

# etcd
etcd:
  enabled: false

# kubeControllerManager
kubeControllerManager:
  enabled: false

# kubeScheduler
kubeScheduler:
  enabled: false

# kubeProxy
kubeProxy:
  enabled: false