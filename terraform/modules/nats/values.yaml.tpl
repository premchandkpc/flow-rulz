# NATS Helm Values Template
# This template is rendered with Terraform variables

# NATS Server Configuration
nats:
  # JetStream Configuration
  jetstream:
    enabled: ${jetstream_enabled}
    # Memory storage limit for JetStream
    memStorage: "${jetstream_mem_storage}"
    # Disk storage limit for JetStream
    fileStorage: "${jetstream_disk_storage}"
    # Max memory per stream
    maxMemory: "${jetstream_mem_storage}"
    # Max disk per stream
    maxFileStorage: "${jetstream_disk_storage}"
    # Store directory
    storeDir: "/data/jetstream"

  # NATS Server Configuration
  server:
    # NATS version
    version: "${nats_version}"
    # Image repository
    image: "${image_repository}:${image_tag}"
    # Image pull policy
    imagePullPolicy: "IfNotPresent"
    # Resource requests/limits
    resources:
      requests:
        cpu: "500m"
        memory: "1Gi"
      limits:
        cpu: "1000m"
        memory: "2Gi"
    # JVM options for JetStream
    jvm: "-Xms512m -Xmx1g"
    # Config file
    config: |
      listen: 0.0.0.0:4222
      http: 0.0.0.0:8222
      jetstream {
        max_mem: ${jetstream_mem_storage}
        max_file: ${jetstream_disk_storage}
        store_dir: "/data/jetstream"
      }
      # Logging
      log_level: "INFO"
      log_file: "/var/log/nats/nats.log"
      # Authorization (optional - can be enabled for production)
      # authorization {
      #   user: "nats"
      #   password: "changeme"
      #   timeout: 2
      # }

  # Cluster Configuration
  cluster:
    enabled: true
    # Number of NATS servers in the cluster
    replicas: ${cluster_size}
    # Pod disruption budget
    podDisruptionBudget:
      minAvailable: 1
    # Anti-affinity for HA
    affinity:
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                  - key: "app.kubernetes.io/name"
                    operator: In
                    values:
                      - nats
              topologyKey: kubernetes.io/hostname

  # Persistence Configuration
  persistence:
    enabled: true
    # Storage class for JetStream data
    storageClass: "gp3"
    # Size of persistent volume per NATS pod
    size: "${jetstream_disk_storage}"
    # Access mode
    accessMode: "ReadWriteOnce"

  # Service Configuration
  service:
    # ClusterIP service for internal access
    type: ClusterIP
    # Ports
    ports:
      client: 4222
      monitor: 8222
      cluster: 6222
      leafnode: 7422
      gateways: 7522

  # LoadBalancer service for external access (optional)
  loadbalancer:
    enabled: false
    # If enabled, configure annotations for AWS NLB
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
      service.beta.kubernetes.io/aws-load-balancer-scheme: "internal"
      service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: "true"

  # Monitoring
  monitoring:
    enabled: true
    # Prometheus metrics
    prometheus:
      enabled: true
      port: 7777
      path: "/metrics"
    # Grafana dashboards
    grafana:
      enabled: true
      dashboards:
        enabled: true

  # TLS Configuration (optional - for production)
  tls:
    enabled: false
    # certManager:
    #   enabled: true
    #   issuerRef:
    #     name: letsencrypt-prod
    #     kind: ClusterIssuer

# Additional configuration for production
global:
  # Enable debug logging
  debug: false
  # Log format
  logFormat: "json"

# Pod Security Context
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsGroup: 1000

# Container Security Context
containerSecurityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: false
  capabilities:
    drop:
      - ALL

# Network Policies (if enabled in cluster)
networkPolicy:
  enabled: false