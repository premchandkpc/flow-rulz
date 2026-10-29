# Application Module
# Deploys FlowRulz API, Worker, and Console on EKS

# Kubernetes namespace for the application
resource "kubernetes_namespace" "app" {
  metadata {
    name = "${var.project_name}-${var.environment}"
    labels = {
      name        = "${var.project_name}-${var.environment}"
      environment = var.environment
    }
  }
}

# Service Account for IRSA
resource "kubernetes_service_account" "app" {
  metadata {
    name      = "flowrulz"
    namespace = kubernetes_namespace.app.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = var.irsa_role_arn
    }
  }
}

# ConfigMap for application configuration
resource "kubernetes_config_map" "app_config" {
  metadata {
    name      = "flowrulz-config"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    # Application Configuration
    APP_ENV                 = var.app_env
    LOG_LEVEL               = var.log_level
    HTTP_ADDR               = ":8080"
    SHUTDOWN_TIMEOUT        = "30s"
    FLOWRULZ_METRICS_ADDR   = ":9090"
    FLOWRULZ_CONSOLE_ORIGIN = var.console_origin

    # Database Configuration
    # DATABASE_URL holds credentials and comes from the Secret below, never
    # from this ConfigMap.
    STORAGE_BACKEND = "postgres"
    MIGRATIONS_DIR  = "/app/migrations"

    # NATS Configuration
    NATS_URL              = var.nats_url
    NATS_STREAM           = "flow-rulz"
    NATS_CONSUMER         = ""
    NATS_RULESETS         = ""
    NATS_ACK_WAIT_MS      = "30000"
    NATS_MAX_DELIVER      = "-1"
    NATS_STREAM_MAX_AGE   = "0"
    NATS_STREAM_MAX_BYTES = "-1"
    NATS_STREAM_MAX_MSGS  = "-1"

    # Redis Configuration
    # REDIS_PASSWORD comes from the Secret below.
    REDIS_ADDR = var.redis_addr
    REDIS_DB   = "0"

    # Worker Configuration
    NUM_SHARDS                 = tostring(var.num_shards)
    WORKER_REPLICAS            = tostring(var.worker_replicas)
    MAX_GLOBAL_IN_FLIGHT       = tostring(var.max_global_in_flight)
    FETCH_BATCH_SIZE           = tostring(var.fetch_batch_size)
    EFFECT_PUBLISH_INTERVAL_MS = "5000"
    EFFECT_PUBLISH_BATCH_SIZE  = "10"
    EVENT_RETRY_DELAY_MS       = "5000"
    MAX_PER_KEY_QUEUE          = "100"
    KEY_QUEUE_WORKERS          = "1"
    HOT_KEY_THRESHOLD          = "1000"
    HOT_KEY_CHECK_INTERVAL_MS  = "10000"
    BATCH_MODE                 = "none"
    BATCH_MAX                  = "100"
    BATCH_WINDOW_MS            = "30000"
    SCHEDULER_POLL_MS          = "5000"
    SCHEDULER_BATCH_SIZE       = "100"
    LEASE_TTL_MS               = tostring(var.lease_ttl_ms)
    LEASE_RENEWAL_INTERVAL_MS  = tostring(var.lease_renewal_interval_ms)

    # Authentication
    # JWT_SECRET and FLOWRULZ_TRUSTED_PROXY_TOKEN come from the Secret below.
    REQUIRE_AUTH    = tostring(var.require_auth)
    JWT_ISSUER      = "flowrulz"
    JWT_AUDIENCE    = "flowrulz-api"
    REQUIRED_SCOPES = ""
  }
}

# Secret for sensitive configuration
resource "kubernetes_secret" "app_secrets" {
  metadata {
    name      = "flowrulz-secrets"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    DATABASE_URL        = var.database_url
    REDIS_PASSWORD      = var.redis_password
    JWT_SECRET          = var.jwt_secret
    NATS_CREDS          = var.nats_creds
    TRUSTED_PROXY_TOKEN = var.trusted_proxy_token
  }

  type = "Opaque"
}

# Deployment: API Server
resource "kubernetes_deployment" "api" {
  metadata {
    name      = "flowrulz-api"
    namespace = kubernetes_namespace.app.metadata[0].name
    labels = {
      app         = "flowrulz-api"
      environment = var.environment
      component   = "api"
    }
  }

  spec {
    replicas = var.api_replicas
    selector {
      match_labels = {
        app = "flowrulz-api"
      }
    }

    template {
      metadata {
        labels = {
          app         = "flowrulz-api"
          environment = var.environment
          component   = "api"
        }
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9090"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        security_context {
          run_as_non_root = true
          run_as_user     = 1000
          fs_group        = 1000
        }

        container {
          name              = "api"
          image             = "${var.app_repository}/flowrulz-api:${var.app_image_tag}"
          image_pull_policy = "IfNotPresent"

          port {
            container_port = 8080
            name           = "http"
          }

          port {
            container_port = 9090
            name           = "metrics"
          }

          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }

          env_from {
            secret_ref {
              name = kubernetes_secret.app_secrets.metadata[0].name
            }
          }

          resources {
            requests = {
              cpu    = var.api_cpu_request
              memory = var.api_memory_request
            }
            limits = {
              cpu    = var.api_cpu_limit
              memory = var.api_memory_limit
            }
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 8080
            }
            initial_delay_seconds = 15
            period_seconds        = 10
            timeout_seconds       = 3
            failure_threshold     = 5
          }

          readiness_probe {
            http_get {
              path = "/ready"
              port = 8080
            }
            initial_delay_seconds = 5
            period_seconds        = 10
            timeout_seconds       = 3
            failure_threshold     = 3
          }

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = false
            capabilities {
              drop = ["ALL"]
            }
          }
        }

        # Affinity for HA
        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                label_selector {
                  match_expressions {
                    key      = "app"
                    operator = "In"
                    values   = ["flowrulz-api"]
                  }
                }
                topology_key = "kubernetes.io/hostname"
              }
            }
          }
        }

        toleration {
          key      = "workload"
          operator = "Equal"
          value    = "flow-rulz-workers"
          effect   = "NoSchedule"
        }
      }
    }
  }
}

# Service: API Server
resource "kubernetes_service" "api" {
  metadata {
    name      = "flowrulz-api"
    namespace = kubernetes_namespace.app.metadata[0].name
    labels = {
      app = "flowrulz-api"
    }
  }

  spec {
    selector = {
      app = "flowrulz-api"
    }
    port {
      name        = "http"
      port        = 80
      target_port = 8080
    }
    port {
      name        = "metrics"
      port        = 9090
      target_port = 9090
    }
    type = "ClusterIP"
  }
}

# Deployment: Worker
resource "kubernetes_deployment" "worker" {
  metadata {
    name      = "flowrulz-worker"
    namespace = kubernetes_namespace.app.metadata[0].name
    labels = {
      app         = "flowrulz-worker"
      environment = var.environment
      component   = "worker"
    }
  }

  spec {
    replicas = var.worker_replicas
    selector {
      match_labels = {
        app = "flowrulz-worker"
      }
    }

    template {
      metadata {
        labels = {
          app         = "flowrulz-worker"
          environment = var.environment
          component   = "worker"
        }
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9090"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        security_context {
          run_as_non_root = true
          run_as_user     = 1000
          fs_group        = 1000
        }

        container {
          name              = "worker"
          image             = "${var.app_repository}/flowrulz-worker:${var.app_image_tag}"
          image_pull_policy = "IfNotPresent"

          port {
            container_port = 9090
            name           = "metrics"
          }

          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }

          env_from {
            secret_ref {
              name = kubernetes_secret.app_secrets.metadata[0].name
            }
          }

          resources {
            requests = {
              cpu    = var.worker_cpu_request
              memory = var.worker_memory_request
            }
            limits = {
              cpu    = var.worker_cpu_limit
              memory = var.worker_memory_limit
            }
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 9090
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 5
          }

          readiness_probe {
            http_get {
              path = "/ready"
              port = 9090
            }
            initial_delay_seconds = 10
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 3
          }

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = false
            capabilities {
              drop = ["ALL"]
            }
          }
        }

        # Affinity for HA - prefer worker nodes
        affinity {
          node_affinity {
            required_during_scheduling_ignored_during_execution {
              node_selector_term {
                match_expressions {
                  key      = "workload"
                  operator = "In"
                  values   = ["flow-rulz-workers"]
                }
              }
            }
          }
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                label_selector {
                  match_expressions {
                    key      = "app"
                    operator = "In"
                    values   = ["flowrulz-worker"]
                  }
                }
                topology_key = "kubernetes.io/hostname"
              }
            }
          }
        }

        toleration {
          key      = "workload"
          operator = "Equal"
          value    = "flow-rulz-workers"
          effect   = "NoSchedule"
        }
      }
    }
  }
}

# Service: Worker (for metrics)
resource "kubernetes_service" "worker" {
  metadata {
    name      = "flowrulz-worker"
    namespace = kubernetes_namespace.app.metadata[0].name
    labels = {
      app = "flowrulz-worker"
    }
  }

  spec {
    selector = {
      app = "flowrulz-worker"
    }
    port {
      name        = "metrics"
      port        = 9090
      target_port = 9090
    }
    type = "ClusterIP"
  }
}

# Deployment: Console (optional)
resource "kubernetes_deployment" "console" {
  count = var.enable_console ? 1 : 0

  metadata {
    name      = "flowrulz-console"
    namespace = kubernetes_namespace.app.metadata[0].name
    labels = {
      app         = "flowrulz-console"
      environment = var.environment
      component   = "console"
    }
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "flowrulz-console"
      }
    }

    template {
      metadata {
        labels = {
          app         = "flowrulz-console"
          environment = var.environment
          component   = "console"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        security_context {
          run_as_non_root = true
          run_as_user     = 1000
          fs_group        = 1000
        }

        container {
          name              = "console"
          image             = "${var.app_repository}/flowrulz-console:${var.app_image_tag}"
          image_pull_policy = "IfNotPresent"

          port {
            container_port = 8080
            name           = "http"
          }

          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }

          # Secrets were removed from the ConfigMap; the console gets them
          # here so its environment stays complete.
          env_from {
            secret_ref {
              name = kubernetes_secret.app_secrets.metadata[0].name
            }
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }

          liveness_probe {
            http_get {
              path = "/healthz"
              port = 8080
            }
            initial_delay_seconds = 10
            period_seconds        = 10
            timeout_seconds       = 3
            failure_threshold     = 5
          }

          readiness_probe {
            http_get {
              path = "/healthz"
              port = 8080
            }
            initial_delay_seconds = 5
            period_seconds        = 10
            timeout_seconds       = 3
            failure_threshold     = 3
          }

          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = false
            capabilities {
              drop = ["ALL"]
            }
          }
        }

        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                label_selector {
                  match_expressions {
                    key      = "app"
                    operator = "In"
                    values   = ["flowrulz-console"]
                  }
                }
                topology_key = "kubernetes.io/hostname"
              }
            }
          }
        }
      }
    }
  }
}

# Service: Console
resource "kubernetes_service" "console" {
  count = var.enable_console ? 1 : 0

  metadata {
    name      = "flowrulz-console"
    namespace = kubernetes_namespace.app.metadata[0].name
    labels = {
      app = "flowrulz-console"
    }
  }

  spec {
    selector = {
      app = "flowrulz-console"
    }
    port {
      name        = "http"
      port        = 80
      target_port = 8080
    }
    type = "ClusterIP"
  }
}

# Horizontal Pod Autoscaler for API
resource "kubernetes_horizontal_pod_autoscaler" "api" {
  metadata {
    name      = "flowrulz-api-hpa"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = kubernetes_deployment.api.metadata[0].name
    }
    min_replicas = var.api_min_replicas
    max_replicas = var.api_max_replicas

    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = var.api_target_cpu_utilization
        }
      }
    }

    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = var.api_target_memory_utilization
        }
      }
    }

    behavior {
      scale_down {
        stabilization_window_seconds = 300
        policy {
          type           = "Percent"
          value          = 10
          period_seconds = 60
        }
      }
      scale_up {
        stabilization_window_seconds = 0
        policy {
          type           = "Percent"
          value          = 100
          period_seconds = 15
        }
        policy {
          type           = "Pods"
          value          = 4
          period_seconds = 15
        }
        select_policy = "Max"
      }
    }
  }
}

# Horizontal Pod Autoscaler for Worker
resource "kubernetes_horizontal_pod_autoscaler" "worker" {
  metadata {
    name      = "flowrulz-worker-hpa"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = kubernetes_deployment.worker.metadata[0].name
    }
    min_replicas = var.worker_min_replicas
    max_replicas = var.worker_max_replicas

    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = var.worker_target_cpu_utilization
        }
      }
    }

    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = var.worker_target_memory_utilization
        }
      }
    }

    behavior {
      scale_down {
        stabilization_window_seconds = 300
        policy {
          type           = "Percent"
          value          = 10
          period_seconds = 60
        }
      }
      scale_up {
        stabilization_window_seconds = 0
        policy {
          type           = "Percent"
          value          = 100
          period_seconds = 15
        }
        policy {
          type           = "Pods"
          value          = 4
          period_seconds = 15
        }
        select_policy = "Max"
      }
    }
  }
}

# Pod Disruption Budget for API
resource "kubernetes_pod_disruption_budget" "api" {
  metadata {
    name      = "flowrulz-api-pdb"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    min_available = var.api_replicas > 1 ? 1 : 0
    selector {
      match_labels = {
        app = "flowrulz-api"
      }
    }
  }
}

# Pod Disruption Budget for Worker
resource "kubernetes_pod_disruption_budget" "worker" {
  metadata {
    name      = "flowrulz-worker-pdb"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    min_available = var.worker_replicas > 1 ? 1 : 0
    selector {
      match_labels = {
        app = "flowrulz-worker"
      }
    }
  }
}

# Network Policies (if enabled)
resource "kubernetes_network_policy" "api" {
  count = var.enable_network_policies ? 1 : 0

  metadata {
    name      = "flowrulz-api-netpol"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        app = "flowrulz-api"
      }
    }
    policy_types = ["Ingress", "Egress"]

    ingress {
      from {
        namespace_selector {
          match_labels = {
            name = "ingress-nginx"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 8080
      }
    }

    ingress {
      from {
        pod_selector {
          match_labels = {
            app = "flowrulz-console"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 8080
      }
    }

    ingress {
      from {
        pod_selector {
          match_labels = {
            app = "prometheus"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 9090
      }
    }

    egress {
      to {
        namespace_selector {
          match_labels = {
            name = kubernetes_namespace.app.metadata[0].name
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 5432 # PostgreSQL
      }
      ports {
        protocol = "TCP"
        port     = 4222 # NATS
      }
      ports {
        protocol = "TCP"
        port     = 6379 # Redis
      }
    }

    egress {
      to {
        namespace_selector {}
      }
      ports {
        protocol = "TCP"
        port     = 53 # DNS
      }
      ports {
        protocol = "UDP"
        port     = 53 # DNS
      }
    }
  }
}

resource "kubernetes_network_policy" "worker" {
  count = var.enable_network_policies ? 1 : 0

  metadata {
    name      = "flowrulz-worker-netpol"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        app = "flowrulz-worker"
      }
    }
    policy_types = ["Ingress", "Egress"]

    ingress {
      from {
        pod_selector {
          match_labels = {
            app = "prometheus"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 9090
      }
    }

    egress {
      to {
        namespace_selector {
          match_labels = {
            name = kubernetes_namespace.app.metadata[0].name
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 5432 # PostgreSQL
      }
      ports {
        protocol = "TCP"
        port     = 4222 # NATS
      }
      ports {
        protocol = "TCP"
        port     = 6379 # Redis
      }
    }

    egress {
      to {
        namespace_selector {}
      }
      ports {
        protocol = "TCP"
        port     = 53 # DNS
      }
      ports {
        protocol = "UDP"
        port     = 53 # DNS
      }
    }
  }
}

resource "kubernetes_network_policy" "console" {
  count = var.enable_network_policies && var.enable_console ? 1 : 0

  metadata {
    name      = "flowrulz-console-netpol"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        app = "flowrulz-console"
      }
    }
    policy_types = ["Ingress", "Egress"]

    ingress {
      from {
        namespace_selector {
          match_labels = {
            name = "ingress-nginx"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 8080
      }
    }

    egress {
      to {
        pod_selector {
          match_labels = {
            app = "flowrulz-api"
          }
        }
      }
      ports {
        protocol = "TCP"
        port     = 8080
      }
    }

    egress {
      to {
        namespace_selector {}
      }
      ports {
        protocol = "TCP"
        port     = 53 # DNS
      }
      ports {
        protocol = "UDP"
        port     = 53 # DNS
      }
    }
  }
}