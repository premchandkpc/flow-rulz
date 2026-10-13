package observability

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config holds observability configuration.
type Config struct {
	ServiceName   string
	Environment   string
	OTLPEndpoint  string
	MetricsPort   int
	EnableTracing bool
}

// DefaultConfig returns default observability configuration.
func DefaultConfig() Config {
	port := 9090
	if p := os.Getenv("METRICS_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}

	return Config{
		ServiceName:   os.Getenv("SERVICE_NAME"),
		Environment:   os.Getenv("ENVIRONMENT"),
		OTLPEndpoint:  os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		MetricsPort:   port,
		EnableTracing: os.Getenv("ENABLE_TRACING") == "true",
	}
}

// Observability manages logging, metrics, and tracing.
type Observability struct {
	Config         Config
	Logger         *Logger
	MetricsMux     *http.ServeMux
	MetricsServer  *http.Server
	TracerProvider interface{ Shutdown(context.Context) error }
}

// New creates a new Observability instance.
func New(config Config) (*Observability, error) {
	if config.ServiceName == "" {
		config.ServiceName = "flowrulz"
	}
	if config.Environment == "" {
		config.Environment = "development"
	}

	o := &Observability{
		Config:     config,
		Logger:     NewLogger(config.ServiceName, config.Environment),
		MetricsMux: http.NewServeMux(),
	}

	o.MetricsMux.Handle("/metrics", promhttp.Handler())

	if config.EnableTracing && config.OTLPEndpoint != "" {
		ctx := context.Background()
		tp, err := InitTracer(ctx, config.ServiceName, config.OTLPEndpoint)
		if err != nil {
			o.Logger.Warn("failed to init tracer", "error", err)
		} else {
			o.TracerProvider = tp
			o.Logger.Info("tracing enabled", "endpoint", config.OTLPEndpoint)
		}
	}

	if config.MetricsPort > 0 {
		o.MetricsServer = &http.Server{
			Addr:         ":" + strconv.Itoa(config.MetricsPort),
			Handler:      o.MetricsMux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}

		go func() {
			o.Logger.Info("metrics server starting", "port", config.MetricsPort)
			if err := o.MetricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				o.Logger.Error("metrics server error", "error", err)
			}
		}()
	}

	return o, nil
}

// Shutdown gracefully stops the observability components.
func (o *Observability) Shutdown(ctx context.Context) error {
	if o.MetricsServer != nil {
		if err := o.MetricsServer.Shutdown(ctx); err != nil {
			o.Logger.Error("shutdown metrics server", "error", err)
			return err
		}
	}

	if o.TracerProvider != nil {
		if err := o.TracerProvider.Shutdown(ctx); err != nil {
			o.Logger.Error("shutdown tracer", "error", err)
			return err
		}
	}

	return nil
}
