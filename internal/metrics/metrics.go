package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shirou/gopsutil/v4/process"
	hostinstrumentation "go.opentelemetry.io/contrib/instrumentation/host"
	runtimeinstrumentation "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

const scopeName = "github.com/sanjuthomas/log-forwarder"

// Collector records forwarder metrics via OpenTelemetry.
type Collector struct {
	linesRead            metric.Int64Counter
	linesPublished       metric.Int64Counter
	linesSkipped         metric.Int64Counter
	linesFiltered        metric.Int64Counter
	bufferDropped        metric.Int64Counter
	transformErrors      metric.Int64Counter
	publishFailures      metric.Int64Counter
	publishRetries       metric.Int64Counter
	publishTruncations   metric.Int64Counter
	publishDuration      metric.Float64Histogram

	provider *sdkmetric.MeterProvider
	server   *http.Server
	registry *promclient.Registry
	proc     *process.Process
}

type Snapshot struct {
	FilesWatched     func() int64
	BufferDepth      func() int64
	BufferCapacity   int64
}

// New creates a metrics collector and HTTP server when metrics are enabled.
// When disabled, it returns a no-op collector and nil shutdown function.
func New(cfg config.MetricsConfig, snapshot Snapshot, readiness *Readiness) (*Collector, func(context.Context) error, error) {
	if !cfg.Enabled {
		return noopCollector()
	}

	registry := promclient.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("create prometheus exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("log-forwarder"),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	if err := runtimeinstrumentation.Start(
		runtimeinstrumentation.WithMeterProvider(provider),
		runtimeinstrumentation.WithMinimumReadMemStatsInterval(time.Second),
	); err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, fmt.Errorf("start runtime instrumentation: %w", err)
	}

	if err := hostinstrumentation.Start(hostinstrumentation.WithMeterProvider(provider)); err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, fmt.Errorf("start host instrumentation: %w", err)
	}

	meter := provider.Meter(scopeName)
	collector, err := newInstruments(meter, snapshot)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, err
	}
	collector.provider = provider
	collector.registry = registry

	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, fmt.Errorf("open process stats: %w", err)
	}
	collector.proc = proc

	if err := registerProcessMemoryGauge(meter, proc); err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	if readiness != nil {
		mux.HandleFunc(cfg.Readiness.ReadyPath(), readiness.handler())
	}
	mux.Handle(cfg.MetricsPath(), promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	collector.server = &http.Server{
		Addr:              cfg.Addr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	shutdown := func(ctx context.Context) error {
		var shutdownErr error
		if collector.server != nil {
			shutdownErr = errors.Join(shutdownErr, collector.server.Shutdown(ctx))
		}
		if collector.provider != nil {
			shutdownErr = errors.Join(shutdownErr, collector.provider.Shutdown(ctx))
		}
		return shutdownErr
	}

	return collector, shutdown, nil
}

func newInstruments(meter metric.Meter, snapshot Snapshot) (*Collector, error) {
	linesRead, err := meter.Int64Counter(
		"log_forwarder.lines.read",
		metric.WithDescription("Total number of log lines read from watched files."),
		metric.WithUnit("{line}"),
	)
	if err != nil {
		return nil, err
	}

	linesPublished, err := meter.Int64Counter(
		"log_forwarder.lines.published",
		metric.WithDescription("Total number of log lines published to the configured sink."),
		metric.WithUnit("{line}"),
	)
	if err != nil {
		return nil, err
	}

	linesSkipped, err := meter.Int64Counter(
		"log_forwarder.lines.skipped",
		metric.WithDescription("Total number of log lines skipped due to transform errors."),
		metric.WithUnit("{line}"),
	)
	if err != nil {
		return nil, err
	}

	linesFiltered, err := meter.Int64Counter(
		"log_forwarder.lines.filtered",
		metric.WithDescription("Total number of log lines dropped by configured filters."),
		metric.WithUnit("{line}"),
	)
	if err != nil {
		return nil, err
	}

	bufferDropped, err := meter.Int64Counter(
		"log_forwarder.pipeline.buffer.dropped",
		metric.WithDescription("Total number of line events dropped because the pipeline buffer was full."),
		metric.WithUnit("{line}"),
	)
	if err != nil {
		return nil, err
	}

	transformErrors, err := meter.Int64Counter(
		"log_forwarder.transform.errors",
		metric.WithDescription("Total number of transform errors encountered."),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	publishFailures, err := meter.Int64Counter(
		"log_forwarder.publish.failures",
		metric.WithDescription("Total number of failed sink publish attempts."),
		metric.WithUnit("{attempt}"),
	)
	if err != nil {
		return nil, err
	}

	publishTruncations, err := meter.Int64Counter(
		"log_forwarder.publish.truncations",
		metric.WithDescription("Total number of records truncated to fit max publish size."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, err
	}

	publishRetries, err := meter.Int64Counter(
		"log_forwarder.publish.retries",
		metric.WithDescription("Total number of sink publish retries after a failure."),
		metric.WithUnit("{retry}"),
	)
	if err != nil {
		return nil, err
	}

	publishDuration, err := meter.Float64Histogram(
		"log_forwarder.publish.duration",
		metric.WithDescription("Sink publish latency."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	if _, err := meter.Int64ObservableGauge(
		"log_forwarder.files.watched",
		metric.WithDescription("Number of log files currently being tailed."),
		metric.WithUnit("{file}"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			if snapshot.FilesWatched != nil {
				observer.Observe(snapshot.FilesWatched())
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err := meter.Int64ObservableGauge(
		"log_forwarder.pipeline.buffer.depth",
		metric.WithDescription("Current number of line events buffered between watcher and pipeline."),
		metric.WithUnit("{event}"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			if snapshot.BufferDepth != nil {
				observer.Observe(snapshot.BufferDepth())
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if _, err := meter.Int64ObservableUpDownCounter(
		"log_forwarder.pipeline.buffer.capacity",
		metric.WithDescription("Configured pipeline buffer capacity."),
		metric.WithUnit("{event}"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			observer.Observe(snapshot.BufferCapacity)
			return nil
		}),
	); err != nil {
		return nil, err
	}

	return &Collector{
		linesRead:            linesRead,
		linesPublished:       linesPublished,
		linesSkipped:         linesSkipped,
		linesFiltered:        linesFiltered,
		bufferDropped:        bufferDropped,
		transformErrors:      transformErrors,
		publishFailures:      publishFailures,
		publishRetries:       publishRetries,
		publishTruncations:   publishTruncations,
		publishDuration:      publishDuration,
	}, nil
}

func registerProcessMemoryGauge(meter metric.Meter, proc *process.Process) error {
	_, err := meter.Int64ObservableGauge(
		"process.memory.usage",
		metric.WithDescription("Amount of physical memory used by the forwarder process."),
		metric.WithUnit("By"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			mem, err := proc.MemoryInfo()
			if err != nil {
				return nil
			}
			observer.Observe(int64(mem.RSS))
			return nil
		}),
	)
	return err
}

// PrometheusHandler returns the Prometheus scrape handler for this collector.
func (c *Collector) PrometheusHandler() http.Handler {
	if c == nil || c.registry == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

func (c *Collector) Start(logger *slog.Logger) error {
	if c == nil || c.server == nil {
		return nil
	}

	go func() {
		logger.Info("metrics server started", "addr", c.server.Addr)
		if err := c.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped", "error", err)
		}
	}()
	return nil
}

func (c *Collector) RecordLineRead(ctx context.Context, count int64) {
	if c == nil || c.linesRead == nil {
		return
	}
	c.linesRead.Add(ctx, count)
}

func (c *Collector) RecordLinePublished(ctx context.Context) {
	if c == nil || c.linesPublished == nil {
		return
	}
	c.linesPublished.Add(ctx, 1)
}

func (c *Collector) RecordLineSkipped(ctx context.Context) {
	if c == nil || c.linesSkipped == nil {
		return
	}
	c.linesSkipped.Add(ctx, 1)
}

func (c *Collector) RecordLineFiltered(ctx context.Context) {
	if c == nil || c.linesFiltered == nil {
		return
	}
	c.linesFiltered.Add(ctx, 1)
}

func (c *Collector) RecordLineBufferDropped(ctx context.Context) {
	if c == nil || c.bufferDropped == nil {
		return
	}
	c.bufferDropped.Add(ctx, 1)
}

func (c *Collector) RecordTransformError(ctx context.Context) {
	if c == nil || c.transformErrors == nil {
		return
	}
	c.transformErrors.Add(ctx, 1)
}

func (c *Collector) RecordPublishFailure(ctx context.Context) {
	if c == nil || c.publishFailures == nil {
		return
	}
	c.publishFailures.Add(ctx, 1)
}

func (c *Collector) RecordPublishRetry(ctx context.Context) {
	if c == nil || c.publishRetries == nil {
		return
	}
	c.publishRetries.Add(ctx, 1)
}

func (c *Collector) RecordPublishTruncation(ctx context.Context) {
	if c == nil || c.publishTruncations == nil {
		return
	}
	c.publishTruncations.Add(ctx, 1)
}

func (c *Collector) RecordPublishDuration(ctx context.Context, duration time.Duration) {
	if c == nil || c.publishDuration == nil {
		return
	}
	c.publishDuration.Record(ctx, duration.Seconds())
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}

func noopCollector() (*Collector, func(context.Context) error, error) {
	return &Collector{}, func(context.Context) error { return nil }, nil
}
