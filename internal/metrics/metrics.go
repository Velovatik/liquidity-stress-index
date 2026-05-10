package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"method", "path", "code"})

	httpRequestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	etlRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "etl_runs_total",
		Help: "Total number of ETL runs.",
	}, []string{"result"})

	etlRunDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "etl_run_duration_seconds",
		Help:    "ETL run duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"result"})

	etlLastSuccessUnix = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etl_last_success_unix",
		Help: "Unix timestamp of the last successful ETL run.",
	})

	etlLastRunUnix = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "etl_last_run_unix",
		Help: "Unix timestamp of the last finished ETL run (success or failure).",
	})
)

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(statusCode int) {
	w.code = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// InstrumentHandler records request count + latency.
func InstrumentHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)

		path := r.URL.Path
		httpRequestsTotal.WithLabelValues(r.Method, path, strconv.Itoa(sw.code)).Inc()
		httpRequestDurationSeconds.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
	})
}

func ObserveETLSuccess(d time.Duration) {
	etlRunsTotal.WithLabelValues("success").Inc()
	etlRunDurationSeconds.WithLabelValues("success").Observe(d.Seconds())
	now := float64(time.Now().UTC().Unix())
	etlLastRunUnix.Set(now)
	etlLastSuccessUnix.Set(now)
}

func ObserveETLError(d time.Duration) {
	etlRunsTotal.WithLabelValues("error").Inc()
	etlRunDurationSeconds.WithLabelValues("error").Observe(d.Seconds())
	etlLastRunUnix.Set(float64(time.Now().UTC().Unix()))
}

