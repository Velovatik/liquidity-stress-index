package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"liquidity-stress-index/internal/storage"
)

type lsijson struct {
	LSI  float64 `json:"lsi"`
	Date string  `json:"date"`
}

type ETLState struct {
	mu        sync.RWMutex
	running   bool
	lastStart time.Time
	lastEnd   time.Time
	lastOK    bool
	lastErr   string
}

func (s *ETLState) SetRunning(start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = true
	s.lastStart = start
}

func (s *ETLState) SetSucceeded(end time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.lastEnd = end
	s.lastOK = true
	s.lastErr = ""
}

func (s *ETLState) SetFailed(end time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.lastEnd = end
	s.lastOK = false
	if err != nil {
		s.lastErr = err.Error()
	} else {
		s.lastErr = "unknown error"
	}
}

type etlStateJSON struct {
	Running   bool   `json:"running"`
	LastStart string `json:"last_start,omitempty"`
	LastEnd   string `json:"last_end,omitempty"`
	LastOK    *bool  `json:"last_ok,omitempty"`
	LastErr   string `json:"last_err,omitempty"`
}

func (s *ETLState) snapshot() etlStateJSON {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := etlStateJSON{Running: s.running}
	if !s.lastStart.IsZero() {
		out.LastStart = formatDateTime(s.lastStart)
	}
	if !s.lastEnd.IsZero() {
		out.LastEnd = formatDateTime(s.lastEnd)
	}
	if !s.lastStart.IsZero() || !s.lastEnd.IsZero() {
		v := s.lastOK
		out.LastOK = &v
	}
	if s.lastErr != "" {
		out.LastErr = s.lastErr
	}
	return out
}

func formatDateTime(ts time.Time) string {
	return ts.UTC().Format(time.RFC3339)
}

// NewMux регистрирует HTTP-эндпоинты демо на стандартном net/http.
func NewMux(db *storage.DB, etlState *ETLState) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /health", wrap(func(ctx context.Context, w http.ResponseWriter, _ *http.Request) error {
		pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		if err := db.Ping(pingCtx); err != nil {
			return fmt.Errorf("db ping: %w", err)
		}
		payload := map[string]any{
			"ok":  true,
			"db":  "ok",
			"etl": etlState.snapshot(),
		}
		writeJSON(w, http.StatusOK, payload)
		return nil
	}))

	mux.HandleFunc("GET /lsi/latest", wrap(func(ctx context.Context, w http.ResponseWriter, _ *http.Request) error {
		rec, err := db.LatestLSI(ctx)
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, lsijson{LSI: rec.LSI, Date: formatDate(rec.Date)})
		return nil
	}))

	mux.HandleFunc("GET /lsi/history", wrap(func(ctx context.Context, w http.ResponseWriter, _ *http.Request) error {
		const limit int32 = 30
		rows, err := db.HistoryLSI(ctx, limit)
		if err != nil {
			return err
		}
		payload := make([]lsijson, len(rows))
		for i := range rows {
			payload[i] = lsijson{LSI: rows[i].LSI, Date: formatDate(rows[i].Date)}
		}
		writeJSON(w, http.StatusOK, payload)
		return nil
	}))

	return mux
}

func formatDate(ts time.Time) string {
	return ts.UTC().Truncate(24 * time.Hour).Format(time.DateOnly)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("ошибка кодирования JSON: %v", err)
	}
}

type handlerFn func(context.Context, http.ResponseWriter, *http.Request) error

func wrap(h handlerFn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := h(r.Context(), w, r)
		if err == nil {
			return
		}

		switch {
		case errors.Is(err, pgx.ErrNoRows):
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "метрики LSI ещё не загружены — сначала выполните ETL",
			})
		default:
			log.Printf("ошибка обработчика: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "внутренняя ошибка сервера"})
		}
	}
}
