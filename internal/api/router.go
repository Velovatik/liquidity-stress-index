package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"liquidity-stress-index/internal/storage"
)

type lsijson struct {
	LSI  float64 `json:"lsi"`
	Date string  `json:"date"`
}

// NewMux регистрирует HTTP-эндпоинты демо на стандартном net/http.
func NewMux(db *storage.DB) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

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
