package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"liquidity-stress-index/internal/api"
	"liquidity-stress-index/internal/etl"
	"liquidity-stress-index/internal/metrics"
	"liquidity-stress-index/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("критическая ошибка: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	root, err := projectRoot()
	if err != nil {
		return fmt.Errorf("корень проекта: %w", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(dbURL) == "" {
		dbURL = "postgres://lsi:lsi@localhost:5432/lsi?sslmode=disable"
	}

	db, err := waitForDatabase(ctx, dbURL, 30)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := applyMigrations(ctx, db); err != nil {
		return err
	}

	csvPath := getenv("CSV_PATH", "")
	if strings.TrimSpace(csvPath) == "" {
		csvPath, err = etl.EnsureLiquidityCSV(root)
		if err != nil {
			return fmt.Errorf("подготовка liquidity.csv: %w", err)
		}
	}

	etlInterval, err := parseDurationEnv("ETL_INTERVAL", "")
	if err != nil {
		return err
	}

	state := &api.ETLState{}
	runETL := func() {
		start := time.Now().UTC()
		state.SetRunning(start)
		if err := etl.RunPipeline(ctx, csvPath, db); err != nil {
			state.SetFailed(time.Now().UTC(), err)
			metrics.ObserveETLError(time.Since(start))
			log.Printf("ETL ошибка: %v", err)
			return
		}
		state.SetSucceeded(time.Now().UTC())
		metrics.ObserveETLSuccess(time.Since(start))
	}

	// Первый прогон — синхронно: так API не отдаст “пусто”, если данных реально нет.
	runETL()

	var wg sync.WaitGroup
	if etlInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(etlInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					runETL()
				}
			}
		}()
	}

	addr := getenv("LISTEN_ADDR", ":8080")
	log.Printf("демо API LSI слушает %s\n", addr)
	handler := metrics.InstrumentHandler(api.NewMux(db, state))
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		wg.Wait()
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			wg.Wait()
			return nil
		}
		return err
	}
}

func projectRoot() (string, error) {
	if trimmed := strings.TrimSpace(os.Getenv("APP_ROOT")); trimmed != "" {
		return trimmed, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return wd, nil
}

func getenv(k, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return fallback
}

func parseDurationEnv(k, fallback string) (time.Duration, error) {
	raw := getenv(k, fallback)
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s должен быть duration (например 30s, 5m), получили %q: %w", k, raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s не может быть отрицательным: %q", k, raw)
	}
	return d, nil
}

func applyMigrations(ctx context.Context, db *storage.DB) error {
	bytes, err := os.ReadFile("migrations/init.sql")
	if err != nil {
		return fmt.Errorf("чтение миграций: %w", err)
	}
	for _, stmt := range statements(string(bytes)) {
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("миграция: %w", err)
		}
	}
	log.Println("миграции успешно применены")
	return nil
}

func statements(contents string) []string {
	raw := strings.Split(contents, ";")
	out := make([]string, 0, len(raw))
	for _, fragment := range raw {
		stmt := strings.TrimSpace(fragment)
		if stmt == "" {
			continue
		}
		out = append(out, stmt)
	}
	return out
}

func waitForDatabase(ctx context.Context, url string, maxAttempts int) (*storage.DB, error) {
	for attempt := range maxAttempts {
		pool, err := storage.Connect(ctx, url)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			errPing := pool.Ping(pingCtx)
			cancel()
			if errPing == nil {
				log.Printf("база данных готова с попытки %d", attempt+1)
				return pool, nil
			}
			pool.Close()
			log.Printf("ping не прошёл (попытка %d/%d): %v", attempt+1, maxAttempts, errPing)
		} else {
			log.Printf("соединение не удалось (попытка %d/%d): %v", attempt+1, maxAttempts, err)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return nil, fmt.Errorf("база данных недоступна после %d попыток", maxAttempts)
}
