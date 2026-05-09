package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"liquidity-stress-index/internal/api"
	"liquidity-stress-index/internal/etl"
	"liquidity-stress-index/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("критическая ошибка: %v", err)
	}
}

func run() error {
	ctx := context.Background()

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

	csvPath, err := etl.EnsureLiquidityCSV(root)
	if err != nil {
		return fmt.Errorf("подготовка liquidity.csv: %w", err)
	}

	if err := etl.RunPipeline(ctx, csvPath, db); err != nil {
		return fmt.Errorf("конвейер ETL: %w", err)
	}

	addr := getenv("LISTEN_ADDR", ":8080")
	log.Printf("демо API LSI слушает %s\n", addr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewMux(db),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return srv.ListenAndServe()
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
