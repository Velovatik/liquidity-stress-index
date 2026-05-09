package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row — один день: исходные показатики и рассчитанный LSI.
type Row struct {
	Date             time.Time
	BankReserves     float64
	RepoRate         float64
	OFZYield         float64
	TaxPressure      float64
	TreasuryBalance  float64
	LSI              float64
}

// DB — пул соединений Postgres с небольшими хелперами для этого демо.
type DB struct {
	*pgxpool.Pool
}

// ClearLSIData удаляет строки перед новой загрузкой ETL (упрощение для демо).
func (db *DB) ClearLSIData(ctx context.Context) error {
	_, err := db.Exec(ctx, `DELETE FROM lsi_data`)
	return err
}

// InsertRows пакетно вставляет нормализованные строки с LSI.
func (db *DB) InsertRows(ctx context.Context, rows []Row) error {
	const insertSQL = `
INSERT INTO lsi_data (date, bank_reserves, repo_rate, ofz_yield, tax_pressure, treasury_balance, lsi)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`

	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(insertSQL,
			r.Date.UTC().Truncate(24*time.Hour),
			r.BankReserves,
			r.RepoRate,
			r.OFZYield,
			r.TaxPressure,
			r.TreasuryBalance,
			r.LSI,
		)
	}

	res := db.Pool.SendBatch(ctx, batch)
	defer res.Close()

	for range rows {
		if _, err := res.Exec(); err != nil {
			return fmt.Errorf("пакетная вставка: %w", err)
		}
	}
	return nil
}

// LatestLSI возвращает последнюю по дате запись с LSI.
func (db *DB) LatestLSI(ctx context.Context) (*Row, error) {
	const q = `
SELECT date, bank_reserves, repo_rate, ofz_yield, tax_pressure, treasury_balance, lsi
FROM lsi_data
ORDER BY date DESC
LIMIT 1`

	var row Row
	err := db.QueryRow(ctx, q).Scan(
		&row.Date,
		&row.BankReserves,
		&row.RepoRate,
		&row.OFZYield,
		&row.TaxPressure,
		&row.TreasuryBalance,
		&row.LSI,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// HistoryLSI — последние limit записей, по дате по возрастанию.
func (db *DB) HistoryLSI(ctx context.Context, limit int32) ([]Row, error) {
	const q = `
SELECT date, bank_reserves, repo_rate, ofz_yield, tax_pressure, treasury_balance, lsi
FROM (
  SELECT date, bank_reserves, repo_rate, ofz_yield, tax_pressure, treasury_balance, lsi
  FROM lsi_data
  ORDER BY date DESC
  LIMIT $1
) sub
ORDER BY date ASC`

	rows, err := db.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(
			&r.Date,
			&r.BankReserves,
			&r.RepoRate,
			&r.OFZYield,
			&r.TaxPressure,
			&r.TreasuryBalance,
			&r.LSI,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
