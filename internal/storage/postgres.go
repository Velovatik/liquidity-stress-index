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

// LatestDate возвращает максимальную дату, загруженную в lsi_data.
// Если таблица пуста — ok=false.
func (db *DB) LatestDate(ctx context.Context) (date time.Time, ok bool, err error) {
	const q = `SELECT max(date) FROM lsi_data`
	var d *time.Time
	if err := db.QueryRow(ctx, q).Scan(&d); err != nil {
		return time.Time{}, false, err
	}
	if d == nil {
		return time.Time{}, false, nil
	}
	return d.UTC().Truncate(24 * time.Hour), true, nil
}

// UpsertRows пакетно вставляет/обновляет строки (идемпотентная загрузка по date).
func (db *DB) UpsertRows(ctx context.Context, rows []Row) error {
	const upsertSQL = `
INSERT INTO lsi_data (date, bank_reserves, repo_rate, ofz_yield, tax_pressure, treasury_balance, lsi)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (date) DO UPDATE SET
  bank_reserves = EXCLUDED.bank_reserves,
  repo_rate = EXCLUDED.repo_rate,
  ofz_yield = EXCLUDED.ofz_yield,
  tax_pressure = EXCLUDED.tax_pressure,
  treasury_balance = EXCLUDED.treasury_balance,
  lsi = EXCLUDED.lsi
`

	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(upsertSQL,
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
			return fmt.Errorf("пакетная вставка/upsert: %w", err)
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
