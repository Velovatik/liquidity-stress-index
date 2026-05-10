package etl

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	"liquidity-stress-index/internal/storage"
)

const (
	weightsPerMetric = 0.2
	// csvCols — число числовых колонок: bank_reserves, repo_rate, ofz_yield, tax_pressure, treasury_balance
	csvCols = 5
)

// ParsedRow — одна строка CSV до нормализации.
type ParsedRow struct {
	Date            time.Time
	BankReserves    float64
	RepoRate        float64
	OFZYield        float64
	TaxPressure     float64
	TreasuryBalance float64
}

// RunPipeline — один синхронный проход: CSV → расчёт LSI → Postgres.
func RunPipeline(ctx context.Context, csvPath string, db *storage.DB) error {
	all, err := readCSV(csvPath)
	if err != nil {
		return fmt.Errorf("чтение csv: %w", err)
	}
	if len(all) == 0 {
		return fmt.Errorf("в %s нет строк данных", csvPath)
	}

	// Инкрементальная загрузка: если в БД уже есть данные, перезагружаем только новые даты.
	// Нормализация считается по всему окну CSV, чтобы шкала LSI была стабильна между прогонами.
	// Вставка при этом остаётся инкрементальной/идемпотентной (грузим только новые даты).
	raw := all
	if last, ok, err := db.LatestDate(ctx); err != nil {
		return fmt.Errorf("чтение последней даты в БД: %w", err)
	} else if ok {
		raw = filterAfterDate(all, last)
		if len(raw) == 0 {
			return nil
		}
	}

	// Нормализуем по полному CSV-окну (all), но LSI считаем/пишем только для raw (новые даты).
	normalizedWeights := [][]float64{
		columnValues(all, func(r ParsedRow) float64 { return r.BankReserves }),
		columnValues(all, func(r ParsedRow) float64 { return r.RepoRate }),
		columnValues(all, func(r ParsedRow) float64 { return r.OFZYield }),
		columnValues(all, func(r ParsedRow) float64 { return r.TaxPressure }),
		columnValues(all, func(r ParsedRow) float64 { return r.TreasuryBalance }),
	}

	// Для буферов ликвидности инвертируем: меньше резервов / меньше казначейство → выше стресс.
	invertMask := []bool{true, false, false, false, true}

	normcolsAll := make([][]float64, len(invertMask))
	for i := range invertMask {
		normcolsAll[i] = normalizeMinMax(normalizedWeights[i], invertMask[i])
	}

	// Индекс соответствия: date -> индекс строки в all.
	// Временная зона/округление синхронизированы с тем, как храним date в БД.
	idxByDate := make(map[time.Time]int, len(all))
	for i := range all {
		d := all[i].Date.UTC().Truncate(24 * time.Hour)
		idxByDate[d] = i
	}

	rows := make([]storage.Row, len(raw))
	for i := range raw {
		key := raw[i].Date.UTC().Truncate(24 * time.Hour)
		allIdx, ok := idxByDate[key]
		if !ok {
			return fmt.Errorf("внутренняя ошибка: дата %s отсутствует в полном наборе CSV", key.Format(time.DateOnly))
		}

		sum := 0.0
		for j := 0; j < len(normcolsAll); j++ {
			sum += weightsPerMetric * normcolsAll[j][allIdx]
		}
		lsi := 100 * sum // веса в сумме 1.0, нормализованные члены в [0,1]

		rows[i] = storage.Row{
			Date:            raw[i].Date,
			BankReserves:    raw[i].BankReserves,
			RepoRate:        raw[i].RepoRate,
			OFZYield:        raw[i].OFZYield,
			TaxPressure:     raw[i].TaxPressure,
			TreasuryBalance: raw[i].TreasuryBalance,
			LSI:             clamp(lsi, 0, 100),
		}
	}

	if err := db.UpsertRows(ctx, rows); err != nil {
		return fmt.Errorf("вставка строк: %w", err)
	}

	return nil
}

func clamp(v, low, high float64) float64 {
	return math.Min(high, math.Max(low, v))
}

func filterAfterDate(rows []ParsedRow, after time.Time) []ParsedRow {
	after = after.UTC().Truncate(24 * time.Hour)
	out := rows[:0]
	for _, r := range rows {
		d := r.Date.UTC().Truncate(24 * time.Hour)
		if d.After(after) {
			out = append(out, r)
		}
	}
	return out
}

func columnValues(rows []ParsedRow, pick func(ParsedRow) float64) []float64 {
	out := make([]float64, len(rows))
	for i := range rows {
		out[i] = pick(rows[i])
	}
	return out
}

func normalizeMinMax(vals []float64, invert bool) []float64 {
	minV, maxV := vals[0], vals[0]
	for _, v := range vals {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	den := maxV - minV
	if den == 0 {
		den = 1
	}

	out := make([]float64, len(vals))
	for i, v := range vals {
		t := (v - minV) / den
		if invert {
			t = 1 - t
		}
		out[i] = t
	}
	return out
}

func readCSV(path string) ([]ParsedRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("чтение заголовка: %w", err)
	}
	expected := []string{"date", "bank_reserves", "repo_rate", "ofz_yield", "tax_pressure", "treasury_balance"}
	if len(header) != len(expected) {
		return nil, fmt.Errorf("неверное число колонок csv: %v, ожидалось %v", header, expected)
	}
	for i := range expected {
		if header[i] != expected[i] {
			return nil, fmt.Errorf("неверный заголовок %s, ожидалось %s", header[i], expected[i])
		}
	}

	var rows []ParsedRow
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(expected) {
			return nil, fmt.Errorf("неверное число полей в строке %+v", rec)
		}
		ds := rec[0]
		date, err := time.Parse(time.DateOnly, ds)
		if err != nil {
			return nil, fmt.Errorf("разбор даты %q: %w", ds, err)
		}

		nums := make([]float64, csvCols)
		for i := range nums {
			nums[i], err = strconv.ParseFloat(rec[i+1], 64)
			if err != nil {
				return nil, fmt.Errorf("число %s, колонка %d: %w", rec[i+1], i+1, err)
			}
		}
		rows = append(rows, ParsedRow{
			Date:            date,
			BankReserves:    nums[0],
			RepoRate:        nums[1],
			OFZYield:        nums[2],
			TaxPressure:     nums[3],
			TreasuryBalance: nums[4],
		})
	}
	return rows, nil
}
