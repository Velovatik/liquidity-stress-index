package etl

import (
	"encoding/csv"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"
)

// EnsureLiquidityCSV при отсутствии файла создаёт data/liquidity.csv с синтетическими шумовыми показателями.
func EnsureLiquidityCSV(rootDir string) (string, error) {
	dir := filepath.Join(rootDir, "data")
	path := filepath.Join(dir, "liquidity.csv")
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	if err := writeSyntheticLiquidityCSV(path, 100, time.Now().UTC()); err != nil {
		return "", err
	}
	return path, nil
}

func writeSyntheticLiquidityCSV(path string, rows int, end time.Time) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)
	if err := w.Write([]string{"date", "bank_reserves", "repo_rate", "ofz_yield", "tax_pressure", "treasury_balance"}); err != nil {
		return err
	}

	// Зерно ГСЧ — воспроизводимые кривые для демо; лёгкий дрейф и джиттер по дням.
	src := rand.New(rand.NewPCG(42, 7))
	br := 1_050_000.0
	repo := 7.55
	ofz := 10.95
	tax := 45.20
	tr := 980_500.0

	j := func(amplitude float64) float64 {
		return (src.Float64()*2 - 1) * amplitude
	}

	for i := 0; i < rows; i++ {
		day := end.Truncate(24 * time.Hour).Add(-time.Duration(rows-1-i) * 24 * time.Hour)
		date := day.Format(time.DateOnly)

		br *= 1 + j(0.0009)
		repo += j(0.06)
		ofz += j(0.085)
		tax += j(0.22)
		tr *= 1 + j(0.00075)

		// Небольшой детерминированный сдвиг вдоль горизонта, чтобы ряды не «залипали».
		if i%23 == 0 {
			br *= 1.002
			repo += 0.02
			ofz += 0.03
			tax += 0.15
			tr *= 0.998
		}

		row := []string{
			date,
			fmt.Sprintf("%.4f", br),
			fmt.Sprintf("%.4f", repo),
			fmt.Sprintf("%.4f", ofz),
			fmt.Sprintf("%.4f", tax),
			fmt.Sprintf("%.4f", tr),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
