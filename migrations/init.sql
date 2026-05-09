-- Демо-схема: дневные метрики ликвидности и рассчитанный LSI
CREATE TABLE IF NOT EXISTS lsi_data (
    date DATE PRIMARY KEY,
    bank_reserves DOUBLE PRECISION NOT NULL,
    repo_rate DOUBLE PRECISION NOT NULL,
    ofz_yield DOUBLE PRECISION NOT NULL,
    tax_pressure DOUBLE PRECISION NOT NULL,
    treasury_balance DOUBLE PRECISION NOT NULL,
    lsi DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lsi_data_date_desc ON lsi_data (date DESC);
