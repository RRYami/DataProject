-- Add performance indexes (these work fine in DuckDB)
CREATE INDEX IF NOT EXISTS idx_tickers_active ON tickers(active);
CREATE INDEX IF NOT EXISTS idx_tickers_market ON tickers(market);
CREATE INDEX IF NOT EXISTS idx_company_active ON company_details(active);
CREATE INDEX IF NOT EXISTS idx_company_exchange ON company_details(primary_exchange);
CREATE INDEX IF NOT EXISTS idx_company_sic ON company_details(sic_code);
CREATE INDEX IF NOT EXISTS idx_price_data_date ON price_data(date);

-- Verify indexes were created
SELECT
    index_name,
    table_name,
    sql
FROM duckdb_indexes()
ORDER BY table_name, index_name;
