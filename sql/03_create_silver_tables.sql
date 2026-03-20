USE trading_pipeline;
GO

CREATE TABLE silver.fact_transactions 
(
    transaction_id INT IDENTITY(1,1) PRIMARY KEY,
    bronze_raw_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    account NVARCHAR(50) NOT NULL,
    transaction_type NVARCHAR(50) NOT NULL,
    instrument_name NVARCHAR(255) NOT NULL,
    quantity DECIMAL(18,4),
    price DECIMAL(18,6),
    amount DECIMAL(18,2),
    transaction_currency CHAR(3),
    fee DECIMAL(18,2),
    exchange_rate DECIMAL(18,6),
    instrument_currency CHAR(3),
    isin NVARCHAR(20),
    result DECIMAL(18,2),
    source_file NVARCHAR(255) NOT NULL,
    loaded_at DATETIME2(3) NOT NULL
);
GO

CREATE INDEX idx_fact_transactions_bronze_raw_id
    ON silver.fact_transactions(bronze_raw_id);