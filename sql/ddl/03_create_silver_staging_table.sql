USE trading_pipeline;
GO

CREATE TABLE silver.stg_transactions
(
    stg_transaction_id INT IDENTITY(1,1) PRIMARY KEY,
    bronze_raw_id INT NOT NULL,
    transaction_date DATE,
    account NVARCHAR(50),
    transaction_type NVARCHAR(50),
    instrument_name NVARCHAR(150),
    quantity INT,
    price DECIMAL(10,4),
    amount DECIMAL(10,2),
    transaction_currency CHAR(3),
    fee DECIMAL(10,2),
    instrument_currency CHAR(3),
    isin CHAR(12),
    result DECIMAL(10,2),
    source_file NVARCHAR(255) NOT NULL,
    loaded_at DATETIME2(3) NOT NULL
);