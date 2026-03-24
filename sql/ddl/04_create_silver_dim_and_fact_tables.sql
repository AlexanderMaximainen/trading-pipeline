USE trading_pipeline;
GO

CREATE TABLE silver.dim_instrument 
(
    instrument_id INT IDENTITY(1,1) PRIMARY KEY, 
    isin CHAR(12) NOT NULL UNIQUE,
    instrument_name NVARCHAR(150) NOT NULL,
    instrument_group NVARCHAR(50),
    underlying_asset NVARCHAR(30),
    direction VARCHAR(10),
    issuer NVARCHAR(20),

    CONSTRAINT chk_dim_instrument_direction
        CHECK (direction IN ('LONG', 'SHORT'))
);
GO

CREATE TABLE silver.dim_date
(
	date_id INT NOT NULL,
	[date] DATE NOT NULL, 
    [year] SMALLINT NOT NULL,
	[month] TINYINT NOT NULL,
	[month_name] NVARCHAR(20) NOT NULL,
    [weekday] TINYINT NOT NULL,
	weekday_name NVARCHAR(20) NOT NULL,
    [week] TINYINT NOT NULL,
	[day] TINYINT NOT NULL,
    [quarter] TINYINT NOT NULL,
	quarter_name CHAR(2) NOT NULL,
    year_month VARCHAR(7) NOT NULL,

	CONSTRAINT PK_dim_date PRIMARY KEY (date_id),
	CONSTRAINT UQ_dim_date_date UNIQUE ([date])
);
GO

CREATE TABLE silver.fact_trades 
(
    trade_id INT IDENTITY(1,1) PRIMARY KEY,
    bronze_raw_id INT NOT NULL,
    instrument_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    account NVARCHAR(50) NOT NULL,
    trade_type NVARCHAR(10) NOT NULL,
    quantity INT,
    price DECIMAL(10,4),
    amount DECIMAL(10,2),
    transaction_currency CHAR(3),
    fee DECIMAL(10,2),
    instrument_currency CHAR(3),
    result DECIMAL(10,2),
    source_file NVARCHAR(255) NOT NULL,
    loaded_at DATETIME2(3) NOT NULL,

    CONSTRAINT FK_fact_trades_dim_instrument FOREIGN KEY (instrument_id)
        REFERENCES silver.dim_instrument (instrument_id),

    CONSTRAINT chk_fact_trades_trade_type
        CHECK (trade_type IN ('Köp', 'Sälj'))
);
GO

CREATE INDEX idx_fact_trades_bronze_raw_id
    ON silver.fact_trades (bronze_raw_id);
GO

CREATE INDEX idx_fact_trades_instrument_id
    ON silver.fact_trades (instrument_id);
GO

CREATE INDEX idx_fact_trades_transaction_date
    ON silver.fact_trades (transaction_date);
GO

CREATE TABLE silver.fact_cash_movements
(
    cash_movement_id INT IDENTITY(1,1) PRIMARY KEY,
    bronze_raw_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    account NVARCHAR(50) NOT NULL,
    cash_movement_type NVARCHAR(50) NOT NULL,
    [description] NVARCHAR(100),
    amount DECIMAL(10,2) NOT NULL,
    transaction_currency CHAR(3),
    source_file NVARCHAR(255) NOT NULL,
    loaded_at DATETIME2(3) NOT NULL
);
GO

CREATE INDEX idx_fact_cash_movements_bronze_raw_id
    ON silver.fact_cash_movements (bronze_raw_id);
GO

CREATE INDEX idx_fact_cash_movements_transaction_date
    ON silver.fact_cash_movements (transaction_date);
GO
