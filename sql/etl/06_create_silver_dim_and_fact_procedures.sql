USE trading_pipeline;
GO

---------------------------------------------------------------------------
-- Stored procedure for dim_instrument load
-- Populates instrument dimension from staging data
-- Applies incremental load on new instruments based on unique ISIN values
-- Updates descriptive attributes such as underlying_asset, direction,
-- issuer and instrument_group
---------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.usp_load_dim_instrument
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.dim_instrument 
    (
        isin,
        instrument_name
    )
    SELECT DISTINCT
        s.isin,
        s.instrument_name
    FROM silver.stg_transactions s
    LEFT JOIN silver.dim_instrument d
        ON d.isin = s.isin
    WHERE s.isin IS NOT NULL
        AND s.instrument_name IS NOT NULL
        AND s.transaction_type IN (N'Köp', N'Sälj')
        AND d.isin IS NULL;

    ---------------------------------------------------------------------
    -- Update underlying asset in dim_instrument based on instrument name
    ---------------------------------------------------------------------

    UPDATE silver.dim_instrument
    SET underlying_asset =
        CASE
            -- Metals
            WHEN instrument_name LIKE '%GULD%' THEN 'GULD'
            WHEN instrument_name LIKE '%SILVER%' THEN 'SILVER'
            WHEN instrument_name LIKE '%KOPPAR%' THEN 'KOPPAR'

            -- Energy
            WHEN instrument_name LIKE '%OLJA%' THEN 'OLJA'
            WHEN instrument_name LIKE '%GAS%' THEN 'GAS'

            -- Indices
            WHEN instrument_name LIKE '%NASDAQ%' THEN 'NASDAQ'
            WHEN instrument_name LIKE '%SP500%' THEN 'SP500'
            WHEN instrument_name LIKE '%DOW%' THEN 'DOW'
            WHEN instrument_name LIKE '%DAX%' THEN 'DAX'
            WHEN instrument_name LIKE '%OMX%' THEN 'OMX'

            ELSE underlying_asset
        END
    WHERE underlying_asset IS NULL;

    ---------------------------------------------------------------
    -- Update direction in dim_instrument based on instrument name
    ---------------------------------------------------------------

    UPDATE silver.dim_instrument
    SET direction =
        CASE
            WHEN instrument_name LIKE '%TURBO S %'
                OR instrument_name LIKE '%SHORT%'
                OR instrument_name LIKE '%BEAR%'
            THEN 'SHORT'

            WHEN instrument_name LIKE '%TURBO L %'
                OR instrument_name LIKE '%LONG%'
                OR instrument_name LIKE '%BULL%'
            THEN 'LONG'

            ELSE direction
        END
    WHERE direction IS NULL;

    ------------------------------------------------------------
    -- Update issuer in dim_instrument based on instrument name
    ------------------------------------------------------------
    UPDATE silver.dim_instrument
    SET issuer =
        CASE
            WHEN instrument_name LIKE '%AVA%' THEN 'AVA'
            WHEN instrument_name LIKE '%BNP%' THEN 'BNP'
            WHEN instrument_name LIKE '% VT%' THEN 'VT'
            ELSE issuer
        END
    WHERE issuer IS NULL;
    
    ----------------------------------------------------------------------
    -- Update instrument_group in dim_instrument based on instrument name
    ----------------------------------------------------------------------

    UPDATE silver.dim_instrument
    SET instrument_group =
        CASE
            WHEN instrument_name LIKE '%TURBO%'
            THEN CONCAT(
                'TURBO ',
                CASE 
                    WHEN direction = 'SHORT' THEN 'S'
                    WHEN direction = 'LONG' THEN 'L'
                END,
                ' ',
                underlying_asset
            )

            WHEN instrument_name LIKE '%BULL%'
            THEN CONCAT('BULL ', underlying_asset)

            WHEN instrument_name LIKE '%BEAR%'
            THEN CONCAT('BEAR ', underlying_asset)

            ELSE instrument_group
        END
    WHERE instrument_group IS NULL
        AND underlying_asset IS NOT NULL
        AND direction IS NOT NULL;
END;
GO

-----------------------------------------------------------------
-- Stored procedure for fact_trades load
-- Loads trade transactions from silver staing into fact_trades
-- Filters for BUY/SELL (Köp/Sälj) transactions only
-- Applies incremental load by using bronze_raw_id
-----------------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.usp_load_fact_trades
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.fact_trades
    (
        bronze_raw_id,
        instrument_id,
        transaction_date,
        account,
        trade_type,
        quantity,
        price,
        amount,
        transaction_currency,
        fee,
        instrument_currency,
        result,
        source_file,
        loaded_at
    )
    SELECT
        s.bronze_raw_id,
        d.instrument_id,
        s.transaction_date,
        s.account,
        s.transaction_type AS trade_type,
        s.quantity,
        s.price,
        s.amount,
        s.transaction_currency,
        s.fee,
        s.instrument_currency,
        s.result,
        s.source_file,
        s.loaded_at
    FROM silver.stg_transactions s
    JOIN silver.dim_instrument d
        ON d.isin = s.isin
    WHERE s.transaction_type IN (N'Köp', N'Sälj')
        AND NOT EXISTS
        (
            SELECT 1
            FROM silver.fact_trades f
            WHERE f.bronze_raw_id = s.bronze_raw_id
        );
END;
GO

-----------------------------------------------------------------------------
-- Stored procedure for fact_cash_movements load
-- Loads non-trade transactions from silver staging into fact_cash_movements
-- Filters for transactions not in BUY/SELL (Köp, Sälj)
-- Applies incremental load using bronze_raw_id
-----------------------------------------------------------------------------
  
CREATE OR ALTER PROCEDURE silver.usp_load_fact_cash_movements
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.fact_cash_movements
    (
        bronze_raw_id,
        transaction_date,
        account,
        cash_movement_type,
        [description],
        amount,
        transaction_currency,
        source_file,
        loaded_at
    )
    SELECT
        s.bronze_raw_id,
        s.transaction_date,
        s.account,
        s.transaction_type AS cash_movement_type,
        s.instrument_name AS [description],
        s.amount,
        s.transaction_currency,
        s.source_file,
        s.loaded_at
    FROM silver.stg_transactions s
    WHERE s.transaction_type NOT IN (N'Köp', N'Sälj')
        AND NOT EXISTS
        (
            SELECT 1
            FROM silver.fact_cash_movements f
            WHERE f.bronze_raw_id = s.bronze_raw_id
        );
END;
GO
        

exec silver.usp_load_dim_instrument
exec.silver.usp_load_fact_cash_movements
exec silver.usp_load_fact_trades

select * from silver.dim_instrument;
select count(*) from silver.dim_instrument;

select * from silver.fact_trades;
select count(*) from silver.fact_trades;

select * from silver.fact_cash_movements;
select count(*) from silver.fact_cash_movements;

select * from silver.stg_transactions;