USE trading_pipeline;
GO

CREATE OR ALTER PROCEDURE silver.usp_load_stg_transactions
AS
BEGIN
    SET NOCOUNT ON;

    -- -------------------------------------------------------
    -- Load and transform data from bronze to silver layer
    -- Includes data type conversion and duplicate protection
    -- -------------------------------------------------------

    INSERT INTO silver.stg_transactions
    (
        bronze_raw_id,
        transaction_date,
        account,
        transaction_type,
        instrument_name,
        quantity,
        price,
        amount,
        transaction_currency,
        fee,
        instrument_currency,
        isin,
        result,
        source_file,
        loaded_at
    )
    SELECT
        b.raw_id,
        TRY_CONVERT(DATE, [Datum]) AS transaction_date,
        [Konto] AS account,
        [Typ av transaktion] AS transaction_type,
        [Värdepapper/beskrivning] AS instrument_name,
        TRY_CONVERT(INT, TRY_CONVERT(DECIMAL(10,0), REPLACE([Antal], ',', '.'))),
        TRY_CONVERT(DECIMAL(10,4), REPLACE([Kurs], ',', '.')) AS price,
        TRY_CONVERT(DECIMAL(10,2), REPLACE([Belopp], ',', '.')) AS amount,
        UPPER(LTRIM(RTRIM([Transaktionsvaluta]))) AS transaction_currency,
        TRY_CONVERT(DECIMAL(10,2), REPLACE([Courtage], ',', '.')) AS fee,
        UPPER(LTRIM(RTRIM([Instrumentvaluta]))) AS instrument_currency,
        [ISIN] AS isin,
        TRY_CONVERT(DECIMAL(10,2), REPLACE([Resultat], ',', '.')) AS result,
        [source_file],
        [loaded_at]
    FROM bronze.transactions b
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM silver.stg_transactions s
        WHERE s.bronze_raw_id = b.raw_id
    );
END;
GO