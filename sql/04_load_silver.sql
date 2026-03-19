USE trading_pipeline;
GO

INSERT INTO silver.fact_transactions 
(
    transaction_date,
    account,
    transaction_type,
    instrument_name,
    quantity,
    price,
    amount,
    transaction_currency,
    fee,
    exchange_rate,
    instrument_currency,
    isin,
    result,
    source_file,
    loaded_at
)
SELECT
    TRY_CONVERT(DATE, [Datum]) AS transaction_date,
    [Konto] AS account,
    [Typ av transaktion] AS transaction_type,
    [Värdepapper/beskrivning] AS instrument_name,
    TRY_CONVERT(DECIMAL(18,4), REPLACE([Antal], ',', '.')) AS quantity,
    TRY_CONVERT(DECIMAL(18,6), REPLACE([Kurs], ',', '.')) AS price,
    TRY_CONVERT(DECIMAL(18,2), REPLACE([Belopp], ',', '.')) AS amount,
    UPPER(LTRIM(RTRIM([Transaktionsvaluta]))) AS transaction_currency,
    TRY_CONVERT(DECIMAL(18,2), REPLACE([Courtage], ',', '.')) AS fee,
    TRY_CONVERT(DECIMAL(18,6), REPLACE([Valutakurs], ',', '.')) AS exchange_rate,
    UPPER(LTRIM(RTRIM([Instrumentvaluta]))) AS instrument_currency,
    [ISIN] AS isin,
    TRY_CONVERT(DECIMAL(18,2), REPLACE([Resultat], ',', '.')) AS result,
    [source_file],
    [loaded_at]
FROM bronze.transactions;
GO

select * from silver.fact_transactions;