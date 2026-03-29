USE trading_pipeline;
GO
---------------------------------------------------------------
-- 1. Daily PnL
--    Realised PnL per day based on sell transactions
--    Buy transactions are excluded since PnL is realized on sell
--    Includes a running total for use in Power BI
---------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_daily_pnl AS
WITH daily AS 
(
    SELECT
        d.[date] AS trade_date,
        COUNT(*) AS total_sells,
        ROUND(SUM(f.result), 2) AS daily_pnl,
        ROUND(SUM(COALESCE(f.fee, 0)), 2) AS daily_fees
    FROM silver.fact_trades f
    JOIN silver.dim_date d
        ON f.date_id = d.date_id
    WHERE f.trade_type = N'Sälj'
      AND f.result IS NOT NULL
    GROUP BY d.[date]
)
SELECT
    trade_date,
    total_sells,
    daily_pnl,
    daily_fees,
    ROUND(SUM(daily_pnl) OVER (
        ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS cumulative_pnl
FROM daily;
GO

---------------------------------------------------------------
-- 2. Monthly PnL
--    Realised PnL per month based on sell transactions
--    Buy transactions are excluded since PnL is realized on sell
--    Includes a running total for use in Power BI
---------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_monthly_pnl AS
WITH monthly AS
(
    SELECT
        DATEFROMPARTS(d.[year], d.[month], 1) AS month_start,
        COUNT(*) AS total_sells,
        ROUND(SUM(f.result), 2) AS monthly_pnl,
        ROUND(SUM(COALESCE(f.fee, 0)), 2) AS monthly_fees
    FROM silver.fact_trades f
    JOIN silver.dim_date d
        ON f.date_id = d.date_id
    WHERE f.trade_type = N'Sälj'
      AND f.result IS NOT NULL
    GROUP BY
        DATEFROMPARTS(d.[year], d.[month], 1)
)
SELECT
    month_start,
    CONVERT(CHAR(7), month_start, 126) AS year_month,
    total_sells,
    monthly_pnl,
    monthly_fees,
    ROUND(
        SUM(monthly_pnl) OVER (
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        2
    ) AS cumulative_pnl
FROM monthly;
GO
    
---------------------------------------------------------------
-- 3. PnL by instrument
--    Realized PnL per instrument based on sell transactions
--    Useful for comparing which instruments performs the best 
---------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_pnl_by_instrument AS
SELECT
    COALESCE(i.instrument_group, i.instrument_name, 'Unknown') AS instrument,
    i.underlying_asset,
    i.direction,
    f.transaction_currency AS currency,
    COUNT(*) AS total_sells,
    ROUND(SUM(f.result), 2) AS total_pnl,
    ROUND(SUM(CASE WHEN f.result > 0 THEN f.result ELSE 0 END), 2) AS gross_profit,
    ROUND(ABS(SUM(CASE WHEN f.result < 0 THEN f.result ELSE 0 END)), 2) AS gross_loss,
    ROUND(SUM(COALESCE(f.fee, 0)), 2) AS total_fees,
    CAST(
        ROUND(
            100.0 * SUM(CASE WHEN f.result > 0 THEN 1 ELSE 0 END) / COUNT(*),
            2
        ) AS DECIMAL(5,2) 
    ) AS win_rate_pct,
    MIN(d.[date]) AS first_trade_date,
    MAX(d.[date]) AS last_trade_date
FROM silver.fact_trades f
JOIN silver.dim_date d
    ON f.date_id = d.date_id
LEFT JOIN silver.dim_instrument i
    ON f.instrument_id = i.instrument_id
WHERE f.trade_type = N'Sälj'
  AND f.result IS NOT NULL
GROUP BY
    COALESCE(i.instrument_group, i.instrument_name, 'Unknown'),
    i.underlying_asset,
    i.direction,
    f.transaction_currency;
GO
------------------------------------------------------------------------------------
-- 4. PnL by asset
--    Realized PnL grouped by underlying asset (e.g. GULD, OLJA)
--    Helps to see which assets perform best
--    Includes win rate to see how often trades of a specific asset are profitable
------------------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_pnl_by_asset AS
SELECT
    COALESCE(d.underlying_asset, 'Unknown') AS underlying_asset,
    COUNT(*) AS total_sells,
    ROUND(SUM(f.result), 2) AS total_pnl,
    ROUND(SUM(CASE WHEN f.result > 0 THEN f.result ELSE 0 END), 2) AS gross_profit,
    ROUND(ABS(SUM(CASE WHEN f.result < 0 THEN f.result ELSE 0 END)), 2) AS gross_loss,
    CAST(
        ROUND(
            100.0 * SUM(CASE WHEN f.result > 0 THEN 1 ELSE 0 END) / COUNT(*),
            2
        ) AS DECIMAL(5,2) 
    ) AS win_rate_pct
FROM silver.fact_trades f
LEFT JOIN silver.dim_instrument d
    ON f.instrument_id = d.instrument_id
WHERE f.trade_type = N'Sälj'
  AND f.result IS NOT NULL
GROUP BY
    COALESCE(d.underlying_asset, 'Unknown');
GO

---------------------------------------------------------------------------------------
-- 5. PnL by direction
--    Realized PnL grouped by trade direction (LONG vs SHORT)
--    Helps to see if trading LONG or SHORT performs better
--    Includes win rate to see how often trades of a specific direction are profitable
---------------------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_pnl_by_direction AS
SELECT
    COALESCE(d.direction, 'Unknown') AS direction,
    COUNT(*) AS total_sells,
    ROUND(SUM(f.result), 2) AS total_pnl,
    ROUND(SUM(CASE WHEN f.result > 0 THEN f.result ELSE 0 END), 2) AS gross_profit,
    ROUND(ABS(SUM(CASE WHEN f.result < 0 THEN f.result ELSE 0 END)), 2) AS gross_loss,
    CAST(
        ROUND(
            100.0 * SUM(CASE WHEN f.result > 0 THEN 1 ELSE 0 END) / COUNT(*),
            2
        ) AS DECIMAL(5,2) 
    ) AS win_rate_pct
FROM silver.fact_trades f
LEFT JOIN silver.dim_instrument d
    ON f.instrument_id = d.instrument_id
WHERE f.trade_type = N'Sälj'
  AND f.result IS NOT NULL
GROUP BY
    COALESCE(d.direction, 'Unknown');
GO

----------------------------------------------------------------
-- 6. Monthly cash flow
--    Aggregates non-trade cash movements per month
--    Gives an overview of capital inflow and outflow over time
----------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_monthly_cash_flow AS
WITH monthly AS
(
    SELECT
        DATEFROMPARTS(d.[year], d.[month], 1) AS month_start,
        c.cash_movement_type,
        ROUND(SUM(c.amount), 2) AS monthly_amount
    FROM silver.fact_cash_movements c
    JOIN silver.dim_date d
        ON c.date_id = d.date_id
    GROUP BY
        DATEFROMPARTS(d.[year], d.[month], 1),
        c.cash_movement_type
)
SELECT
    month_start,
    CONVERT(CHAR(7), month_start, 126) AS year_month,
    cash_movement_type,
    monthly_amount
FROM monthly;
GO

---------------------------------------------------------------------------------------
-- 7. Unclassified instruments
--    Shows instruments that have missing classification values in dim_instrument
--    Useful for checking which mappings need to be added
---------------------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.v_unclassified_instruments AS
SELECT
    isin,
    instrument_name,
    instrument_group,
    underlying_asset,
    direction,
    issuer
FROM silver.dim_instrument
WHERE instrument_group IS NULL
    OR underlying_asset IS NULL
    OR direction IS NULL
    OR issuer IS NULL;
GO