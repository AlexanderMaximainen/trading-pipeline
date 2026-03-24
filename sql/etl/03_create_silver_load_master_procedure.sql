USE trading_pipeline;
GO

----------------------------------------------------------------
-- Master procedure for silver layer load
-- Executes all silver procedures in dependency order
-- dim_date is a static table and not part of the regular load
----------------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.usp_load_silver
AS
BEGIN
    SET NOCOUNT ON;

    EXEC silver.usp_load_stg_transactions;
    EXEC silver.usp_load_dim_instrument;
    EXEC silver.usp_load_fact_trades;
    EXEC silver.usp_load_fact_cash_movements;
END;
GO