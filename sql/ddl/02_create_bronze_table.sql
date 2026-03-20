USE trading_pipeline;
GO

CREATE TABLE bronze.transactions 
(
    [raw_id] INT IDENTITY(1,1) PRIMARY KEY,
    [Datum] NVARCHAR(50),
    [Konto] NVARCHAR(50),
    [Typ av transaktion] NVARCHAR(100),
    [Värdepapper/beskrivning] NVARCHAR(255),
    [Antal] NVARCHAR(50),
    [Kurs] NVARCHAR(50),
    [Belopp] NVARCHAR(50),
    [Transaktionsvaluta] NVARCHAR(20),
    [Courtage] NVARCHAR(50),
    [Valutakurs] NVARCHAR(50),
    [Instrumentvaluta] NVARCHAR(20),
    [ISIN] NVARCHAR(20),
    [Resultat] NVARCHAR(50),
    [source_file] NVARCHAR(255) NOT NULL,
    [loaded_at] DATETIME2(3) NOT NULL
);
GO