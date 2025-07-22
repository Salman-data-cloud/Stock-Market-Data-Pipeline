CREATE OR REPLACE TABLE STOCK_CLEANED(
    TradingDate DATE,
    Scrip VARCHAR(30),
    OpenPrice FLOAT,
    HighPrice FLOAT,
    LowPrice FLOAT,
    ClosePrice FLOAT,
    Volume INT,
    SourceFile VARCHAR(100),
    PRIMARY KEY(TradingDate, Scrip)
)