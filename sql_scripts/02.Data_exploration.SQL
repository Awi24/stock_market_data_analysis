-- General data quality checks

--data count of each table
SELECT COUNT (*) AS companies_count FROM companies;
SELECT COUNT (*) AS stock_prieces_count FROM stock_prices;

-- the date range of stock prices data
SELECT MIN (trade_date) AS earliest_date, MAX (trade_date) as latest_date FROM stock_prices;

-- Trading days of each stock in the dataset
SELECT 
    ticker,
    COUNT (trade_date) AS trading_days,
    MIN (trade_date) as first_trading_date
FROM
    stock_prices
GROUP BY
    ticker
ORDER BY 
    trading_days ASC;

--Checking each company have null on sector.  
SELECT 
    industry,
    sector,
    COUNT (ticker) AS number_of_companies
FROM companies
GROUP BY sector, industry
ORDER BY number_of_companies DESC;
-- The total of number_of_companies = 30, which confirms that 
--there are no null values in companies table.

-- DATA INTEGRITY CHECKS
-- This check each stock data entry has corresponding company in the companies table using a LEFT JOIN. 
SELECT 
    DISTINCT sp.ticker
from stock_prices AS sp
LEFT JOIN companies as c
    ON sp.ticker= c.ticker
WHERE c.ticker IS NULL;
-- no reuslts means the dataset have no orphaned data. 

-- This checks if any company has zero price data in stock_prices table using a LEFT JOIN.
SELECT
    DISTINCT c.ticker
FROM companies as c 
LEFT JOIN stock_prices as sp
    ON c.ticker=sp.ticker
WHERE sp.ticker IS NULL;


--Testing data uniqueness. 
-- Checking multiple entries of the same copany in the companies table
SELECT ticker, COUNT(*)
FROM companies
GROUP BY ticker
HAVING COUNT (*) > 1; 

-- check for duplicate prices on same day for same stock
SELECT 
    ticker,
    trade_date,
    COUNT(*)
FROM stock_prices
GROUP BY ticker, trade_date
HAVING COUNT(*)>1

-- These checks confrims that the data is uniqe and complete without any duplication and integrity issues.