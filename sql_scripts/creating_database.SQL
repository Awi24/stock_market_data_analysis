-- Create database
CREATE DATABASE stock_analysis;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS stock_prices;
DROP TABLE IF EXISTS companies;

-- Daily stock price table
CREATE TABLE IF NOT EXISTS stock_prices (
    trade_date DATE,
    ticker VARCHAR(10),
    open_price NUMERIC(10,3),
    high_price NUMERIC(10,3),   
    low_price NUMERIC(10,3),
    close_price NUMERIC(10,3),
    volume BIGINT,
    PRIMARY KEY (ticker, trade_date)
);

-- Company information table
CREATE TABLE IF NOT EXISTS companies (
    ticker VARCHAR(10),
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    PRIMARY KEY (ticker)
);

-- Importing historical data to the database
COPY stock_prices (trade_date,ticker, open_price, high_price, low_price, close_price, volume) 
FROM 'D:\Coding_Projects\stock_market_data_analysis\data\stock_prices.csv' WITH (FORMAT csv, HEADER true);

-- Impporting company data from csv.
COPY companies (ticker, company_name, sector,industry) FROM 'D:\Coding_Projects\stock_market_data_analysis\data\company_info.csv' WITH (FORMAT csv, HEADER true);