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

