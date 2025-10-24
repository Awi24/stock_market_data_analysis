import enum
import yfinance as yf
import pandas as pd
import time
import logging
import os

logging.basicConfig(level=logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')

#-- Configuration--
# Ticker list of DOW Jones Insudtrial Average Index (DJI)
TICKERS = [
    'AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON',
    'IBM', 'INTC', 'JNJ', 'KO', 'JPM', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE',
    'PG', 'TRV', 'UNH', 'CRM', 'VZ', 'V', 'WBA', 'WMT', 'DIS', 'DOW'
]

# data gathering period  
history= "5y"

# final output file names 
data_folder= 'data'

## Data fetching functions
# fetching company info such as sector and name 
def fetch_company_info (ticker_symbol):
    try:
        ticker= yf.Ticker(ticker_symbol)
        info = ticker.info

        company_data= {
            'ticker': ticker_symbol,
            'company_name': info.get('longName'),
            'sector': info.get('sector'),
            'industry':info.get('industry')
        }
        logging.info(f"Company information of {ticker_symbol} fetched successfully.")
        return company_data
    except Exception as e:
        logging.warning(f"Data fetching failed for {ticker_symbol}. Error:{e}")
        return None

# historical price data gathering function
def fetch_historical_price(ticker_symbol):
    # Collect historic daily OHLC price data for specific time range

    try:
        ticekr =yf.Ticker (ticker_symbol)
        history_data=ticekr.history(period= history)

        if history_data.empty: # handling empty data error.
            logging.warning(f"No historical data found for {ticker_symbol}")
            return None
        
        # data cleaning and formatting
        history_data.reset_index(inplace= True)

        #formatting columns to match the database schema
        history_data.rename(columns ={
            'Date' :'trade_date',
            'Open' :'open_price',
            'High' : 'high_price',
            'Low' : 'low_price',
            'Close' : 'close_price',
            'Volume' : 'volume'
        }, inplace= True)

        # Adding ticker symbol
        history_data['ticker'] =ticker_symbol
        
        # Finalized Columns
        data_final= history_data[['trade_date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']].copy()
        # Rounding the price values
        data_final[['open_price', 'high_price', 'low_price', 'close_price']]= data_final[['open_price', 'high_price', 'low_price', 'close_price']].round(3)

        #Converting date to date as it acts as the index of the data.
        data_final['trade_date']= pd.to_datetime(data_final['trade_date'].dt.date)

        #final log message
        logging.info(f"Successfully fetched historical prices for {ticker_symbol}")
        return data_final
    except Exception as e:
        logging.error (f"Error while fetching historical data for {ticker_symbol}. Error:{e}")
        return None
    
    #--Main Function--
def main():
    logging.info("Fetching 5Y historic data")

    company_info=[]
    price_data = []

    for i, ticker in enumerate(TICKERS):
        logging.info(f"Processing Ticker: {ticker}")

        #Fetching company info
        info=fetch_company_info(ticker)
        if info:
             company_info.append(info)

        #Fetch historic prices
        history= fetch_historical_price(ticker)
        if history is not None:
            price_data.append(history)

        # Sleep to avoid rate-limiting
        time.sleep(1) 

    #-- Saving to files
    if company_info:
        df_companies= pd.DataFrame(company_info)
        df_companies.to_csv('data/company_info.csv', index =False)
        logging.info(f"Company info.csv has saved successfully to: {data_folder}")
    else:
        logging.error("Company info fetching failed.")
        
    if price_data:
        df_prices=pd.concat(price_data, ignore_index=False)
        df_prices.to_csv('data/stock_prices.csv', index=False)
        logging.info(f"stock_prices.csv has saved successfully to: {data_folder}")
    else:
        logging.error("stock price data fetching failed.")
        
    logging.info("--- Data Fetching Process Completed Successfully ---")

if __name__ == "__main__":
    main()
        

