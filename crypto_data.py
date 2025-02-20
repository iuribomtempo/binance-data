# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 08:54:34 2024

@author: Iuri Bomtempo Retamal

"""

#%%
#Install Packages
!pip install python-binance
!pip install pandas
!pip install mplfinance
!pip install python-dotenv
#%%

#%%
#Import packages
from binance.client import Client
import pandas as pd
import mplfinance as mpf
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

#%% Load Binance API_KEY e SECRET_KEY from .env file

load_dotenv()

# Load variables from .env file
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

#%%
# Instantiate the client
client = Client(API_KEY, SECRET_KEY, testnet=True)

#%%
# Getting avaibles tickers from Binance
tickers = client.get_all_tickers()

# Convert tickers to a pandas df
ticker_df = pd.DataFrame(tickers)

# Reove the price column
ticker_df = ticker_df.drop('price', axis=1)


# Visualize Ticker's header
print(ticker_df.head())

# Set symbol as index
ticker_df.set_index('symbol', inplace=True)

# Localize a specific ticker e.g. BTC/USDT (Bitcoin/Theter)
ticker_df.loc['BTCUSDT']

#%% Filter USDT and BTC pairs from tickers

# USDT pairs
usdt_pairs = ticker_df[ticker_df.index.str.contains("USDT")]

btc_pairs = ticker_df[ticker_df.index.str.contains('BTC')]


#%% #Available Intervals
'''
KLINE_INTERVAL_1SECOND = '1s'
KLINE_INTERVAL_1MINUTE = '1m'
KLINE_INTERVAL_3MINUTE = '3m'
KLINE_INTERVAL_5MINUTE = '5m'
KLINE_INTERVAL_15MINUTE = '15m'
KLINE_INTERVAL_30MINUTE = '30m'
KLINE_INTERVAL_1HOUR = '1h'
KLINE_INTERVAL_2HOUR = '2h'
KLINE_INTERVAL_4HOUR = '4h'
KLINE_INTERVAL_6HOUR = '6h'
KLINE_INTERVAL_8HOUR = '8h'
KLINE_INTERVAL_12HOUR = '12h'
KLINE_INTERVAL_1DAY = '1d'
KLINE_INTERVAL_3DAY = '3d'
KLINE_INTERVAL_1WEEK = '1w'
KLINE_INTERVAL_1MONTH = '1M'
'''

#%% Get Historical Kline/Candlesticks
# Fetch klines for any date range and interval

# fetch 1 minute klines for the last day up until now
klines = client.get_historical_klines("BNBBTC", Client.KLINE_INTERVAL_1MINUTE,
                                      "1 day ago UTC")

# fetch 30 minute klines for specifc month of 2017
klines = client.get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_30MINUTE,
                                      "1 Dec, 2017", "1 Jan, 2018")

# fetch weekly klines since it listed
klines = client.get_historical_klines("NEOBTC", Client.KLINE_INTERVAL_1WEEK,
                                      "1 Jan, 2017")

#%% Get Historical Kline/Candlesticks using a generator

for kline in client.get_historical_klines_generator(
        "BTCUSDT", Client.KLINE_INTERVAL_1WEEK, "1 day ago UTC"):
    print(kline)

test_kline = client.get_historical_klines_generator(
        "BTCUSDT", Client.KLINE_INTERVAL_1WEEK, "1 day ago UTC")



#%% 
def fetch_binance_ohlcv(symbol: str, interval: str, start_str: str,
                        end_str: str, save_path: str):
    """
    Fetch OHLCV data from Binance and save it into a CSV file.
    
    :param symbol: Trading pair symbol (e.g., 'BTCUSDT').
    :param interval: Time interval (e.g., Client.KLINE_INTERVAL_1DAY).
    :param start_str: Start date (e.g., '1 Jan 2020').
    :param end_str: End date (e.g., '1 Jan 2021').
    :param save_path: Path to save the CSV file.
    """
    # Load Binance API keys from environment variables
    API_KEY = os.getenv("BINANCE_API_KEY")
    API_SECRET = os.getenv("BINANCE_API_SECRET")
    
    # Initialize Binance client
    client = Client(API_KEY, API_SECRET)
    
    # Fetch historical Kline (candlestick) data
    klines = client.get_historical_klines(symbol=symbol,
                                          interval=interval,
                                          start_str=start_str,
                                          end_str=end_str)
    
    # Convert to DataFrame
    columns = ["open_time", "open", "high", "low", "close", "close_time",
               "volume"]
    df = pd.DataFrame(klines, columns=["open_time", "open", "high", "low",
                                       "close", "volume", "close_time",
                                       "quote_asset_volume","number_of_trades",
                                       "taker_buy_base_volume",
                                       "taker_buy_quote_volume", "ignore"])
    df = df[columns]
    
    # Convert timestamps to datetime format
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')
    
    # Convert numeric columns to float
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    
    # Save to CSV
    df.to_csv(save_path, index=False)
    print(f"Data saved to {save_path}")


#%% Example usage

if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_1DAY
    start_str = "1 Jan 2020"
    end_str = "1 Jan 2021"
    object_name = "btcusdt_1d"
    save_path = f"{object_name}.csv"
    
    fetch_binance_ohlcv(symbol=symbol, interval=interval, start_str=start_str,
                        end_str=end_str, save_path=save_path)

#%%
def fetch_historical_data(symbol: str, interval: str):
    """
    Retrieve historical OHLCV data from Binance since 01/01/2017 to yesterday and save it as CSV.
    
    :param symbol: Trading pair symbol (e.g., 'BTCUSDT').
    :param interval: Time interval (e.g., Client.KLINE_INTERVAL_4HOUR).
    """
    start_str = "1 Jan 2017"
    end_str = (datetime.now() - timedelta(days=1)).strftime("%d %b %Y")
    save_path = f"{symbol}_{interval}.csv"
    
    fetch_binance_ohlcv(symbol, interval, start_str, end_str, save_path)
    
#%% Example usage

if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = Client.KLINE_INTERVAL_4HOUR
    fetch_historical_data(symbol, interval)

#%% 
def fetch_batch_data(symbols, intervals, start_str="1 Jan 2017", end_str=None):
    """
    Fetch OHLCV data for multiple symbols and intervals, and save each combination to a CSV.

    :param symbols: List of symbols (e.g., ['BTCUSDT', 'ETHUSDT']).
    :param intervals: List of intervals (e.g., [Client.KLINE_INTERVAL_4HOUR, Client.KLINE_INTERVAL_1DAY]).
    :param start_str: Start date as a string (default '1 Jan 2017').
    :param end_str: End date as a string (default = 'yesterday').
    """

    # Se end_str não for fornecido, use 'ontem' como padrão
    if end_str is None:
        end_str = (datetime.now() - timedelta(days=1)).strftime("%d %b %Y")

    for sym in symbols:
        for interval in intervals:
            # Gera nome de arquivo a partir de symbol e interval
            save_path = f"{sym}_{interval}.csv"
            fetch_binance_ohlcv(sym, interval, start_str, end_str, save_path)

#%% 
if __name__ == "__main__":
    # Exemple of lists of symbols and interval
    symbols = ["BTCUSDT", "ETHUSDT"]
    intervals = [Client.KLINE_INTERVAL_1WEEK]

    # call de function
    fetch_batch_data(symbols, intervals)
