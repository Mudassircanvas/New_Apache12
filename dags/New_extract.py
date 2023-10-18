import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import datetime
import aiohttp  #
from datetime import date as dt
from datetime import time as tm
import pytz
import asyncio
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
import requests
import os
import glob
import nest_asyncio
import sys
import pickle
# import talib
import glob
import os.path
import re
nest_asyncio.apply()
import datetime

def working(ticker):
    
    
    ticker1 = ticker
   
    
    
    raw_merged_feather = pd.read_feather(f'./docker_storage/raw_data/{ticker1}-Tick-Data.ftr')
    raw_merged_feather.reset_index(inplace=True, drop=True)
    print(raw_merged_feather)
    raw_merged_feather['date'] = raw_merged_feather['participant_timestamp'].dt.date
    a = raw_merged_feather['date'].tail(1)
    a = pd.Series(a)
    a = a.values[0]
    prev_date = a.strftime("%Y-%m-%d")
    prev_date
    today_date = datetime.date.today()
    today_date = today_date.strftime("%Y-%m-%d")
    today_date
    config = {
    "ticker": ticker,
    "start_date":prev_date ,
    "end_date": today_date
    }
    print(config)


    global data_dict
    data_dict = {}
    #ticker_info = {}

    def unix_to_date(dataset, col_name):
        dataset[col_name] = pd.to_datetime(dataset[col_name])
        dataset[col_name] = dataset[col_name].dt.tz_localize('UTC')
        dataset[col_name] = dataset[col_name].dt.tz_convert('US/Eastern')
        dataset[col_name] = dataset[col_name].dt.tz_localize(None)
        return dataset[col_name]


    def daterange(date1, date2):
        for n in range(int((date2 - date1).days) + 1):
            yield date1 + timedelta(n)


    async def get(
        session: aiohttp.ClientSession,
        date: str,
        **kwargs
    ) -> dict:
        global data_dict
        api = f"https://api.polygon.io/v3/trades/{ticker}?timestamp={date}&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
        resp = await session.request('GET', url=api, **kwargs)
        data = await resp.json()
        data_dict[date] = data
        next_url = data.get("next_url", None)
        while next_url is not None:
            next_url_ = next_url+"&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
            resp = await session.request('GET', url=next_url_, **kwargs)
            data = await resp.json()
            data_dict[date]["results"] += data["results"]
            next_url = data.get("next_url", None)


    async def main(dates, **kwargs):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for c in dates:
                tasks.append(get(session=session, date=c, **kwargs))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses

  ############################################################################################

    print("THE CURRENT TICKER IS -> ", config["ticker"])
    print("FOR START DATE -> ", config["start_date"])
    print("FOR END DATE -> ", config["end_date"])
    ticker = config["ticker"]
    start_date = config["start_date"]
    end_date = config["end_date"]
    start_date_fixed = start_date
    end_date_fixed = end_date
    path = "docker_storage/raw_data"
    dir_list = os.listdir(path)
    print(dir_list)
    available_tickers = []
    for filename in dir_list:
        print(filename)
        if "Tick-Data111" in filename:
            ticker_name = filename.split('-')
            print(ticker_name)
            available_tickers.append(ticker_name[0])
    print(available_tickers)

    if ticker not in available_tickers:
        print(f"FETCHING DATA FOR {ticker}")


        date_lst = []
        while start_date < end_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            start_date += timedelta(days = 1)
            temp_date = start_date
            start_date += timedelta(days = 2)
            temp_date = temp_date.strftime('%Y-%m-%d')
            start_date = start_date.strftime('%Y-%m-%d')
            date_lst.append([temp_date, start_date])
        #date_lst = pickle.load(open('date_list.pkl', 'rb'))

        for start_date, end_date in date_lst:
            dates = []
            for i in daterange(pd.to_datetime(start_date), pd.to_datetime(end_date)):
                dates.append(i.date().strftime("%Y-%m-%d"))
            print(dates)
            asyncio.run(main(dates))
            new_dict = []

            for index, i in enumerate(data_dict):
                if 'results' not in list(data_dict[i].keys()):
                    pass
                else:
                    new_dict = new_dict + data_dict[i]['results']
            df = pd.DataFrame(new_dict)
            if len(df) != 0:
                if "participant_timestamp" not in df.columns:
                    df["participant_timestamp"] = df["sip_timestamp"]
                df['participant_timestamp'] = unix_to_date(df, "participant_timestamp")
                #df['sip_timestamp'] = unix_to_date(df, "sip_timestamp")
                df = df.sort_values(by="participant_timestamp")
                df = df.set_index("participant_timestamp")
                df = df[["price", "size"]]
                df = df.reset_index()
                df["participant_timestamp"] = df["participant_timestamp"]
                ftr_files = glob.glob(os.path.join('./docker_storage/raw_data', f"{ticker}-Tick-Data111.ftr"))
                print('-----------------------------------------------------------------')
                print('ftr:',ftr_files)
                if len(ftr_files) == 0:
                    print('here')
#                     df.to_feather(f"./docker_storage/raw_data/{ticker}-|{start_date_fixed}_{end_date_fixed}|-Tick-Data.ftr")
                    df.to_feather(f"./docker_storage/raw_data/{ticker}-Tick-Data111.ftr")

                else:
                    if 'df3' not in locals():
                        print('here3')

                        df2 = pd.read_feather(f"./docker_storage/raw_data/{ticker}-Tick-Data111.ftr")
                    else:
                        df2 = df3
                    df3 = df2.append(df)
                    df3 = df3.reset_index(drop = True)
                    print('here2')
                    df3.to_feather(f"./docker_storage/raw_data/{ticker}-Tick-Data111.ftr")
                    del df2  # memory flush
                    #del df3
                del df
            else:
                print(f"NO DATA FOR THIS DURATION -> {start_date}-{end_date}")

            del data_dict
            data_dict = {}

        path_loc = f"./docker_storage/raw_data/{ticker}-Tick-Data111.ftr"
        ticker_info = {"name" : ticker, "path" : path_loc, "start-date" : start_date_fixed, "end-date" : end_date_fixed}
        return ticker_info
    else:
        print(f"DATA FOR {ticker} ALREADY EXISTS")
        for filename in dir_list:
            if "Tick-Data" in filename:
                ticker_name = filename.split('-')[0]
                print("ticker_name", ticker_name, "ticker", ticker)
                if ticker_name == ticker:
                    data_name = filename
#                     dates = filename.split('|')[1]
#                     dates = dates.replace('_', '-')
#                     dates = dates.split('-')
#                     start_date = dates[0:3]
#                     start_date = '-'.join(start_date)
#                     end_date = dates[3:6]
#                     end_date = '-'.join(end_date)
                    break
        path_loc = f"./docker_storage/raw_data/{data_name}"
        print(data_name)
        ticker_info = {"name" : ticker, "path" : path_loc, "start-date" : start_date, "end-date" : end_date}
        print('done')
        raw_merged_feather = pd.read_feather(f'./docker_storage/raw_data/{ticker1}-Tick-Data.ftr')
        raw_merged_feather.reset_index(inplace=True, drop=True)
        df = pd.read_feather(f'./docker_storage/raw_data/{ticker1}-Tick-Data111.ftr')
        df.reset_index(inplace=True, drop=True)
        combined_data = pd.concat([raw_merged_feather, df], axis=0, ignore_index=True)
        combined_data.to_feather(f'./docker_storage/raw_data/{ticker1}-Tick-Data.ftr')

        
        
        
        return ticker_info
    
    
    
    
    
    
def working2(ticker):
    config = {
    "ticker": ticker,
    "start_date": "2015-01-01",
    "end_date": "2023-07-19"
    }

    global data_dict
    data_dict = {}
    #ticker_info = {}

    def unix_to_date(dataset, col_name):
        dataset[col_name] = pd.to_datetime(dataset[col_name])
        dataset[col_name] = dataset[col_name].dt.tz_localize('UTC')
        dataset[col_name] = dataset[col_name].dt.tz_convert('US/Eastern')
        dataset[col_name] = dataset[col_name].dt.tz_localize(None)
        return dataset[col_name]


    def daterange(date1, date2):
        for n in range(int((date2 - date1).days) + 1):
            yield date1 + timedelta(n)


    async def get(
        session: aiohttp.ClientSession,
        date: str,
        **kwargs
    ) -> dict:
        global data_dict
        api = f"https://api.polygon.io/v3/trades/{ticker}?timestamp={date}&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
        resp = await session.request('GET', url=api, **kwargs)
        data = await resp.json()
        data_dict[date] = data
        next_url = data.get("next_url", None)
        while next_url is not None:
            next_url_ = next_url+"&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
            resp = await session.request('GET', url=next_url_, **kwargs)
            data = await resp.json()
            data_dict[date]["results"] += data["results"]
            next_url = data.get("next_url", None)


    async def main(dates, **kwargs):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for c in dates:
                tasks.append(get(session=session, date=c, **kwargs))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses

  ############################################################################################

    print("THE CURRENT TICKER IS -> ", config["ticker"])
    print("FOR START DATE -> ", config["start_date"])
    print("FOR END DATE -> ", config["end_date"])
    ticker = config["ticker"]
    start_date = config["start_date"]
    end_date = config["end_date"]
    start_date_fixed = start_date
    end_date_fixed = end_date
    path = "./docker_storage/raw_data"
    dir_list = os.listdir(path)
    available_tickers = []
    for filename in dir_list:
        if "Tick-Data" in filename:
            ticker_name = filename.split('-')
            available_tickers.append(ticker_name[0])
    print(available_tickers)

    if ticker not in available_tickers:
        print(f"FETCHING DATA FOR {ticker}")


        date_lst = []
        while start_date < end_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            start_date += timedelta(days = 1)
            temp_date = start_date
            start_date += timedelta(days = 2)
            temp_date = temp_date.strftime('%Y-%m-%d')
            start_date = start_date.strftime('%Y-%m-%d')
            date_lst.append([temp_date, start_date])
        #date_lst = pickle.load(open('date_list.pkl', 'rb'))

        for start_date, end_date in date_lst:
            dates = []
            for i in daterange(pd.to_datetime(start_date), pd.to_datetime(end_date)):
                dates.append(i.date().strftime("%Y-%m-%d"))
            print(dates)
            asyncio.run(main(dates))
            new_dict = []

            for index, i in enumerate(data_dict):
                if 'results' not in list(data_dict[i].keys()):
                    pass
                else:
                    new_dict = new_dict + data_dict[i]['results']
            df = pd.DataFrame(new_dict)
            if len(df) != 0:
                if "participant_timestamp" not in df.columns:
                    df["participant_timestamp"] = df["sip_timestamp"]
                df['participant_timestamp'] = unix_to_date(df, "participant_timestamp")
                #df['sip_timestamp'] = unix_to_date(df, "sip_timestamp")
                df = df.sort_values(by="participant_timestamp")
                df = df.set_index("participant_timestamp")
                df = df[["price", "size"]]
                df = df.reset_index()
                df["participant_timestamp"] = df["participant_timestamp"]
#3&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                ftr_files = glob.glob(os.path.join('./docker_storage/raw_data', f"{ticker}-Tick-Data.ftr"))

                if len(ftr_files) == 0:
#                     df.to_feather(f"./docker_storage/raw_data/{ticker}-|{start_date_fixed}_{end_date_fixed}|-Tick-Data.ftr")
                    df.to_feather(f"./docker_storage/raw_data/{ticker}-Tick-Data.ftr")

                else:
                    if 'df3' not in locals():
                        df2 = pd.read_feather(f"./docker_storage/raw_data/{ticker}-Tick-Data.ftr")
                    else:
                        df2 = df3
                    df3 = df2.append(df)
                    df3 = df3.reset_index(drop = True)
                    df3.to_feather(f"./docker_storage/raw_data/{ticker}-Tick-Data.ftr")
                    del df2  # memory flush
                    #del df3
                del df
            else:
                print(f"NO DATA FOR THIS DURATION -> {start_date}-{end_date}")

            del data_dict
            data_dict = {}

        path_loc = f"./docker_storage/raw_data/{ticker}-Tick-Data.ftr"
        ticker_info = {"name" : ticker, "path" : path_loc, "start-date" : start_date_fixed, "end-date" : end_date_fixed}
        return ticker_info
    else:
        print(f"DATA FOR {ticker} ALREADY EXISTS")
        for filename in dir_list:
            if "Tick-Data" in filename:
                ticker_name = filename.split('-')[0]
                print("ticker_name", ticker_name, "ticker", ticker)
                if ticker_name == ticker:
                    data_name = filename
#                     dates = filename.split('|')[1]
#                     dates = dates.replace('_', '-')
#                     dates = dates.split('-')
#                     start_date = dates[0:3]
#                     start_date = '-'.join(start_date)
#                     end_date = dates[3:6]
#                     end_date = '-'.join(end_date)
                    break
        path_loc = f"./docker_storage/raw_data/{data_name}"
        ticker_info = {"name" : ticker, "path" : path_loc, "start-date" : start_date, "end-date" : end_date}
        return ticker_info



def get_data():

    ticker = 'MSFT'
    config = {
	"ticker": ticker,
    "start_date": "2015-01-01",
    "end_date": "2023-07-19"
    }
    ticker1 = ticker
    import os

#     cpath =  f'./docker_storage/raw_data/{ticker1}-Tick-Data.ftr'

    import os

# Get the current working directory
    current_directory = os.path.abspath(os.getcwd())

    # Print the current directory
    print("Current directory:", current_directory)

    cpath =  f'./docker_storage/raw_data/{ticker1}-Tick-Data.ftr'
    cpath2 =  f'./storage/docker_storage/raw_data/ENPH-Tick-Data.ftr'
    cpath3 =  f'/docker_storage/raw_data/ENPH-Tick-Data.ftr'
    cpath4 =  f'./raw_data/ENPH-Tick-Data.ftr'
    print(cpath)

    if os.path.exists(cpath):
        print('thus')
        a = working(ticker1)
        return a
        print('thus1')
        
    elif os.path.exists(cpath2) or os.path.exists(cpath3) or os.path.exists(cpath4):
        print('2')

    else:
        a = working2(ticker1)
        return a
        print('thus22')        