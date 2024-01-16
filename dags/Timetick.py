
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

def k(ticker_info):
    print(ticker_info)
    def unix_to_date(dataset, col_name):
        dataset[col_name] = pd.to_datetime(dataset[col_name], unit='ms')
        dataset[col_name] = dataset[col_name].dt.tz_localize('UTC')
        dataset[col_name] = dataset[col_name].dt.tz_convert('US/Eastern')
        dataset[col_name] = dataset[col_name].dt.tz_localize(None)
#         print('this')
        return dataset[col_name]


    def daterange(date1, date2):
#         print('this1')
        for n in range(int((date2 - date1).days) + 1):

            yield date1 + timedelta(n)

    global data_dict
    data_dict = {}
    
    new_dict = []
    
    
    tickerr= ticker_info['name']
    start = str(ticker_info['start-date'])
    prev_date = str(ticker_info['end-date'])
    print('jeeeeeeeeeeeee1')
    print(tickerr,str(start),prev_date)
    
    
    async def get(
        session: aiohttp.ClientSession,
        date: str,
        **kwargs
    ) -> dict:
        global data_dict
        api = f"https://api.polygon.io/v2/aggs/ticker/{tickerr}/range/15/minute/{date}/{date}?adjusted=true&sort=asc&limit=1440&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
        # api = f"https://api.polygon.io/v3/trades/NVDA?timestamp={date}&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
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
#             print('this2')




    async def main(dates, **kwargs):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for c in dates:
                tasks.append(get(session=session, date=c, **kwargs))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
#             print('this3')
            return responses
    


    cd = 0
    if cd == 0 :
        
            
#         raw_merged_feather = pd.read_feather(f'./docker_storage/Tick_Data/AdjustedData/{tickerr}-Tick-Data111.ftr')
#         raw_merged_feather.reset_index(inplace=True, drop=True)
#         print(raw_merged_feather)
#         raw_merged_feather['date'] = raw_merged_feather['participant_timestamp'].dt.date
#         start = raw_merged_feather['date'].head(1)
#         start = pd.Series(start)
#         start = start.values[0]
#         start = start.strftime("%Y-%m-%d")
#         print(start)

        
#         a = raw_merged_feather['date'].tail(1)
#         a = pd.Series(a)
#         a = a.values[0]
#         prev_date = a.strftime("%Y-%m-%d")
        start = str(ticker_info['start-date'])
        prev_date = str(ticker_info['end-date'])
        start_date = start
        end_date = prev_date
        print('p',prev_date)
#         today_date = datetime.today()
#         today_date = today_date.strftime("%Y-%m-%d")
#         print('t',today_date)


#         start_date = start
#         end_date = prev_date
#         start_date = '2023-05-01'
#         start_date = '2023-06-14'
#         end_date = '2023-05-08'
        dates = []
        for i in daterange(pd.to_datetime(start_date), pd.to_datetime(end_date)):
            dates.append(i.date().strftime("%Y-%m-%d"))
        print(dates)
        asyncio.run(main(dates))
#     new_dict = []



    for index,i in enumerate(data_dict):

        if 'results' not in list(data_dict[i].keys()):
            pass
        else:
            new_dict = new_dict + data_dict[i]['results']
    df = pd.DataFrame(new_dict)
    df = pd.DataFrame(new_dict)
    print(df)
    df['t'] = unix_to_date(df, "t")
    #df['sip_timestamp'] = unix_to_date(df, "sip_timestamp")
    df = df.sort_values(by = "t")
    df = df.set_index("t")
    df.index=pd.to_datetime(df.index)
    df=df.between_time('9:30', '15:30')
    df.reset_index(inplace=True)
    time_df = df.copy()
    df.to_feather(f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay11.ftr')
    raw_merged_feather = pd.read_feather(f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr')
    raw_merged_feather.reset_index(inplace=True, drop=True)
    df = pd.read_feather(f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay11.ftr')
    df.reset_index(inplace=True, drop=True)
    combined_data = pd.concat([raw_merged_feather, df], axis=0, ignore_index=True)
    combined_data.to_feather(f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr')

    
    
    
    ticker_info['resample-path'] = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
    ticker_info["impute-path"] = f"./docker_storage/raw_data/split_adjusted/{tickerr}-Tick-splitted-Data.ftr"
    ticker_info['resample-path'] = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
    
    
    return ticker_info

def k2(ticker_info):
    
    def unix_to_date(dataset, col_name):
        dataset[col_name] = pd.to_datetime(dataset[col_name], unit='ms')
        dataset[col_name] = dataset[col_name].dt.tz_localize('UTC')
        dataset[col_name] = dataset[col_name].dt.tz_convert('US/Eastern')
        dataset[col_name] = dataset[col_name].dt.tz_localize(None)
#         print('this')
        return dataset[col_name]


    def daterange(date1, date2):
#         print('this1')
        for n in range(int((date2 - date1).days) + 1):

            yield date1 + timedelta(n)

    global data_dict
    data_dict = {}
    
    new_dict = []
    
    
    tickerr=ticker_info['name']
    start = ticker_info['start-date']
    prev_date = ticker_info['end-date']
    print('jeeeeeeeeeeeeeTTTTTTTTTTTTTTTTTTTTTTT')
    print(tickerr,start,tickerr)
    
    
    async def get(
        session: aiohttp.ClientSession,
        date: str,
        **kwargs
    ) -> dict:
        global data_dict
        api = f"https://api.polygon.io/v2/aggs/ticker/{tickerr}/range/15/minute/{date}/{date}?adjusted=true&sort=asc&limit=1440&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
        # api = f"https://api.polygon.io/v3/trades/NVDA?timestamp={date}&apiKey=Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB&limit=50000"
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
#             print('this2')




    async def main(dates, **kwargs):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for c in dates:
                tasks.append(get(session=session, date=c, **kwargs))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
#             print('this3')
            return responses
    

    cd = 0
    if cd == 0 :
        
        
            
#         raw_merged_feather = pd.read_feather(f'./docker_storage/Tick_Data/AdjustedData/{tickerr}-Tick-Data111.ftr')
#         raw_merged_feather.reset_index(inplace=True, drop=True)
#         print(raw_merged_feather)
#         raw_merged_feather['date'] = raw_merged_feather['participant_timestamp'].dt.date
#         start = raw_merged_feather['date'].head(1)
#         start = pd.Series(start)
#         start = start.values[0]
#         start = start.strftime("%Y-%m-%d")
#         print(start)
        start = str(ticker_info['start-date'])
        prev_date = str(ticker_info['end-date'])
        start_date = start
        end_date = prev_date
        print('p',prev_date)
#      
        
#         a = raw_merged_feather['date'].tail(1)
#         a = pd.Series(a)
#         a = a.values[0]
#         prev_date = a.strftime("%Y-%m-%d")
        
        
#         print('p',prev_date)
# #         today_date = datetime.today()
# #         today_date = today_date.strftime("%Y-%m-%d")
# #         print('t',today_date)


#         start_date = start
#         end_date = prev_date
# #         start_date = '2023-05-01'
#         # start_date = '2023-06-14'
# #         end_date = '2023-05-08'
        dates = []
        for i in daterange(pd.to_datetime(start_date), pd.to_datetime(end_date)):
            dates.append(i.date().strftime("%Y-%m-%d"))
        print(dates)
        asyncio.run(main(dates))
#     new_dict = []



    for index,i in enumerate(data_dict):

        if 'results' not in list(data_dict[i].keys()):
            pass
        else:
            new_dict = new_dict + data_dict[i]['results']
    df = pd.DataFrame(new_dict)
    df = pd.DataFrame(new_dict)
    print(df)
    df['t'] = unix_to_date(df, "t")
    #df['sip_timestamp'] = unix_to_date(df, "sip_timestamp")
    df = df.sort_values(by = "t")
    df = df.set_index("t")
    df.index=pd.to_datetime(df.index)
    df=df.between_time('9:30', '15:30')
    df.reset_index(inplace=True)
    time_df = df.copy()
    df.to_feather(f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr')
    ticker_info['resample-path'] = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
    ticker_info["impute-path"] = f"./docker_storage/raw_data/split_adjusted/{tickerr}-Tick-splitted-Data.ftr"
    ticker_info['resample-path'] = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
    
    return ticker_info
            


def get_data1(ticker_info):

    
        
    tickerr=ticker_info['name']
    start = ticker_info['start-date']
    prev_date = ticker_info['end-date']
    
    
    import os

    cpath =  f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
    import os

# Get the current working directory
    current_directory = os.getcwd()

    # Print the current directory
    print("Current directory:", current_directory)

    if os.path.exists(cpath):
        print('thus')
        a = k(ticker_info)
        ticker_info['resample-path'] = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
        return a
        print('thus1')

    else:
        a = k2(ticker_info)
#         ticker_info['resample-path'] = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
        return a
        print('thus22')        