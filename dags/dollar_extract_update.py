
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
import re
from datetime import datetime,date

def get_dollar_data_updated(tickers):
    tickers = tickers
#     print(tickers)
#     ticker = 'TSLA'
    ticker = tickers
#     print(ticker)
    year = date.today().year
    current_folder = f'./docker_storage/Raw_Data_New/{ticker}_{year}/'
    # print(year)
    # print(today)
#     print(current_folder)
    if os.path.exists(current_folder):
        exist = os.listdir(current_folder)
#         print('exist',exist)
        dates = [re.search(r"(\d{4}-\d{2}-\d{2})", f).group(1) for f in exist]
        date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in dates]
        sorted_dates = sorted(date_objects)
#         last_date = sorted_dates[-1]
        last_date = sorted_dates[-2]
        last_date = last_date.strftime("%Y-%m-%d")
#         print(last_date)
#         print(last_date1)
        start_date = str(last_date)
        

        today = date.today()
        end_date = str(today)  
        
#         end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()

#         # Subtract one day to get the start_date
#         start_date_obj = end_date_obj - timedelta(days=4)
#         start_date = start_date_obj.strftime("%Y-%m-%d")

#         print('end_date',end_date)
#         print('start_date',start_date)

    else:
        print('past data doesnt exist')
    
    print('end_date',type(end_date))
    print('start_date',type(start_date))
    

    

    global data_dict,yearl
    yearl = []
    data_dict = {}
    ticker_info = {}

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

    print("THE CURRENT TICKER IS -> ", ticker)
    print("FOR START DATE -> ", start_date)
    print("FOR END DATE -> ", end_date)
    ticker = ticker
    start_date = start_date
    end_date = end_date
    start_date_fixed = start_date
    end_date_fixed = end_date
    path='./docker_storage/'
    pathh = './docker_storage/Raw_Data_New/'
#     dir_list = os.listdir(path)
    dir_list2 = os.listdir(pathh)
#     print('dir_list',dir_list)
#     print('dir_list2',dir_list2)
    available_tickers = []
    for filename in dir_list2:
        if "Tick-Data" in filename:
            ticker_name = filename.split('-')
            available_tickers.append(ticker_name[0])
#         print(available_tickers)
    
    if ticker not in available_tickers:
        print(f"FETCHING DATA FOR {ticker}")
    
        date_lst = []
        while start_date < end_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            start_date += timedelta(days = 1)
            temp_date = start_date
            start_date += timedelta(days = 2)
            temp_date = temp_date.strftime('%Y-%m-%d')
            start_date = start_date.strftime('%Y-%m-%d')
            date_lst.append([temp_date, start_date])
        dates = []
        for start_date, end_date in date_lst:
            
            for i in daterange(pd.to_datetime(start_date), pd.to_datetime(end_date)):
                dates.append(i.date().strftime("%Y-%m-%d"))
        
        for i in dates:
            curr_date = [i]
#             print((curr_date[0]))
            year = curr_date[0].split('-')[0]
            if year not in yearl:
                yearl.append(year)
#             print('year',year)
            asyncio.run(main(curr_date))
            new_dict = []
            

            # Directly use the fetched data for the current date
            if 'results' not in data_dict[curr_date[0]].keys():
                print(f"NO DATA FOR DATE -> {curr_date[0]}")
                continue
            else:
                new_dict = data_dict[curr_date[0]]['results']
#             print('raw_path',raw_path)
            df = pd.DataFrame(new_dict)
            file_path = f'./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr' 
            raw_path = f'./docker_storage/Raw_Data_New/{ticker}_{year}/'
#             print('raw_path',raw_path)
            if not os.path.exists(raw_path):
                os.makedirs(raw_path)
#                 print('enter')
                if not os.path.exists(file_path):
                    
#                     print('file_path not exist',file_path)
#                     print('created')

                    if len(df) != 0:
                        if "participant_timestamp" not in df.columns:
                            df["participant_timestamp"] = df["sip_timestamp"]
                        df['participant_timestamp'] = unix_to_date(df, "participant_timestamp")
                        df = df.sort_values(by="participant_timestamp")
                        df = df.set_index("participant_timestamp")
                        df = df[["price", "size"]]
                        df = df.reset_index()
                        df["participant_timestamp"] = df["participant_timestamp"]
#                         print(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
                        df.to_feather(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
#                         print('zero')
                        del df
                    else:
                        print(f"NO DATA FOR THIS DATE -> {curr_date[0]}")

                    del data_dict[curr_date[0]]     
                    
                else:
#                     os.makedirs(raw_path)
                    print('already exist')
#                     print('file_path exist',file_path)

                    if len(df) != 0:
                        if "participant_timestamp" not in df.columns:
                            df["participant_timestamp"] = df["sip_timestamp"]
                        df['participant_timestamp'] = unix_to_date(df, "participant_timestamp")
                        df = df.sort_values(by="participant_timestamp")
                        df = df.set_index("participant_timestamp")
                        df = df[["price", "size"]]
                        df = df.reset_index()
                        df["participant_timestamp"] = df["participant_timestamp"]
#                         print(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
                        df.to_feather(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
                        print('zero')
                        del df
                    else:
                        print(f"NO DATA FOR THIS DATE -> {curr_date[0]}")

                    del data_dict[curr_date[0]]     
                    
                
            else:
                dir_list1 = os.listdir(f"./docker_storage/Raw_Data_New/{ticker}_{year}")
                if not os.path.exists(file_path):
#                     print('file_path not exist1',file_path)
                    if len(df) != 0:
                        if "participant_timestamp" not in df.columns:
                            df["participant_timestamp"] = df["sip_timestamp"]
                        df['participant_timestamp'] = unix_to_date(df, "participant_timestamp")
                        df = df.sort_values(by="participant_timestamp")
                        df = df.set_index("participant_timestamp")
                        df = df[["price", "size"]]
                        df = df.reset_index()
                        df["participant_timestamp"] = df["participant_timestamp"]
#                         print(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
                        df.to_feather(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
#                         print('zero')
                        del df
                    else:
                        print(f"NO DATA FOR THIS DATE -> {curr_date[0]}")

                else:
                    print('file_path exist1',file_path)
#                     continue

                    if len(df) != 0:
                        if "participant_timestamp" not in df.columns:
                            df["participant_timestamp"] = df["sip_timestamp"]
                        df['participant_timestamp'] = unix_to_date(df, "participant_timestamp")
                        df = df.sort_values(by="participant_timestamp")
                        df = df.set_index("participant_timestamp")
                        df = df[["price", "size"]]
                        df = df.reset_index()
                        df["participant_timestamp"] = df["participant_timestamp"]
                        print(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
#                         df.to_feather(f"./docker_storage/Raw_Data_New/{ticker}_{year}/{ticker}_{curr_date[0]}_Tick-Data.ftr")
                        print('zero')
                        del df
                    else:
                        print(f"NO DATA FOR THIS DATE -> {curr_date[0]}")

                    del data_dict[curr_date[0]]     
                
           
                print('deleting')
#                 del data_dict[curr_date[0]]
    raw_path = f'./docker_storage/Raw_Data_New/{ticker}_{year}/'
    path_loc =  raw_path
    year_path = [year]
    ticker_info = {"name" : ticker, "path" : path_loc,'year_path':year_path}
    print(ticker_info)
    return ticker_info

