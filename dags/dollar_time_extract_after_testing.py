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

def kit_aftert(ticker_info):
    
    
    print(ticker_info)
    tickerr=ticker_info['name']
    start = ticker_info['start_date']
    prev_date = ticker_info['end_date']
    print('jeeeeeeeeeeeeeTTTTTTTTTTTTTTTTTTTTTTT')
    print(tickerr,start,tickerr)

    
    global data_dict,yearl
    yearl = []
    data_dict = {}
    new_dict = []    

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

#     global data_dict
#     data_dict = {}

    
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
        start = str(start)
        prev_date = str(prev_date)
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


        for i in dates:
            curr_date = [i]
            year = curr_date[0].split('-')[0]
            if year not in yearl:
                yearl.append(year)
#             print('year', year)
            asyncio.run(main(curr_date))

# ...[rest of your code above the for loop]

            keys_to_remove = []  # This will store keys to be removed after iteration

            for index, i in enumerate(list(data_dict.keys())):  # Convert keys to list before iteration
                if 'results' not in list(data_dict[i].keys()):
                    continue  # skip this iteration if 'results' key is not found

                curr_date_data = data_dict[i]['results']
                df = pd.DataFrame(curr_date_data)

                file_path = f'./docker_storage/Time_after/{tickerr}_{year}_market/{tickerr}_{curr_date[0]}_Time_Tick-Data.ftr'
                raw_path = f'./docker_storage/Time_after/{tickerr}_{year}_market/'

                if not os.path.exists(raw_path):
                    os.makedirs(raw_path)
                    print('Directory created:', raw_path)
                else:
                    print('Directory exists:', raw_path)

                if not os.path.exists(file_path):
                    if len(df) != 0:
#                         l
                        df['t'] = unix_to_date(df, "t")
                        df = df.sort_values(by="t")
                        df = df.set_index("t")
                        df.index = pd.to_datetime(df.index)
# #                         df = df.between_time('15:30', '09:30')
                        df_afternoon = df.between_time('15:30', '19:45')
#                         print(df_afternoon)
                        df_morning = df.between_time('04:00', '09:30')
#                         print(df_morning)
                        df_combined = pd.concat([df_morning, df_afternoon]).sort_index()
                        df_combined.reset_index(inplace=True)
#                         print(df_combined)


                        
#                         df.reset_index(inplace=True)

# #                         l
#                         df['t'] = unix_to_date(df, "t")
#                         df = df.sort_values(by="t")
#                         df = df.set_index("t")
#                         df.index = pd.to_datetime(df.index)
#                         print(df)
#                         # Filtering for the two time ranges

#                         df_morning = df.between_time('04:00', '09:30')
#                         print(df_morning)
#                         # Combining the two filtered dataframes
#                         df_combined = pd.concat([df_morning, df_afternoon]).sort_index()

#                         df_combined.reset_index(inplace=True)
#                         df_combined

        
                        df_combined.to_feather(file_path)
#                         print('Data saved to:', file_path)
                    else:
                        print(f"NO DATA FOR THIS DATE -> {curr_date[0]}")

                keys_to_remove.append(i)  # Append the key to the list instead of deleting right away

            # Now, outside the loop, you can remove the keys
            for key in keys_to_remove:
                del data_dict[key]
    return ticker_info
            # ...[rest of your code]
