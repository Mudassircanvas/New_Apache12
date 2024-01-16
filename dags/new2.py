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


def dollar_adjusting_update(ticker_info):
    print(ticker_info)
    ticker = ticker_info['name']
    path = ticker_info['path']
    yearpath = ['2023']
    for inn in [ticker]:
        try:
            newp = f'./docker_storage/Raw_Data_New/{ticker}_2023/'
            path = newp            
            print('------------------------------------')
    #         print(inn)
            KEY = 'Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB'
            TICKER = inn
            print('problem',TICKER)
            dirs = os.listdir(path)
            dirs_pathl = len(os.listdir(path))
            print(dirs)
            api_ = f'https://api.polygon.io/v3/reference/splits?ticker={TICKER}&sort=execution_date&apiKey={KEY}'
            print(f'{TICKER}&sort=execution_date&apiKey={KEY}')
            print('dirs_len',dirs_pathl)
            for i in range(dirs_pathl):
                dirs_path = dirs[i]
    #             print('dirs_path',dirs_path)
                real_path = path+dirs_path
                name = dirs[i]
                parts = name.split("_")
                stock_ticker = parts[0]
                date = parts[1]
                year = date.split("-")[0]
                split_folder = './docker_storage/Raw_Data_New/Raw_Data_Splitted/'
                split_path = f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/{stock_ticker}_{date}_Splitted_data.ftr'
    #             print('split_path',split_path)
                raw_path = f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/'
                if not os.path.exists(raw_path):
                    os.makedirs(raw_path)
                    if not os.path.exists(split_path):
    #                     print('enterrrrrrrrrrrr')
    #                     print('split_path',split_path)
                        df = pd.read_feather(real_path)
                        df = df.reset_index(drop = True)

                        df=df[['participant_timestamp','price','size']].copy()
                        print(len(df))
                        try:
                            data_split = requests.get(api_).json()
        #                     print('%%%%%%%%%%%%%%%%%%%%')
                            print(f'{TICKER}&sort=execution_date&apiKey={KEY}')
                            df_splits = pd.DataFrame(data_split["results"])
        # 
        #                     print('df_splits.columns',df_splits.columns)
                            df_splits = df_splits.sort_values(by = "execution_date", ascending = False)
                            df_splits = df_splits.reset_index(drop = True)

                            for row in df_splits.itertuples():
                                df_temp = df[df["participant_timestamp"] < row.execution_date]

                                if len(df_temp) != 0:
                                    ratio = row.split_to / row.split_from
                                    df_temp["price"] = df_temp["price"] / ratio
                                    df_temp["size"] = df["size"] * ratio
                                    df[df["participant_timestamp"] < row.execution_date] = df_temp
                                    print(len(df_temp))
                        except:
                            print('here1')
                            print(len(df))
                            
                            
                            else:
                                df.to_feather(f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/{stock_ticker}_{date}_Splitted_data.ftr')
                                print(f'No data to be adjusted for {row.execution_date}')
                        df.to_feather(f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/{stock_ticker}_{date}_Splitted_data.ftr')

#                         del df





                else:
                    if not os.path.exists(split_path):
                        df = pd.read_feather(real_path)
                        df = df.reset_index(drop = True)

                        df=df[['participant_timestamp','price','size']].copy()
                        print(len(df))
                        try:
                            data_split = requests.get(api_).json()
                            print(len(data_split))
                            print('%%%%%%%%%%%%%%%%%%%%')
                            print(f'{TICKER}&sort=execution_date&apiKey={KEY}')
                            df_splits = pd.DataFrame(data_split["results"])

                            print('df_splits.columns',df_splits.columns)
                            df_splits = df_splits.sort_values(by = "execution_date", ascending = False)
                            df_splits = df_splits.reset_index(drop = True)

                            for row in df_splits.itertuples():
                                df_temp = df[df["participant_timestamp"] < row.execution_date]
                                print('test1')
                                print(row)
                                print(len(df_temp))
                                if len(df_temp) != 0:
                                    ratio = row.split_to / row.split_from
                                    df_temp["price"] = df_temp["price"] / ratio
                                    df_temp["size"] = df["size"] * ratio
                                    df[df["participant_timestamp"] < row.execution_date] = df_temp
                                    print(len(df))
                        except:
                            print('here2')
                            print(len(df))
                            
                            
                            df.to_feather(f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/{stock_ticker}_{date}_Splitted_data.ftr')
                            else:
                                print(f'No data to be adjusted for {row.execution_date}')
                        df.to_feather(f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/{stock_ticker}_{date}_Splitted_data.ftr')
#                         del df
#         except:
#             print('here3')
# #             df=
#             print(len(df))
#             df.to_feather(f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{stock_ticker}_{year}_split/{stock_ticker}_{date}_Splitted_data.ftr')
#             del df
