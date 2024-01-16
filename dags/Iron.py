import pandas as pd
import pickle
import glob
import os.path
from datetime import datetime, timedelta
# import matplotlib.pyplot as plt
import pandas as pd
import talib
import random
from joblib import Parallel, delayed
from numpy.random import randint
pd.options.mode.chained_assignment = None
# import seaborn as sns
import time
import numpy as np
from numpy import random
from numpy.random import randint
from datetime import datetime
from datetime import timedelta
# from faker import Faker
# import exchange_calendars as xcals
import numpy as np
from sklearn.model_selection import train_test_split
import gzip
import requests
import talib
import json
# plt.rcParams["figure.figsize"] = (20,10)
pd.set_option('display.float_format', '{:.6f}'.format)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)




def splite(ticker_info):
    ticker = ticker_info['name']
    path = f'./docker_storage/Raw_Data_New/full_file/{ticker}_full.ftr'
    path1 = f'./docker_storage/Raw_Data_New/full_file/{ticker}_full.ftr'
#     print(path)
#     print(path1)
    for i in [ticker]:
        try:
            print('------------------------------------')
            print(i)
            KEY = 'Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB'
            TICKER = i

            api_ = f'https://api.polygon.io/v3/reference/splits?ticker={TICKER}&sort=execution_date&apiKey={KEY}'

#             print('here1')
            df = pd.read_feather(path1)
            # df = df[df["participant_timestamp"] > "2021-01-01"]
            df = df.reset_index(drop = True)
#             print('here2')

            df=df[['participant_timestamp','price','size']].copy()

            data_split = requests.get(api_).json()
            df_splits = pd.DataFrame(data_split["results"])
            df_splits = df_splits.sort_values(by = "execution_date", ascending = False)
            df_splits = df_splits.reset_index(drop = True)


            for row in df_splits.itertuples():
                df_temp = df[df["participant_timestamp"] < row.execution_date]
                if len(df_temp) != 0:
#                     print(f'Adjusting splits for {row.execution_date}...')
                    ratio = row.split_to / row.split_from
                    df_temp["price"] = df_temp["price"] / ratio
                    df_temp["size"] = df["size"] * ratio
                    df[df["participant_timestamp"] < row.execution_date] = df_temp
                else:
                    print(f'No data to be adjusted for {row.execution_date}')


#             df.to_feather(f"{TICKER}-2015-01-01_2023-05-31-Tick-Data-SplitAdjusted.ftr")
            df.to_feather(f"./docker_storage/Raw_Data_New/Raw_Data_Splitted/full_file/{TICKER}_full_split.ftr")
#             print('done')
#             ticker_info["impute-path"] = f"./docker_storage/raw_data/split_adjusted/{TICKER}-Tick-splitted-Data.ftr"
            ticker_info["resample-path"] = f'./docker_storage/Time_tick/{TICKER}_TimeDF_const_BarsPerDay.ftr'
            return ticker_info
#             ./docker_storage/raw_data/split_adjusted/{TICKER}-Tick-Data.ftr
#             del df
            gc.collect()
        except:
#             print('enter here')
            df.to_feather(f"./docker_storage/Raw_Data_New/Raw_Data_Splitted/full_file/{TICKER}_full_split.ftr")
#             ticker_info["impute-path"] = f"./docker_storage/raw_data/split_adjusted/{TICKER}-Tick-splitted-Data.ftr"
            
            print(f'{i} no splits')
            return ticker_info
        
        


def splite_after(ticker_info):
    ticker = ticker_info['name']
    path = f'./docker_storage/after_raw_data/full_file/{ticker}_full.ftr'
    path1 = f'./docker_storage/after_raw_data/full_file/{ticker}_full.ftr'
#     print(path)
#     print(path1)
    for i in [ticker]:
        try:
            print('------------------------------------')
            print(i)
            KEY = 'Ot5XxPIdM4IAsPj6TdlIqHajQFK356JB'
            TICKER = i

            api_ = f'https://api.polygon.io/v3/reference/splits?ticker={TICKER}&sort=execution_date&apiKey={KEY}'

#             print('here1')
            df = pd.read_feather(path1)
            # df = df[df["participant_timestamp"] > "2021-01-01"]
            df = df.reset_index(drop = True)
#             print('here2')

            df=df[['participant_timestamp','price','size']].copy()

            data_split = requests.get(api_).json()
            df_splits = pd.DataFrame(data_split["results"])
            df_splits = df_splits.sort_values(by = "execution_date", ascending = False)
            df_splits = df_splits.reset_index(drop = True)


            for row in df_splits.itertuples():
                df_temp = df[df["participant_timestamp"] < row.execution_date]
                if len(df_temp) != 0:
#                     print(f'Adjusting splits for {row.execution_date}...')
                    ratio = row.split_to / row.split_from
                    df_temp["price"] = df_temp["price"] / ratio
                    df_temp["size"] = df["size"] * ratio
                    df[df["participant_timestamp"] < row.execution_date] = df_temp
                else:
                    print(f'No data to be adjusted for {row.execution_date}')


#             df.to_feather(f"{TICKER}-2015-01-01_2023-05-31-Tick-Data-SplitAdjusted.ftr")
            df.to_feather(f"./docker_storage/after_raw_data/Raw_Data_Splitted/full_file/{TICKER}_full_split.ftr")
#             print('done')
#             ticker_info["impute-path"] = f"./docker_storage/raw_data/split_adjusted/{TICKER}-Tick-splitted-Data.ftr"
            ticker_info["resample-path"] = f'./docker_storage/Time_tick/{TICKER}_TimeDF_const_BarsPerDay.ftr'
            return ticker_info
#             ./docker_storage/raw_data/split_adjusted/{TICKER}-Tick-Data.ftr
#             del df
            gc.collect()
        except:
#             print('enter here')
            df.to_feather(f"./docker_storage/after_raw_data/Raw_Data_Splitted/full_file/{TICKER}_full_split.ftr")
#             ticker_info["impute-path"] = f"./docker_storage/raw_data/split_adjusted/{TICKER}-Tick-splitted-Data.ftr"
            
            print(f'{i} no splits')
            return ticker_info