from datetime import datetime
from pandas import Timestamp
import pyarrow
import pickle
import numpy as np
from mlfinlab.microstructural_features import encoding, entropy
from mlfinlab.microstructural_features import second_generation
from mlfinlab.microstructural_features.misc import get_avg_tick_size, vwap
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
import requests
import os
import glob
import nest_asyncio
nest_asyncio.apply()
import asyncio
import aiohttp  #
import datetime
import requests
import pandas as pd
import pytz
from datetime import time as tm
from datetime import date as dt
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
import gc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#import seaborn as sns
# from mlfinlab.data_structures import imbalance_data_structures, standard_data_structures
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
import gc
import talib




def feats(df, tic, i):
    print('enter1')

#     if len(df)==0:
#         print('no df')
#         print(i)
#         return 0

#     tradeable H,L

#     if tic not in ['BTC', 'ETH']:

#         df_=df.between_time('9:30', '15:59')
#         if len(df_)!=0:
# #             tradeableO=df.price[0]
#             tradeableH=df_.price.quantile(0.98)
#             tradeableL=df_.price.quantile(0.02)
# #             tradeableC=df.price[-1]

#         else:
# #             tradeableO=np.nan
#             tradeableH=np.nan
#             tradeableL=np.nan
# #             tradeableC=np.nan

#     else:

#         tradeableH=df.price.quantile(0.98)
#         tradeableL=df.price.quantile(0.02)


    df.reset_index(inplace=True, drop=True)


#     OHLCV, DV, BV, BDV
    df['dv']=df['price']*df['size']
#     O=list(df.price)[0]
#     H=df.price.quantile(0.99)
#     L=df.price.quantile(0.01)
#     C=list(df.price)[-1]
    V=df['size'].sum()
    DV=df['dv'].sum()
    BV=df[df.tick_rule==1]['size'].sum()
    BDV=df[df.tick_rule==1].dv.sum()



#     sigma encoding & lempel_ziv & shanon entropy
    rets=df.tic_pct
    try:
        ed=encoding.sigma_mapping(rets, 0.001)
        ePt1Pct=encoding.encode_array(rets, ed)

        lempel_ziv_Pt1Pct=entropy.get_lempel_ziv_entropy(ePt1Pct)
        shanon_Pt1Pct=entropy.get_shannon_entropy(ePt1Pct)

    except:
        lempel_ziv_Pt1Pct=np.nan
        shanon_Pt1Pct=np.nan



    try:
        ed=encoding.sigma_mapping(rets, 0.005)
        ePt5Pct=encoding.encode_array(rets, ed)

        lempel_ziv_Pt5Pct=entropy.get_lempel_ziv_entropy(ePt5Pct)
        shanon_Pt5Pct=entropy.get_shannon_entropy(ePt5Pct)

    except:
        lempel_ziv_Pt5Pct=np.nan
        shanon_Pt5Pct=np.nan


    try:
        ed=encoding.sigma_mapping(rets, 0.01)
        e1Pct=encoding.encode_array(rets, ed)

        lempel_ziv_1Pct=entropy.get_lempel_ziv_entropy(e1Pct)
        shanon_1Pct=entropy.get_shannon_entropy(e1Pct)

    except:
        lempel_ziv_1Pct=np.nan
        shanon_1Pct=np.nan


# second gen microstructural features

    try:
        tb_kyle=second_generation.get_trades_based_kyle_lambda(df.tic_diff, df['size'], df.tick_rule)
        tb_kyle_coef=tb_kyle[0]
        tb_kyle_t=tb_kyle[1]
    except:
        tb_kyle_coef=np.nan
        tb_kyle_t=np.nan

    try:
        tb_amihud=second_generation.get_trades_based_amihud_lambda(df.log_ret, df['dv'])
        tb_amihud_coef=tb_amihud[0]
        tb_amihud_t=tb_amihud[1]
    except:
        tb_amihud_coef=np.nan
        tb_amihud_t=np.nan

    try:
        tb_hasbrouk=second_generation.get_trades_based_hasbrouck_lambda(df.log_ret, df['dv'], df.tick_rule)
        tb_hasbrouk_coef=tb_hasbrouk[0]
        tb_hasbrouk_t=tb_hasbrouk[1]

    except:
        tb_hasbrouk_coef=np.nan
        tb_hasbrouk_t=np.nan


# misc
    try:
        _vwap=vwap(df.dv, df['size'])
        avg_tick_size=get_avg_tick_size(df['size'])
    except:
        _vwap=np.nan
        avg_tick_size=np.nan




    return [lempel_ziv_Pt1Pct, shanon_Pt1Pct, lempel_ziv_Pt5Pct, shanon_Pt5Pct, lempel_ziv_1Pct, shanon_1Pct,
           tb_kyle_coef, tb_kyle_t, tb_amihud_coef, tb_amihud_t, tb_hasbrouk_coef, tb_hasbrouk_t,
           _vwap, avg_tick_size]



def wit_after(ticker_info):
    print('enter0')
#     print(ticker_info)
    print('///////')
    print(ticker_info)
#     tickerr = 'BA'
    tickerr = ticker_info['name']
    import os

    # Get the current working directory
    current_directory = os.getcwd()
#     DOCKER EXECUTE BIN BASH
    # Print the current directory
    print("Current directory:", current_directory)
    resample_path = ticker_info['resample-path']
#     resample_path = f'./docker_storage/raw_data/split_adjusted/{tickerr}-Tick-splitted-Data.ftr'
#     resample_path = f'./docker_storage/Time_tick/{tickerr}_TimeDF_const_BarsPerDay.ftr'
#                                        Time_tick/{TICKER}_TimeDF_const_BarsPerDay.ftr
    
#     docker_storage/
    print(resample_path)
    print('jaaaaaaaaa')

    df = pd.read_feather(resample_path)
    print('print',df)
    dates=df.t
    timestamps_new=[]
    for count, currDate in enumerate(list(dates)):
        if count >0:
            timestamps_new.append((dates[count-1],currDate))

    timestamps_new=pd.Series(timestamps_new)
    timestamps_new.drop_duplicates(keep='first', inplace=True)
    timestamps_new=list(timestamps_new)
    impute_path = f'./docker_storage/Raw_Data_New/full_file/{tickerr}_full.ftr'
    print("Loading DataSet ")
    df = pd.read_feather(impute_path) ## change path read date and time
    df['participant_timestamp'] = pd.to_datetime(df['participant_timestamp'])
    df.set_index('participant_timestamp', inplace=True)
    df['tic_diff']=df.price.diff()
    df['tic_pct']=df.price.pct_change()
    df['price_shift']=df['price'].shift()
    df['log_ret']=np.log(df['price']/df['price_shift'])
    df.loc[(df.tic_diff>0), 'tick_rule']=1
    df.loc[(df.tic_diff<0), 'tick_rule']=-1   
    df['tick_rule'] = df['tick_rule'].ffill()

    print('tick rule done ')
    # df.dropna(inplace=True)
    df=df.iloc[15:, :]
    #     df.sort_index(ascending=True, inplace=True)
    print('Dataset Loaded...')   
    data_store = {}
    count=1
    TIC = tickerr
    for i in (timestamps_new):
        x = df.loc[i[0]:i[1]].iloc[1:]
        print(x)
        x=feats(x, TIC, i)
        data_store[i[1]] = x
        count+=1
    da = pd.DataFrame.from_dict(data_store, orient='index')
    print('aaaaaaaaaaaddddddddddddddddd')
    
    
    
    
    
    
    da.rename(columns={  da.columns[0]: 'lempel_ziv_Pt1Pct', 
                                   da.columns[1]: 'shanon_Pt1Pct',
                                   da.columns[2]: 'lempel_ziv_Pt5Pct',
                                   da.columns[3]: 'shanon_Pt5Pct',
                                 da.columns[4]: 'lempel_ziv_1Pct',
                                   da.columns[5]: 'shanon_1Pct',
                                 
                                 da.columns[6]: 'tb_kyle_coef', 
                                   da.columns[7]: 'tb_kyle_t',
                                   da.columns[8]: 'tb_amihud_coef',
                                   da.columns[9]: 'tb_amihud_t',
                                 da.columns[10]: 'tb_hasbrouk_coef',
                                   da.columns[11]: 'tb_hasbrouk_t',
                                 
                                  da.columns[12]: 'vwap',
                                   da.columns[13]: 'avg_tick_size',

                                   }, inplace=True)
    da.to_csv(f"./docker_storage/features_after/{tickerr}.csv")
    print('done')
    
#     return da

