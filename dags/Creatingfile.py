import pandas as pd
import numpy as np
import os
def creating_files(ticker_info):
    listt=[]
    pathh = ticker_info['path']
    ticker = ticker_info['name']
    print(pathh)
    os.listdir(pathh)
    ab = os.listdir(pathh)
    rawp = './docker_storage/Raw_Data_New/Raw_Data_Splitted/'
    fullf_path = f'./docker_storage/Raw_Data_New/full_file/{ticker}_full.ftr'
#     print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
#         print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt.append(ji)
    #     print(listt)
    if listt is not None:
        df_combined = pd.concat([pd.read_feather(f) for f in listt])
#         print(df_combined)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
#         print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))    
    df_combined.to_feather(f'./docker_storage/Raw_Data_New/full_file/{ticker}_full.ftr')

    
def creating_files_after(ticker_info):
    listt=[]
    pathh = ticker_info['path']
    ticker = ticker_info['name']
    print(pathh)
    os.listdir(pathh)
    ab = os.listdir(pathh)
    rawp = './docker_storage/after_raw_data/Raw_Data_Splitted/'
    fullf_path = f'./docker_storage/after_raw_data/full_file/{ticker}_full.ftr'
#     print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
#         print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt.append(ji)
    #     print(listt)
    if listt is not None:
        df_combined = pd.concat([pd.read_feather(f) for f in listt])
#         print(df_combined)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
#         print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))    
    df_combined.to_feather(f'./docker_storage/after_raw_data/full_file/{ticker}_full.ftr')
    
    
    
    
def creating_files_split(ticker_info):
#     print('[[[[[[[[[')
    print(ticker_info)
    listt1=[]
#     pathh = f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{ticker}_2021_split/'
    ticker = ticker_info['name']
    pathh = f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/{ticker}_2023_split/'
    print(pathh)
    os.listdir(pathh)
    ab = os.listdir(pathh)
#     print(ab)
    fullf_path = f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/full_file/{ticker}_full_split.ftr'
    print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
#         print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt1.append(ji)
#         print(listt1)
    if listt1 is not None:
        df_combined = pd.concat([pd.read_feather(f) for f in listt1])
        print(df_combined)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
#     print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))    
    df_combined.to_feather(f'./docker_storage/Raw_Data_New/Raw_Data_Splitted/full_file/{ticker}_full_split.ftr')
    
def creating_files2(ticker_info):
        
#     ticker_info = {
# 	"ticker": "NFLX",
#     "start-date": "2023-01-01",
#     "end-date": "2023-02-12"
#     }

    
    print(ticker_info)
    listt=[]
#     pathh = ticker_info['path']
    tickerr = ticker_info['name']
    year = '2023'
    pathh = f'./docker_storage/Time_Data_New/{tickerr}_{year}/'
#     print(pathh)
    if not os.path.exists(pathh):
        os.makedirs(pathh)
#         print('created')
    else:
        print('exist')
    os.listdir(pathh)
    ab = os.listdir(pathh)
    rawp = './docker_storage/Raw_Data_New/Raw_Data_Splitted/'
    fullf_path = f'./docker_storage/Raw_Data_New/{tickerr}_{year}/full_file/{tickerr}_full.ftr'
#     print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
#         print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt.append(ji)
#     print(len(listt))
    #     print(listt)
    if listt is not None:
        
        df_combined = pd.concat([pd.read_feather(f) for f in listt])
#         print(df_combined)
#         df_combined.drop('level_0', axis=1, inplace=True)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
#         print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))
    
    df_combined.to_feather(f'./docker_storage/Time_Data_New/full_file/{tickerr}_Time_Tick-Data.ftr')
    ticker_info['resample-path'] = f'./docker_storage/Time_Data_New/full_file/{tickerr}_Time_Tick-Data.ftr'
    return ticker_info
    
    
def creating_files_after2(ticker_info):
        
#     ticker_info = {
# 	"ticker": "NFLX",
#     "start-date": "2023-01-01",
#     "end-date": "2023-02-12"
#     }

    
    print(ticker_info)
    listt=[]
#     pathh = ticker_info['path']
    tickerr = ticker_info['name']
    year = '2023'
    pathh = f'./docker_storage/Time_after/{tickerr}_{year}/'
#     print(pathh)
    if not os.path.exists(pathh):
        os.makedirs(pathh)
#         print('created')
    else:
        print('exist')
    os.listdir(pathh)
    ab = os.listdir(pathh)
    rawp = './docker_storage/Raw_Data_New/Raw_Data_Splitted/'
    fullf_path = f'./docker_storage/Raw_Data_New/{tickerr}_{year}/full_file/{tickerr}_full.ftr'
#     print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
#         print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt.append(ji)
#     print(len(listt))
    #     print(listt)
    if listt is not None:
        
        df_combined = pd.concat([pd.read_feather(f) for f in listt])
#         print(df_combined)
#         df_combined.drop('level_0', axis=1, inplace=True)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
#         print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))
    
    df_combined.to_feather(f'./docker_storage/Time_after/full_file_after/{tickerr}_Time_Tick-Data.ftr')
    ticker_info['resample-path'] = f'./docker_storage/Time_after/full_file_after/{tickerr}_Time_Tick-Data.ftr'
    return ticker_info
    


    
def creating_files_after3(ticker_info):
        
#     ticker_info = {
# 	"ticker": "NFLX",
#     "start-date": "2023-01-01",
#     "end-date": "2023-02-12"
#     }

    
    print(ticker_info)
    listt=[]
#     pathh = ticker_info['path']
    tickerr = ticker_info['name']
    year = '2023'
    pathh = f'./docker_storage/Time_after/{tickerr}_{year}_market/'
#     print(pathh)
    if not os.path.exists(pathh):
        os.makedirs(pathh)
#         print('created')
    else:
        print('exist')
    os.listdir(pathh)
    ab = os.listdir(pathh)
    fullf_path = f'./docker_storage/Raw_Data_New/{tickerr}_{year}_market/full_file/{tickerr}_full.ftr'
#     print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
#         print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt.append(ji)
#     print(len(listt))
    #     print(listt)
    if listt is not None:
        
        df_combined = pd.concat([pd.read_feather(f) for f in listt])
#         print(df_combined)
#         df_combined.drop('level_0', axis=1, inplace=True)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
#         print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))
    
    df_combined.to_feather(f'./docker_storage/Time_after/full_file_after/{tickerr}_Time_Tick-Data1.ftr')
#     print('done')
    ticker_info['resample-path'] = f'./docker_storage/Time_after/full_file_after/{tickerr}_Time_Tick-Data1.ftr'
    return ticker_info
    


