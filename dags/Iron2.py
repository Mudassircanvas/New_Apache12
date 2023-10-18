def spite2(ticker_info):
        
#     ticker_info = {
# 	"ticker": "NFLX",
#     "start-date": "2023-01-01",
#     "end-date": "2023-02-12"
#     }

    
    
    listt=[]
#     pathh = ticker_info['path']
    tickerr = ticker_info['name']
    year = '2023'
    pathh = f'./docker_storage/Time_Data_New/{tickerr}_{year}/'
    print(pathh)
    os.listdir(pathh)
    ab = os.listdir(pathh)
    rawp = './docker_storage/Raw_Data_New/Raw_Data_Splitted/'
    fullf_path = f'./docker_storage/Raw_Data_New/full_file/{tickerr}_full.ftr'
    print(fullf_path)
    file_list_sorted = sorted(ab, key=lambda x: x.split('_')[1])
    for i in range(len(file_list_sorted)):
        print(pathh+file_list_sorted[i])
        ji = pathh+file_list_sorted[i]
        listt.append(ji)
    #     print(listt)
    if listt is not None:
        df_combined = pd.concat([pd.read_feather(f) for f in listt])
        print(df_combined)
#         df_combined.set_index(df_combined['participant_timestamp'],inplace=True)
        df_combined = df_combined.reset_index()
        print(df_combined)
#     print(os.listdir('./docker_storage/Raw_Data_New/full_file'))
    
    df_combined.to_feather(f'./docker_storage/Time_Data_New/{tickerr}_{year}/{tickerr}_Time_Tick-Data.ftr')
    ticker_info['resample-path'] = f'./docker_storage/Time_Data_New/{tickerr}_{year}/{tickerr}_Time_Tick-Data.ftr'
    return ticker_info
    
