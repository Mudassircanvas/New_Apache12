import pandas as pd

def makingohc():
    # stocks=['NVDA']
    # stocks = ['INTC','TSM','NFLX','MRVL','AMZN','MSFT','ASML']
    # stocks = ['TSM','MSFT','NFLX']
    # stocks = ['INTC','TSM','NFLX','MRVL','AMZN','MSFT']
    # stocks = ['NVDA','AMD','AAPL','ASML','ENPH']
    stocks = ['NVDA','AAPL','AMD','ASML','ENPH','INTC','TSM','MRVL','MSFT','AMZN','TSLA','NFLX']
    # stocks=['NFLX']
    for ticker in stocks:
        # # This is for TSLA CODING
        # df1 = pd.read_feather('./docker_storage/a/TSLA_Time_Tick-Data.ftr')
        # df2 =  pd.read_feather('./docker_storage/Time_Data_New/full_file/TSLA_Time_Tick-Data.ftr')
        # df1['t'] = pd.to_datetime(df1['t'])
        # df2['t'] = pd.to_datetime(df2['t'])

        # frames = [df1, df2]
        # combined_df = pd.concat(frames, ignore_index=True)
        # combined_df = combined_df.drop_duplicates(subset='t', keep='first')


        # print(combined_df)
        # combined_df.to_feather('./docker_storage/Time_Data_New/full_file/TSLA_Time_Tick-Data.ftr')
        # combined_df = pd.read_feather('./docker_storage/Time_Data_New/full_file/TSLA_Time_Tick-Data.ftr')
        # df2 = combined_df.reset_index()

        # THIS IS ACTUAL CODING
        df2 = pd.read_feather(f'./docker_storage/Time_Data_New/full_file/{ticker}_Time_Tick-Data.ftr')
        df1 = pd.read_csv(f'./docker_storage/features/{ticker}_execution.csv') # Real one 4 jan
        # df1 = pd.read_csv(f'./docker_storage/features/{ticker}.csv')
        # # df3 = pd.read_csv(f'./docker_storage/{ticker}.csv')
        print(df2.columns)
        print('-'*12)
        print(df2)
        print(df1.columns)
        print('-'*12)
        print(df1)
        print(len(df1))
        # # print(df2.columns)
        # # print('-'*12)
        # # print(df2)

        df1['Unnamed: 0.1'] = pd.to_datetime(df1['Unnamed: 0.1']) # Acutal 4 jan
        # df1['Unnamed: 0'] = pd.to_datetime(df1['Unnamed: 0'])
        df2['t'] = pd.to_datetime(df2['t'])
        combined_df = df2.merge(df1, left_on='t', right_on='Unnamed: 0.1', how='outer')
        combined_df = combined_df[['t','o','c','h','l','v','lempel_ziv_1Pct','tb_kyle_coef','tb_kyle_t','shanon_1Pct','shanon_Pt5Pct','shanon_Pt1Pct']]
        print(combined_df)
        combined_df.to_csv(f'./docker_storage/features/{ticker}_execution11.csv')
        DF = pd.read_csv(f'./docker_storage/features/{ticker}_execution11.csv')
        print(DF)
        # merged_df = df1.merge(df2, left_on='Unnamed: 0.1', right_on='t', how='inner')
        # merged_df = merged_df[['t','o','c','h','l','v','lempel_ziv_1Pct','tb_kyle_coef','tb_kyle_t','shanon_1Pct','shanon_Pt5Pct','shanon_Pt1Pct']]
        # merged_df.to_csv(f'./docker_storage/features/{ticker}_execution111.csv')
        # print('done')
        # print(merged_df)
        print('8888888888888888')
        # print(combined_df)
        # print('--------------------------')
        # print(combined_df.head(1150).tail(900))






# # #TSLA CODE

import pandas as pd

def makingohc1():
    stocks=['TSLA']
    # stocks = ['NVDA','TSLA','AMD','ASML','ENPH','INTC','TSM','NFLX','MRVL','MSFT']
    for ticker in stocks:
        # # This is for TSLA CODING
        df1 = pd.read_feather('./docker_storage/a/TSLA_Time_Tick-Data.ftr')
        df2 =  pd.read_feather('./docker_storage/Time_Data_New/full_file/TSLA_Time_Tick-Data.ftr')
        df1['t'] = pd.to_datetime(df1['t'])
        df2['t'] = pd.to_datetime(df2['t'])

        frames = [df1, df2]
        combined_df = pd.concat(frames, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset='t', keep='first')


        print(combined_df)
        # combined_df.to_feather('./docker_storage/Time_Data_New/full_file/TSLA_Time_Tick-Data.ftr')
        # combined_df = pd.read_feather('./docker_storage/Time_Data_New/full_file/TSLA_Time_Tick-Data.ftr')
        df2 = combined_df.reset_index()

        # THIS IS ACTUAL CODING
        # df2 = pd.read_feather(f'./docker_storage/Time_Data_New/full_file/{ticker}_Time_Tick-Data.ftr')
        df1 = pd.read_csv(f'./docker_storage/features/{ticker}_execution.csv')
        # # df3 = pd.read_csv(f'./docker_storage/{ticker}.csv')
        print(df2.columns)
        print('-'*12)
        print(df2)
        print(df1.columns)
        print('-'*12)
        print(df1)
        print(len(df1))
        # # print(df2.columns)
        # # print('-'*12)
        # # print(df2)

        df1['Unnamed: 0.1'] = pd.to_datetime(df1['Unnamed: 0.1'])
        df2['t'] = pd.to_datetime(df2['t'])
        # combined_df = df2.merge(df1, left_on='t', right_on='Unnamed: 0.1', how='outer')
        # combined_df = combined_df[['t','o','c','h','l','v','lempel_ziv_1Pct','tb_kyle_coef','tb_kyle_t','shanon_1Pct','shanon_Pt5Pct','shanon_Pt1Pct']]
        # combined_df.to_csv(f'./docker_storage/features/{ticker}_execution11.csv')
        merged_df = df1.merge(df2, left_on='Unnamed: 0.1', right_on='t', how='inner')
        merged_df = merged_df[['t','o','c','h','l','v','lempel_ziv_1Pct','tb_kyle_coef','tb_kyle_t','shanon_1Pct','shanon_Pt5Pct','shanon_Pt1Pct']]
        merged_df.to_csv(f'./docker_storage/features/{ticker}_execution11.csv')
        print('done')
        print(merged_df)
        # print('8888888888888888')
        # # print(combined_df)
        # # print('--------------------------')
        # # print(combined_df.head(1150).tail(900))