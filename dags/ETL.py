import sys
sys.path.append('./dags/mlfinlab/')
import os
import pandas as pd
import sklearn
import mlfinlab
import json
import warnings
import datetime
import aiohttp
import pytz
import asyncio
import json
import pandas as pd
import numpy as np
import requests
import os
import glob
import nest_asyncio
import sys
import pickle
import glob
import os.path
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from mlfinlab.data_structures import standard_data_structures
from datetime import date as dt
from datetime import time as tm
from datetime import datetime, date, time, timedelta
warnings.simplefilter(action='ignore', category=FutureWarning)
nest_asyncio.apply()
# from Extract2 import get_data
from Extract_Update import update_all
from Transform import step_one
from Transform_two import transform_stage_two, update_transform_stage_two
#####################3
# from Tranform_three import make_all_features, update_transform_stage_three
from Transform_update import update_step_one
from airflow.utils.dates import days_ago
import exchange_calendars as ec
import talib
# from Extract2 import get_data
from New_extract import get_data
from New_extract2 import get_data2
from split import split
from split_adg import splitSS
from Timetick import get_data1
from feature import wi
from Tranform_six1 import make_all_features, update_transform_stage_three
from Iron import splite, splite_after
from dollar_extract import get_dollar_data
from dollar_extract_after import get_dollar_data_after

from dollar_time_extract import kit
from dollar_time_extract_update import kit_update
from Dollar_Adjust import dollar_adjusting
# from Dollar_Adjust_Update import dollar_adjusting_update
from dollar_extract_update import get_dollar_data_updated
from dollar_extract_update_after import get_dollar_data_updated_after
from dollar_time_extract_after import kit_after
from dollar_time_extract_after_testing import kit_aftert
from dollar_time_extract_after_testing_update import kit_aftert_update
# from new2 import dollar_adjusting_update
from Creatingfile import creating_files,creating_files_after, creating_files_split,creating_files2,creating_files_after2,creating_files_after3
from final_feature import wit
from final_feature_exe import wit_exe
from final_feature_after import wit_after

from final_feature_after_testing import wit_after_testing


DEFAULT_ARGS = {
    'owner' : 'faizan',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'depends_on_past': False,
}



@dag(
    dag_id='New_Data_And_Complete_Pipeline',
    default_args=DEFAULT_ARGS,
    start_date=datetime.now(),
    schedule_interval=None
    )

def test_etl():

    @task
    def extract_tick_data():
        ticker_info = get_data()
        return ticker_info

    @task
    def transform_and_save(ticker_info):
        ticker_info_updated = step_one(ticker_info)
        return ticker_info_updated

    @task
    def transform_stage_two_and_save(ticker_info_updated):
        tic_name = transform_stage_two(ticker_info_updated)
        return tic_name

    @task
    def make_features(tic_name):
        make_all_features(tic_name)



    ticker_info = extract_tick_data()
    ticker_info_updated = transform_and_save(ticker_info)
    tic_name = transform_stage_two_and_save(ticker_info_updated)
    make_features(tic_name)


Dag_main = test_etl()

##########################################################################################################################################################


@dag(
    dag_id='Daily_Data_Update',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval='30 12 * * 1-5', # runs on weekdays (Monday-Friday) at 6pm local
    catchup=False
    )

def update_etl():

    @task
    def is_nasdaq_market_open():
        nasdaq = ec.get_calendar('NASDAQ')
        now = datetime.now(tz=nasdaq.tz).date()
        if nasdaq.is_session(now):
            return True
        else:
            return False
    
    @task
    def update_tick_data(market_open):
        if market_open:
            complete_ticker_info = update_all()
            return complete_ticker_info
        else:
            return None



    market_bool = is_nasdaq_market_open()
    ticker_info = update_tick_data(market_bool)
     

Dag_update = update_etl()

@dag(
    dag_id='test_extract_transform_etl1',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )

def test_extract_transform_etl1():

    @task
    def extract_tick_data():
        ticker_info = get_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    @task
    def transform_and_save(ticker_info):
        ticker_info_updated = step_one(ticker_info)
        return ticker_info_updated
    
    
    @task
    def transform_stage_two_and_save(ticker_info_updated):
        tic_name = transform_stage_two(ticker_info_updated)
        return tic_name

    @task
    def make_features(tic_name):
        make_all_features(tic_name)


    ticker_info = extract_tick_data()
    ticker_info_updated = transform_and_save(ticker_info)
    tic_name = transform_stage_two_and_save(ticker_info_updated)
    make_features(tic_name)


Dag_test_extract_transform = test_extract_transform_etl1()



######################################################################################


@dag(
    dag_id='working1',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )



def working1():

    @task
    def extract_tick_data():
        ticker_info = get_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    @task
    def transform_and_save(ticker_info):
        print('aaaaaaaaaa')
        ticker_info_updated = step_one(ticker_info)
        return ticker_info_updated
    
    
    @task
    def transform_stage_two_and_save(ticker_info_updated):
        print('kkkkkkkkkkkkkkkkk')
        tic_name = transform_stage_two(ticker_info_updated)
        return tic_name

    @task
    def make_features(tic_name):
        print('nnnnnnnnnnnnnnnn')
        make_all_features(tic_name)


    ticker_info = extract_tick_data()
    ticker_info_updated = transform_and_save(ticker_info)
    tic_name = transform_stage_two_and_save(ticker_info_updated)
    make_features(tic_name)


Dag_test_extract_transform1 = working1()



@dag(
    dag_id='new_pipeline',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def new_pipeline():

    @task
    def extract_tick_data():
        ticker_info = get_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def adjusting(ticker_info):
        
        print('sssssaaaaaaaaaa')
        ticker_splitted = split(ticker_info)
        return ticker_splitted
    
    @task
    def time_tick(ticker_info):
        print('aaaaaaaaaa')
        ticker_info_updated = get_data1(ticker_info)
        return ticker_info_updated
    
    @task
    def feature(ticker_info):
        print('tttttttttttt')
        ticker_feature = wi(ticker_info)
        return ticker_feature
    
    
    ticker_info = extract_tick_data()
    datasplit = adjusting(ticker_info)
    timetick_info = time_tick(datasplit)
    a = feature(timetick_info)
    

Dag_test_extract_transform1 = new_pipeline()



@dag(
    dag_id='etll',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def etll():

    @task
    def ticc():
        print('tttttttttttt')
        ticker_feature = wi()
        return ticker_feature

    ac = ticc()
ETL_JUST = etll()




@dag(
    dag_id='new_pipeline_update',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def new_pipeline_update():

    @task
    def extract_tick_data():
        ticker_info = get_data2()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    @task
    def time_tick(ticker_info):
        print('aaaaaaaaaa')
        ticker_info_updated = get_data1(ticker_info)
        return ticker_info_updated
    
    @task
    def feature(ticker_info):
        print('tttttttttttt')
        ticker_feature = wi(ticker_info)
        return ticker_feature
    
    
    ticker_info = extract_tick_data()
#     datasplit = adjusting(ticker_info)
    timetick_info = time_tick(ticker_info)
    a = feature(timetick_info)
    

Dag_test_extract_transform2 = new_pipeline_update()





@dag(
    dag_id='splitss',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def new_splitss():

    @task
    def get_splitss():
        ticker_info = splitSS()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    ticker_info = get_splitss()
    
Dag_test_extract_splitss = new_splitss()



@dag(
    dag_id='dollars',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline():

    @task
    def get_dollar():
        ticker_info = get_dollar_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    @task
    def get_dollar_adjust(ticker_info):
        ticker_info = dollar_adjusting(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    @task
    def get_dollar_file(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_file_split(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    ticker_info = get_dollar()
    dol_adj = get_dollar_adjust(ticker_info)
    dol_adj_file = get_dollar_file(ticker_info)
    dol_adj_file_split = get_dollar_file_split(ticker_info)
Dag_dollar = dollar_pipeline()

# @dag(
#     dag_id='dollars_update',
#     default_args=DEFAULT_ARGS,
#     start_date=days_ago(2),
#     schedule_interval=None,
#     catchup=False
#     )


# def dollar_pipeline_update():

#     @task
#     def get_dollar_update():
#         ticker_info = get_dollar_data_updated()
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
#         return ticker_info

#     @task
#     def get_dollar_adjust(ticker_info):
#         ticker_info = dollar_adjusting(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
#         return ticker_info
#     @task
#     def get_dollar_file(ticker_info):
#         ticker = creating_files(ticker_info)
# #         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
#         return ticker_info
    
#     @task
#     def get_dollar_file_split(ticker_info):
# #         ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
#         return ticker_info

    
    
    
    
#     ticker_info = get_dollar_update()
#     dol_adj = dollar_adjusting_update(ticker_info)
#     dol_adj_file = get_dollar_file(ticker_info)
#     dol_adj_file_split = get_dollar_file_split(ticker_info)

# Dag_dollar_update = dollar_pipeline_update()

# UPDATEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE

@dag(
    dag_id='dollars_update2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_update2():

    @task
    def get_dollar_update2():
        ticker_info = get_dollar_data_updated()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_update(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files2(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
#     @task
#     def fin_feature(ticker_info):
#         ticker = wit(ticker_info)
# #         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
#         return ticker_info
    @task
    def fin_feature(ticker_info):
        ticker = wit_exe(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    



    ticker_info = get_dollar_update2()
    dol_adj_file = get_dollar_file_update2(ticker_info)
    dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
    ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
    ticker_info3=get_dollar_time_file2(ticker_info2)
    ticker_info4 = fin_feature(ticker_info3)

    
Dag_dollar = dollar_pipeline_update2()


@dag(
    dag_id='dollars2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline2():

    @task
    def get_dollar2():
        ticker_info = get_dollar_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    @task
    def get_dollar_file2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_file_split2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_timee2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files2(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def fin_feature(ticker_info):
        ticker = wit(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    

    ticker_info = get_dollar2()
    dol_adj_file = get_dollar_file2(ticker_info)
    dol_adj_file_split = get_dollar_file_split2(dol_adj_file)
    ticker_info2 = get_dollar_timee2(dol_adj_file_split)
    ticker_info3=get_dollar_time_file2(ticker_info2)
    ticker_info4 = fin_feature(ticker_info3)
    
Dag_dollar = dollar_pipeline2()




@dag(
    dag_id='dollars_after2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after2():

    @task
    def get_dollar_after2():
        ticker_info = get_dollar_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    @task
    def get_dollar_file_after2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_file_split_after2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_after2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_after(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files_after2(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def fin_feature_after(ticker_info):
        ticker = wit_after(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    



    ticker_info = get_dollar_after2()
    dol_adj_file = get_dollar_file_after2(ticker_info)
    dol_adj_file_split = get_dollar_file_split_after2(ticker_info)
    ticker_info2 = get_dollar_timee_after2(dol_adj_file_split)
    ticker_info3=get_dollar_time_file2(ticker_info2)
    ticker_info4 = fin_feature_after(ticker_info3)

    
Dag_dollar = dollar_pipeline_after2()


@dag(
    dag_id='dollars_after_testing2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after_testing2():

    
    @task
    def get_dollar():
        ticker_info = get_dollar_data()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    

    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_aftert(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files_after3(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    
    
    @task
    def feature_inprogress(ticker_info):
        ticker_info = wit_after_testing(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    ticker_info = get_dollar()
    dol_adj_file = get_dollar_file_update2(ticker_info)
    dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
    ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
    ticker_info3=get_dollar_time_file2(ticker_info2)
    feature = feature_inprogress(ticker_info3)
    
Dag_dollar = dollar_pipeline_after_testing2()



@dag(
    dag_id='dollars_after_testing_new2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after_testing_new2():

    
    @task
    def get_dollar():
        ticker_info = get_dollar_data_after()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    

    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files_after(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite_after(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_aftert(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files_after3(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    
    
    @task
    def feature_inprogress(ticker_info):
        ticker_info = wit_after_testing(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    ticker_info = get_dollar()
    dol_adj_file = get_dollar_file_update2(ticker_info)
    dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
    ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
    ticker_info3=get_dollar_time_file2(ticker_info2)
    feature = feature_inprogress(ticker_info3)
    
Dag_dollar = dollar_pipeline_after_testing_new2()



@dag(
    dag_id='dollars_after_testing_update2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval='56 6 * * *',
#     schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after_testing_update2():

    
    @task
    def get_dollar_update():
        ticker_info = get_dollar_data_updated()
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info

    

    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_aftert_update(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files_after3(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    
    
    @task
    def feature_inprogress(ticker_info):
        ticker_info = wit_after_testing(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    ticker_info = get_dollar_update()
    dol_adj_file = get_dollar_file_update2(ticker_info)
    dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
    ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
    ticker_info3=get_dollar_time_file2(ticker_info2)
    feature = feature_inprogress(ticker_info3)
    
Dag_dollar = dollar_pipeline_after_testing_update2()

# stocks = ['NVDA','TSLA','AMD','AAPL','ASML','ENPH','INTC','TSM','NFLX','MRVL','AMZN','MSFT']
# stocks = ['ACGL','AGL','AN','BAP','CHNG','FTSL','GILD','HLT','JDST','KS','MIC','NLOK','OSH','RF','SCG','SEP','TCO']
# stocks = ['NVDA','TSLA','AMD','AAPL','ASML','ENPH']
# stocks = ['INTC','TSM','NFLX','MRVL','AMZN','MSFT']
# stocks=['NVDA']
# stocks=['AAPL']
stocks=['INTC']
# stocks=['AMD','ASML']
# stocks = ['ENPH','INTC','TSM','NFLX']
# stocks=['MRVL','AMZN','MSFT']
# stocks=['TSLA']


@dag(
    dag_id='dollars_update_time2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_update_time2():

    @task
    def get_dollar_update2(tickers):
        ticker_info = get_dollar_data_updated(tickers)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_update(ticker_info)
        print(ticker1)
        print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files2(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
#     @task
#     def fin_feature(ticker_info):
#         ticker = wit(ticker_info)
# #         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
#         return ticker_info
    @task
    def fin_feature(ticker_info):
        ticker = wit_exe(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info
    


    for i in stocks:
        try:
            print(i)
            ticker_info = get_dollar_update2(i)
            dol_adj_file = get_dollar_file_update2(ticker_info)
            dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
            ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
            ticker_info3=get_dollar_time_file2(ticker_info2)
            feature = fin_feature(ticker_info3)
        except Exception as e:
        # Handle the exception
            print(f"Error occurred while processing {tickers}: {e}")
    
Dag_dollar = dollar_pipeline_update_time2()









@dag(
    dag_id='dollars_after_testing_update_time2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval='31 13 * * *',
#     schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after_testing_update_time2():
    
        
    @task
    def get_dollar_update(tickers):
        ticker_info = get_dollar_data_updated(tickers)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info

    

    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_aftert_update(ticker_info)
#         print(ticker1)
#         print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files_after3(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info


    
    
    @task
    def feature_inprogress(ticker_info):
        ticker_info = wit_after_testing(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info


    for i in stocks:
#         print(i)
        ticker_info = get_dollar_update(i)
        dol_adj_file = get_dollar_file_update2(ticker_info)
        dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
        ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
        ticker_info3=get_dollar_time_file2(ticker_info2)
        feature = feature_inprogress(ticker_info3)
    
Dag_dollar = dollar_pipeline_after_testing_update_time2()


@dag(
    dag_id='dollars_after_testing_update_time_new2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval='31 13 * * *',
#     schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after_testing_update_time_new2():
    
        
    @task
    def get_dollar_update(tickers):
        ticker_info = get_dollar_data_updated_after(tickers)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info

    

    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files_after(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info
    @task
    def get_dollar_file_split_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = splite_after(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info
    
    @task
    def get_dollar_timee_update2(ticker_info):
#         ticker = creating_files(ticker_info)
        ticker1 = kit_aftert_update(ticker_info)
#         print(ticker1)
#         print('jjjjjjjjjjjjjjjj')
        return ticker1
    @task
    def get_dollar_time_file2(ticker_info):
        ticker = creating_files_after3(ticker_info)
#         ticker1 = creating_files_split(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info


    
    
    @task
    def feature_inprogress(ticker_info):
        ticker_info = wit_after_testing(ticker_info)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info


    for i in stocks:
#         print(i)
        ticker_info = get_dollar_update(i)
        dol_adj_file = get_dollar_file_update2(ticker_info)
        dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
        ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
        ticker_info3=get_dollar_time_file2(ticker_info2)
        feature = feature_inprogress(ticker_info3)
    
Dag_dollar = dollar_pipeline_after_testing_update_time_new2()

##################################################################################################################


# @dag(
#     dag_id='New_Data_And_Complete_Pipeline',
#     default_args=DEFAULT_ARGS,
#     start_date=datetime.now(),
#     schedule_interval=None
#     )

# def test_etl():

#     @task
#     def extract_tick_data():
#         ticker_info = get_data()
#         return ticker_info

#     @task
#     def transform_and_save(ticker_info):
#         ticker_info_updated = step_one(ticker_info)
#         return ticker_info_updated

#     @task
#     def transform_stage_two_and_save(ticker_info_updated):
#         tic_name = transform_stage_two(ticker_info_updated)
#         return tic_name

#     @task
#     def make_features(tic_name):
#         make_all_features(tic_name)



#     ticker_info = extract_tick_data()
#     ticker_info_updated = transform_and_save(ticker_info)
#     tic_name = transform_stage_two_and_save(ticker_info_updated)
#     make_features(tic_name)


# Dag_main = test_etl()



# @dag(
#     dag_id='Testing',
#     default_args=DEFAULT_ARGS,
#     start_date=datetime.now(),
#     schedule_interval=None, # runs on weekdays (Monday-Friday) at 6pm local
#     catchup=False
#     )

# def Testing():

#     @task
#     def Testing1():
#         print("hello world")
#         return "hello world"
    
#     ticker_info = Testing1()
    
# Dag_main1 = Testing()




# def first_func():
#     print("hello world")
#     return "hello world"




















