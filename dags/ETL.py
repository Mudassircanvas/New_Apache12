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
import pytz

from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from mlfinlab.data_structures import standard_data_structures
from datetime import date as dt
from datetime import time as tm
from datetime import datetime, date, time, timedelta
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
from Creatingfile import creating_files,creating_files_after, creating_files_split,creating_files2,creating_files_after2,creating_files_after3,creating_files_after_gpt,creating_files_after_gpt3,creating_files_historic,creating_files_historic_time
from final_feature import wit
from final_feature_exe import wit_exe
from final_feature_after import wit_after

from final_feature_after_testing import wit_after_testing
from makingoh import makingohc,makingohc1



from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain

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
        # ticker = creating_files(ticker_info) # this one ws actuall 4 jan 2024
        ticker = creating_files_historic(ticker_info)
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
        # ticker = creating_files2(ticker_info) # actual
        ticker = creating_files_historic_time(ticker_info)
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

stocks = ['NVDA','TSLA','AMD','INTC','TSM','NFLX','MRVL','AMZN','MSFT','ASML','ENPH','AAPL']
# stocks = ['ENPH','INTC','TSM','NFLX','MRVL','AMZN','MSFT']
# stocks = ['ACGL','AGL','AN','BAP','CHNG','FTSL','GILD','HLT','JDST','KS','MIC','NLOK','OSH','RF','SCG','SEP','TCO']
# stocks = ['AMD','AAPL','ENPH']
# stocks = ['INTC','TSM','NFLX','MRVL','AMZN','MSFT','ASML']
# stocks=['NFLX']
# stocks=['ENPH']
# stocks=['NVDA','AMD','ENPH']
# stocks=['ENPH']
# stocks = ['ENPH','INTC','TSM','NFLX']
# stocks=['TSM','NFLX','MSFT']
# stocks=['INTC']


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
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    last_task = start
    for i in stocks:
        try:
            print(i)
            ticker_info = get_dollar_update2(i)
            dol_adj_file = get_dollar_file_update2(ticker_info)
            dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
            ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
            ticker_info3=get_dollar_time_file2(ticker_info2)
            feature = fin_feature(ticker_info3)

            # chain(start, ticker_info, dol_adj_file, dol_adj_file_split, ticker_info2, ticker_info3, feature, end)


            last_task >> ticker_info
            ticker_info >> dol_adj_file
            dol_adj_file >> dol_adj_file_split
            dol_adj_file_split >> ticker_info2
            ticker_info2 >> ticker_info3
            ticker_info3 >> feature

            # Update the last_task to be the final task for this stock
            last_task = feature

        except Exception as e:
        # Handle the exception
            print(f"Error occurred while processing {tickers}: {e}")
    last_task >> end
    
Dag_dollar = dollar_pipeline_update_time2()









@dag(
    dag_id='dollars_after_testing_update_time2',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval='31 09 * * *',
    # schedule_interval=None,
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
    # schedule_interval='35 14 * * *',
    schedule_interval=None,
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






@dag(
    dag_id='makingoh',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def making():

    @task
    def making1():
        ticker_info = makingohc()
        print()
        print('jjjjjjjjjjjjjjjj')
       
    # @task
    # def making2():
    #     ticker_info = makingohc1()
    #     print()
    #     print('jjjjjjjjjjjjjjjj')

    ticker_info = making1()
    # ticker_info = making2()


Dag_dollar = making()

#############################################################

def nyse_open_in_utc():
    ny_tz = pytz.timezone('America/New_York')
    nyse_open_et = datetime.now(ny_tz).replace(hour=9, minute=31, second=0, microsecond=0)

    # Convert NYSE opening time to UTC
    nyse_open_utc = nyse_open_et.astimezone(pytz.utc)
    return nyse_open_utc.strftime('%M %H * * *')

##################################################################
@dag(
    dag_id='dollars_after_testing_update_time_new3',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=nyse_open_in_utc(),
#     schedule_interval=None,
    catchup=False
    )


def dollar_pipeline_after_testing_update_time_new3():
    
        
    @task
    def get_dollar_update(tickers):
        ticker_info = get_dollar_data_updated_after(tickers)
#         print(ticker_info)
#         print('jjjjjjjjjjjjjjjj')
        return ticker_info

    

    @task
    def get_dollar_file_update2(ticker_info):
        ticker = creating_files_after(ticker_info)
        print(ticker_info)
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
        print(ticker_info)
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
    
Dag_dollar = dollar_pipeline_after_testing_update_time_new3()


###########################################################################################


def nyse_open_8_30_pm_in_utc():
    # Time zone for New York
    ny_tz = pytz.timezone('America/New_York')
    
    # NYSE opening time at 8:30 PM ET
    nyse_open_et = datetime.now(ny_tz).replace(hour=20, minute=39, second=0, microsecond=0)
    
    # Convert NYSE opening time to UTC
    nyse_open_utc = nyse_open_et.astimezone(pytz.utc)
    return nyse_open_utc.strftime('%M %H * * *')

# def nyse_close_plus_10_in_utc():
#     # Time zone for New York
#     ny_tz = pytz.timezone('America/New_York')
    
#     # NYSE closing time at 4:00 PM ET
#     nyse_close_et = datetime.now(ny_tz).replace(hour=16, minute=10, second=0, microsecond=0)
    
#     # Convert NYSE closing time + 10 minutes to UTC
#     nyse_close_utc = nyse_close_et.astimezone(pytz.utc)
#     return nyse_close_utc.strftime('%M %H * * *')


def nyse_close_plus_10_cron():
    # NYSE closes at 16:00 New York time
    ny_close_time = datetime.now(pytz.timezone('America/New_York')).replace(
        hour=16, minute=0, second=0, microsecond=0)

    # Add 10 minutes to the closing time
    ny_close_time_plus_10 = ny_close_time + timedelta(minutes=10)

    # Get the hour and minute in UTC
    ny_close_time_plus_10_utc = ny_close_time_plus_10.astimezone(pytz.utc)
    hour = ny_close_time_plus_10_utc.hour
    minute = ny_close_time_plus_10_utc.minute

    # Generate the cron expression for the schedule
    cron_schedule = f"{minute} {hour} * * *"
    return cron_schedule

@dag(
    dag_id='dollars_update_time3',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=nyse_close_plus_10_cron(),
    catchup=False
    )


def dollar_pipeline_update_time3():

    @task
    def get_dollar_update2(tickers):
        ticker_info = get_dollar_data_updated(tickers)
        print(ticker_info)
        print('jjjjjjjjjjjjjjjj')
        return ticker_info


    @task
    def get_dollar_file_update2(ticker_info):
        # ticker = creating_files(ticker_info) # actuall one 8 jan
        ticker1 = creating_files_historic(ticker_info)
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
        # ticker = creating_files2(ticker_info) # actuall one 8 jan
        ticker1 = creating_files_historic_time(ticker_info)
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
        
    @task
    def making1():
        ticker_inf = makingohc()
        print()
        print('jjjjjjjjjjjjjjjj')
       
    # @task
    # def making2():
    #     ticker_inf = makingohc1()
    #     print()
    #     print('jjjjjjjjjjjjjjjj')



    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    last_task = start
    for i in stocks:
        try:
            print(i)
            ticker_info = get_dollar_update2(i)
            dol_adj_file = get_dollar_file_update2(ticker_info)
            dol_adj_file_split = get_dollar_file_split_update2(dol_adj_file)
            ticker_info2 = get_dollar_timee_update2(dol_adj_file_split)
            ticker_info3=get_dollar_time_file2(ticker_info2)
            feature = fin_feature(ticker_info3)

            # chain(start, ticker_info, dol_adj_file, dol_adj_file_split, ticker_info2, ticker_info3, feature, end)


            last_task >> ticker_info
            ticker_info >> dol_adj_file
            dol_adj_file >> dol_adj_file_split
            dol_adj_file_split >> ticker_info2
            ticker_info2 >> ticker_info3
            ticker_info3 >> feature

            # Update the last_task to be the final task for this stock
            last_task = feature

        except Exception as e:
        # Handle the exception
            print(f"Error occurred while processing {tickers}: {e}")

    all_stocks_processed = DummyOperator(task_id='all_stocks_processed')
    last_task >> all_stocks_processed >> making1() >> end

    # last_task >> end
    
Dag_dollar = dollar_pipeline_update_time3()



@dag(
    dag_id='makingoh1',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
    )


def making_new():


    @task
    def making1():
        ticker_info = makingohc()
        print()
        print('jjjjjjjjjjjjjjjj')
       
    @task
    def making2():
        ticker_info = makingohc1()
        print()
        print('jjjjjjjjjjjjjjjj')

    making1() >> making2()

Dag_dollar = making_new()



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




















