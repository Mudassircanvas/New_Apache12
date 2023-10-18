import pandas as pd
import numpy as np
import talib
import pickle
from mlfinlab.data_structures import imbalance_data_structures, standard_data_structures
from pyarrow import feather
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from datetime import timedelta
# import exchange_calendars as xcals
# from statsmodels.tsa.stattools import adfuller
from scipy.stats import iqr
import warnings
import talib
from mlfinlab.features.fracdiff import frac_diff_ffd
from mlfinlab.microstructural_features import first_generation
from mlfinlab.filters import filters

def getDailyVol(close, span0=100):
    # daily vol reindexed to close
    df0 = close.index.searchsorted(close.index - pd.Timedelta(days=1))
    df0 = df0[df0 > 0]
    df0 = pd.Series(close.index[df0 - 1], index=close.index[close.shape[0] - df0.shape[0]:])
    try:
        df0 = close.loc[df0.index] / close.loc[df0.values].values - 1  # daily rets
    except Exception as e:
        print(f'error: {e}\nplease confirm no duplicate indices')
    df0 = df0.ewm(span=span0).std().rename('dailyVol')
    return df0


def co(curr, prev):
    if prev < 1 and curr > 1:
        return 1
    else:
        return 0


def TradeSim(row, count, slM, tpM):
    bp = row['C']
    dailyVol = row['feature_dailyVol']

    sl = -slM * dailyVol
    tp = tpM * dailyVol

    for i in np.arange(count):
        currLowPct = (row[f'low+{i+1}'] - bp) / bp
        currHighPct = (row[f'high+{i+1}'] - bp) / bp

        currBarTime = row[f'index+{i+1}']

        if currLowPct < sl:
            return [currLowPct, -1, i, currBarTime]
        elif currHighPct >= tp:
            return [currHighPct, 1, i, currBarTime]

    currClosePct = (row[f'close+{count}'] - bp) / bp
    return [currClosePct, 0, count, currBarTime]


def make_all_features(tic):
    secDict = dict()
    try:
        print('------------------------------------------------------------------')
        print(tic)
        df = pickle.load(open(f'docker_storage/Tick_Data/Const_Resampled_2017/dolDF_const_10_BarsPerDay_wFeats/{tic}_dolDF_const_10_BarsPerDay_wFeats.pkl', 'rb'))
        secDict[tic] = df
        print(len(df))
        print(secDict[tic].dropna(subset=['tradeableH', 'tradeableL']).isna().sum().sum())

        # Make Features

        # avg tick size adjusted w price
        secDict[tic]['feature_avg_tick_price*C'] = secDict[tic]['avg_tick_size'] * secDict[tic]['C']

        # ratios of ohlcv, wvap
        secDict[tic]['feature_O/C'] = secDict[tic]['O'] / secDict[tic]['C']
        secDict[tic]['feature_O/H'] = secDict[tic]['O'] / secDict[tic]['H']
        secDict[tic]['feature_O/L'] = secDict[tic]['O'] / secDict[tic]['L']
        secDict[tic]['feature_H/L'] = secDict[tic]['H'] / secDict[tic]['L']
        secDict[tic]['feature_wvap/C'] = secDict[tic]['vwap'] / secDict[tic]['C']

        # BBs
        ub, middleband, lb = talib.BBANDS(secDict[tic].C, timeperiod=21, nbdevup=2, nbdevdn=2, matype=0)
        secDict[tic]['feature_C/UB'] = secDict[tic]['C'] / ub
        secDict[tic]['feature_C/LB'] = secDict[tic]['C'] / lb
        secDict[tic]['feature_UB/LB'] = ub / lb

        # emas
        ema5 = talib.EMA(secDict[tic].C, 5)
        ema10 = talib.EMA(secDict[tic].C, 10)
        ema21 = talib.EMA(secDict[tic].C, 21)
        ema55 = talib.EMA(secDict[tic].C, 55)
        sma500 = talib.SMA(secDict[tic].C, 500)
        secDict[tic]['feature_ema10/ema21'] = ema10 / ema21
        secDict[tic]['feature_ema10/ema55'] = ema10 / ema55
        secDict[tic]['feature_ema21/ema55'] = ema21 / ema55
        secDict[tic]['feature_C/ema10'] = secDict[tic]['C'] / ema10
        secDict[tic]['feature_C/ema21'] = secDict[tic]['C'] / ema21
        secDict[tic]['feature_C/ema55'] = secDict[tic]['C'] / ema55

        # adx
        secDict[tic][f'feature_adx21'] = talib.ADX(secDict[tic].H, secDict[tic].L, secDict[tic].C, 21)
        secDict[tic][f'feature_adx63'] = talib.ADX(secDict[tic].H, secDict[tic].L, secDict[tic].C, 63)

        # mfi
        secDict[tic][f'feature_mfi14'] = talib.MFI(secDict[tic].H, secDict[tic].L, secDict[tic].C, secDict[tic].DV, 14)
        secDict[tic][f'feature_mfi63'] = talib.MFI(secDict[tic].H, secDict[tic].L, secDict[tic].C, secDict[tic].DV, 63)

        # macd
        macd, macdsignal, macdhist = talib.MACD(secDict[tic].C, fastperiod=12, slowperiod=26, signalperiod=9)
        secDict[tic][f'feature_macd'] = macd / sma500
        secDict[tic][f'feature_macdsignal'] = macdsignal / sma500
        secDict[tic][f'feature_macdhist'] = macdhist / sma500

        # rsi
        secDict[tic][f'feature_rsi14'] = talib.RSI(secDict[tic].C, 14)
        secDict[tic][f'feature_rsi63'] = talib.RSI(secDict[tic].C, 63)

        # stochrsi
        fastk, fastd = talib.STOCHRSI(secDict[tic].C, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
        secDict[tic][f'feature_fastk'] = fastk
        secDict[tic][f'feature_fastd'] = fastd

        # UO
        secDict[tic][f'feature_UO'] = talib.ULTOSC(secDict[tic].H, secDict[tic].L, secDict[tic].C, timeperiod1=7, timeperiod2=14, timeperiod3=28)

        # atr
        secDict[tic][f'feature_atr14'] = talib.ATR(secDict[tic].H, secDict[tic].L, secDict[tic].C, timeperiod=14) / sma500
        secDict[tic][f'feature_atr63'] = talib.ATR(secDict[tic].H, secDict[tic].L, secDict[tic].C, timeperiod=63) / sma500

        # tsf
        secDict[tic]['feature_tsf14'] = talib.TSF(secDict[tic].C, 14) / secDict[tic].C
        secDict[tic]['feature_tsf63'] = talib.TSF(secDict[tic].C, 63) / secDict[tic].C

        for i in secDict[tic].columns:
            if 'feature_' in i:
                secDict[tic][f'{i}_angle5'] = talib.LINEARREG_ANGLE(secDict[tic][i], 5)

        for i in secDict[tic].columns:
            if 'feature_' in i:
                secDict[tic][f'{i}_var14'] = talib.VAR(secDict[tic][i], 14)

        daily_vol = getDailyVol(secDict[tic]['C'])
        secDict[tic] = pd.merge(secDict[tic], daily_vol, left_index=True, right_index=True, how='left')
        secDict[tic].rename(columns={'dailyVol': 'feature_dailyVol'}, inplace=True)

        # event sampling
        events = filters.cusum_filter(secDict[tic].C, secDict[tic].feature_dailyVol.median() * 0.75)
        secDict[tic].loc[events, 'event'] = 1
        secDict[tic]['event'] = secDict[tic]['event'].fillna(0)

        # get only tradeable bars
        if tic not in ['BTC', 'ETH']:
            secDict[tic] = secDict[tic].between_time('9:30', '15:59')

        cols = secDict[tic].columns
        cols = [s for s in cols if 'feature_' in s]
        secDict[tic] = secDict[tic].dropna(subset=cols)

        # make targets
        secDict[tic]['index_curr'] = secDict[tic].index
        for i in np.arange(400):
            secDict[tic][f'high+{i+1}'] = secDict[tic]['tradeableH'].shift(-(i + 1))
            secDict[tic][f'low+{i+1}'] = secDict[tic]['tradeableL'].shift(-(i + 1))
            secDict[tic][f'index+{i+1}'] = secDict[tic]['index_curr'].shift(-(i + 1))

        secDict[tic]['close+400'] = secDict[tic]['C'].shift(-(400))

        t = secDict[tic].parallel_apply(lambda x: TradeSim(x, 400, 2, 1), axis=1)
        t = pd.DataFrame(list(t))
        secDict[tic]['feature_target_R'] = t[0]
        secDict[tic]['feature_target_Type'] = t[1]
        secDict[tic]['feature_target_held_for'] = t[2]
        secDict[tic]['feature_target_B'] = t[3]

        # drop unwanted columns
        secDict[tic] = secDict[tic].drop(['avg_tick_size', 'tradeableH', 'tradeableL', 'event', 'index_curr'], axis=1)

        # save
        print(f'{tic} saved')
        pickle.dump(secDict[tic], open(f'docker_storage/Tick_Data/Const_Resampled_2017/dolDF_const_10_BarsPerDay_wFeats/{tic}_dolDF_const_10_BarsPerDay_wFeats.pkl', 'wb'))
    except Exception as e:
        print(f'error: {e}')


def update_transform_stage_three(tic_lst):
    for ticker in tic_lst:
        make_all_features(ticker)