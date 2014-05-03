import math
import pandas
import pytz
import datetime as dt
from datetime import datetime, timedelta
from pandas import concat
#from pandas import Series, TimeSeries, DataFrame, Panel

#2013-11-04 :: 2013-11-06 (3 Trading Days)

# window_length SHOULD EQUAL context.metricPeriod
# BUG with getting daily windows of minute data:
# https://www.quantopian.com/posts/batch-transform-in-minute-backtests
@batch_transform(window_length=1, refresh_period=0)
def accumulateData(data):
    return data

def initialize(context):
      
    context.basket = {
        12915: sid(12915), # MDY (SPDR S&P MIDCAP 400)
        21769: sid(21769), # IEV (ISHARES EUROPE ETF)
        24705: sid(24705), # EEM (ISHARES MSCI EMERGING MARKETS)
        23134: sid(23134), # ILF (ISHARES LATIN AMERICA 40)
        23118: sid(23118), # EPP (ISHARES MSCI PACIFIC EX JAPAN)
        22887: sid(22887), # EDV (VANGUARD EXTENDED DURATION TREASURY)
        40513: sid(40513), # ZIV (VelocityShares Inverse VIX Medium-Term)
    }
         
    context.nextDate = None
    context.bars = None
    context.barsPeriod = None
    context.lastDate = dt.datetime(2013, 10, 18, 20, 0, 0, 0, pytz.utc) # NOTE May be 20 or 21 depending
    context.metricPeriodCount = 0
    
    # Period Volatility and Performance period in DAYS
    context.metricPeriod = 63 # 3 months LOOKBACK
    context.periodVolatility = 21 # Volatility period. Chose a MULTIPLE of metricPeriod
    
    
'''
  The main proccessing function.  This is called and passed data
'''
def handle_data(context, data):  
    
    now = get_datetime()
    
    if context.nextDate is not None:                    
        if now >= context.nextDate or now == context.lastDate:
            if now == context.lastDate:
                context.bars = accumulateData(data)
            
            context.nextDate = None
            context.metricPeriodCount += 1
            
            if context.barsPeriod is None:
                context.barsPeriod = context.bars
            else:
                context.barsPeriod = concat([context.barsPeriod, context.bars])
                
            # Process collected metricPeriod
            if context.metricPeriodCount == context.metricPeriod:
                print context.barsPeriod
                context.barsPeriod = None
                context.metricPeriodCount = 0
        
    context.bars = accumulateData(data)

    if context.bars is None:
        return
    
    if context.nextDate is None:
        context.nextDate = dt.datetime(int(now.year), int(now.month), int(now.day), 0, 0, 0, 0, pytz.utc) + dt.timedelta(days=1)
        #print('now %s, nextDate %s' % (now, context.nextDate))
    
    #print type(bars['open_price'][12915]) # TimeSeries
    #print type(bars['open_price']) # DataFrame
    #print type(bars) # Panel
    
    #Dimensions: 6 (items) x 390 (major_axis) x 7 (minor_axis)
    #Items axis: close_price to volume
    #Major_axis axis: 2013-11-04 14:31:00+00:00 to 2013-11-04 21:00:00+00:00
    #Minor_axis axis: 24705 to 23134
    
    #stock = getStock(context, datapanel['close_price'][12915])
    return


        # Calculate daily performance
        p[s.sid] = (C - O)/O
        if s.sid == 40513:
            print('[%s] O %s, C %s, H %s, L %s' % (s, O, C, H, L))
        
        # http://www.tsresearch.com/public/volatility/historical/
        if algo == 'RS':
            # Calculate the daily Roger and Satchell volatility
            # Since 'daily' the 1/T is skipped
            a = math.log(H/C)
            b = math.log(H/O)
            c = math.log(L/C)
            d = math.log(L/O)
            r = (a * b) + (c * d)
            v[s.sid] = math.sqrt(r)
        elif algo == 'GK':
            # Calculate the daily Garman & Klass volatility
            # Since 'daily' the 1/T is skipped
            a = 0.511 * math.pow((math.log(H/L)), 2)
            b = 0.019 * math.log(C/O) * math.log((H*L)/math.pow(O, 2))
            c = 2.0 * math.log(H/O) * math.log(L/O)
            r = a - b - c
            v[s.sid] = math.sqrt(r)
        elif algo == 'PA':
            # Calculate the daily Parkinson volatility
            # Since 'daily' the 4^T is skipped
            a = math.pow(math.log(H/L), 2)
            b = 1 / (4 * math.log(2))
            r = b * a
            v[s] = math.sqrt(r)
        else:    
            # Calculate the classical daily volatility
            # Since 'daily' the 1/T is skipped
            a = H - L
            b = H + L
            v[s.sid] = a/b
      
        #print('[%s] open %s, close %s, max %s, min %s' % (s, O, C, H, L))
        #print('[%s] PERF %s, RS %s, GK %s, PA %s, DV %s' % (s, perf[s], vRS[s], vGK[s], vPA[s], vDV[s]))
        
    return p, v   
    