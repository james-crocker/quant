import math
import pandas
import pytz
import datetime as dt
from datetime import datetime, timedelta
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
    context.lastDate = dt.datetime(2013, 11, 8, 21, 0, 0, 0, pytz.utc)
    
def getStock(context, data):
    
    print data[0]
    print data[-1]
    
'''
  The main proccessing function.  This is called and passed data
'''
def handle_data(context, data):  
    
    now = get_datetime()
                
    if context.nextDate is not None:
        if now == context.lastDate:
            context.bars = accumulateData(data)
        if now.day == context.nextDate.day or now == context.lastDate:
            #print('now day %s is the nextdate day %s' % (now, context.nextDate))
            #print 'process previous day'
            print context.bars
            context.bars = None
            context.nextDate = None
        
    bars = accumulateData(data)

    if bars is None:
        return
    
    context.bars = bars

    if context.nextDate is None:
        context.nextDate = dt.datetime(int(now.year), int(now.month), int(now.day), 0, 0, 0, 0, pytz.utc) + dt.timedelta(days=1)
    
    #print type(bars['open_price'][12915]) # TimeSeries
    #print type(bars['open_price']) # DataFrame
    #print type(bars) # Panel
    
    #Dimensions: 6 (items) x 390 (major_axis) x 7 (minor_axis)
    #Items axis: close_price to volume
    #Major_axis axis: 2013-11-04 14:31:00+00:00 to 2013-11-04 21:00:00+00:00
    #Minor_axis axis: 24705 to 23134
    
    #stock = getStock(context, datapanel['close_price'][12915])
    return

    