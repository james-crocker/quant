# Global Market Rotation Enhanced (GMRE) - MINUTE DATA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# TRADING PLATFORM: Quantopian

# ABOUT:
# This strategy rotates between six global market ETFs on a monthly
# basis.  Each month the performance and mean 20-day volatility over
# the last 3 months are used to rank which ETF should be invested 
# in for the coming month.
#
# This strategy processes MINUTE data to calculate rotation.

# ATTRIBUTION:
# GMRE Strategy: Frank Grossman, 2013-08-09
#    SeekingAlpha: http://seekingalpha.com/article/1622832-a-global-market-rotation-strategy-with-an-annual-performance-of-41-4-since-2003
#    Logical-Invest: http://www.logical-invest.com/strategy/GlobalMarketRotationEnhancedStrategy.htm
# Quantopian Community: https://www.quantopian.com/posts/global-market-rotation-strategy-buggy-implementation
# Quantopian Author: Ed Bartosh, 2013-09-13
# Quantopian Author: David Quast, 2013-09-13
# Quantopian Author: James Crocker, 2013-11-14 james@constantsc.net

import math
import pandas
import pytz
import datetime as dt
from datetime import datetime, timedelta
from pandas import concat
from collections import defaultdict
#from pandas import Series, TimeSeries, DataFrame, Panel

#2013-11-04 :: 2013-11-06 (3 Trading Days)

# window_length SHOULD EQUAL context.metricPeriod
# BUG with getting daily windows of minute data:
# https://www.quantopian.com/posts/batch-transform-in-minute-backtests
@batch_transform(window_length=1, refresh_period=0)
def accumulateData(data):
    return data

def initialize(context):
    
    context.lastDate = dt.datetime(2013, 11, 29, 20, 0, 0, 0, pytz.utc) # NOTE May be 20 or 21 depending
    context.algoVolatility = 'RS' # Process previous days metrics 'RS|GK|PA|DV' DEFAULT is DV
    # Period Volatility and Performance period in DAYS
    context.metricPeriod = 42 # 3 months (days) LOOKBACK
    context.metricBuyPeriod = 21 # Buy period (days)
    ##context.metricPeriodMean = 2 # Volatility period. Chose a MULTIPLE of metricPeriod
    # Set Performance vs. Volatility factors (7.0, 3.0 from Grossman GMRE
    context.factorPerformance = 0.7
    context.factorVolatility = 0.3
    # Re-enact pricing from original Quast code
    context.orderBuyLimits = False
    context.orderSellLimits = False
    context.priceBuyFactor = 0.0
      
    context.basket = {
        12915: sid(12915), # MDY (SPDR S&P MIDCAP 400)
        21769: sid(21769), # IEV (ISHARES EUROPE ETF)
        24705: sid(24705), # EEM (ISHARES MSCI EMERGING MARKETS)
        23134: sid(23134), # ILF (ISHARES LATIN AMERICA 40)
        23118: sid(23118), # EPP (ISHARES MSCI PACIFIC EX JAPAN)
        22887: sid(22887), # EDV (VANGUARD EXTENDED DURATION TREASURY)
        40513: sid(40513), # ZIV (VelocityShares Inverse VIX Medium-Term)
    }
    
    # Set/Unset logging features for verbosity levels
    context.logWarn = False
    context.logBuy = False
    context.logSell = False
    context.logHold = True
    context.logRank = False
    context.logDebug = False
         
    context.nextDate = None
    context.bars = None
    context.basketStockBest = None
    context.basketPeriodOchlv = []
    context.basketStocksActive = []
    context.p = {}; context.v = {} 
    context.oidBuy = None
    context.oidSell = None
    context.cashStart = None
    context.dateStart = None
    context.buyCount = 0
    context.sellCount = 0
    context.basketAnalyzed = False
    context.currentStock = None

    
def tree(): return defaultdict(tree)
    
def getMinMax(arr):
    return min(arr.values()), max(arr.values())

def extendList(d1, d2):
    for s2 in d2:
        if s2 not in d1:
            d1.update({s2: [d2[s2]]})
        else:
            d1[s2].extend([d2[s2]])
            
def getFiniteBars(context):
    basket = context.basketStocksActive
    bars = context.bars
    finiteBars = tree()
    
    for s in basket:
        for item in bars:
            finiteBars[item][s.sid] = [price for price in bars[item][s.sid] if not math.isnan(price)]
            
    if context.logWarn is True:
        for s in basket:
            for item in bars:
                count = 0
                for val in bars[item][s.sid]:
                    if math.isnan(val):
                        print('[%s] FOUND %s AT %s' % (s.sid, val, bars[item][s.sid].index[count]))
                    count += 1
            
    return finiteBars
        
def basketPeriodOchlv(context):
    
    basket = context.basketStocksActive
    basketPeriodOchlv = tree()
    
    finiteBars = getFiniteBars(context)    
                
    for s in basket:
        H = max(finiteBars['high'][s.sid])
        L = min(finiteBars['low'][s.sid])
        O = finiteBars['close_price'][s.sid][0]
        C = finiteBars['close_price'][s.sid][-1]
        V = sum(finiteBars['volume'][s.sid])

        #print('[%s] O %s, C %s, H %s, L %s, V %s' % (s.sid, O, C, H, L, V))

        #print bars
        #prices = bars['close_price'][s.sid]
        #O = prices[0]
        #C = prices[-1]
        #H = prices.max()
        #L = prices.min()

        basketPeriodOchlv[s.sid]['open'] = O
        basketPeriodOchlv[s.sid]['close'] = C
        basketPeriodOchlv[s.sid]['high'] = H
        basketPeriodOchlv[s.sid]['low'] = L
        basketPeriodOchlv[s.sid]['volume'] = V
    
    return basketPeriodOchlv

def getVolatility(context, prices):
    
    algo = context.algoVolatility

    O = prices['open']
    C = prices['close']
    H = prices['high']
    L = prices['low']
    
    v = None
    
    # http://www.tsresearch.com/public/volatility/historical/
    if algo == 'RS':
        # Calculate the daily Roger and Satchell volatility
        # Since 'daily' the 1/T is skipped
        a = math.log(H/C)
        b = math.log(H/O)
        c = math.log(L/C)
        d = math.log(L/O)
        r = (a * b) + (c * d)
        v = math.sqrt(r)
    elif algo == 'GK':
        # Calculate the daily Garman & Klass volatility
        # Since 'daily' the 1/T is skipped
        a = 0.511 * math.pow((math.log(H/L)), 2)
        b = 0.019 * math.log(C/O) * math.log((H*L)/math.pow(O, 2))
        c = 2.0 * math.log(H/O) * math.log(L/O)
        r = a - b - c
        v = math.sqrt(r)
    elif algo == 'PA':
        # Calculate the daily Parkinson volatility
        # Since 'daily' the 4^T is skipped
        a = math.pow(math.log(H/L), 2)
        b = 1 / (4 * math.log(2))
        r = b * a
        v = math.sqrt(r)
    else:    
        # Calculate the classical Daily Volatility
        # Since 'daily' the 1/T is skipped
        a = H - L
        b = H + L
        v = a/b
        
    return v
    

def getBasketPeriodMetrics(context):
    
    basketPeriodOchlv = context.basketPeriodOchlv
    
    beginOpen = {}; endClose = {}; p = {}; v = {}
        
    for values in basketPeriodOchlv:
        for sid in values:
            endClose[sid] = values[sid]['close']
            if sid not in beginOpen:
                beginOpen[sid] = values[sid]['open']
            if sid not in v:
                v[sid] = []
                
            print('[%s] VOLUME %s' % (sid, values[sid]['volume']))
            
            # Calculate period volatility
            v[sid].append(getVolatility(context, values[sid]))
            
    for sid in beginOpen:
        p[sid] = (endClose[sid] - beginOpen[sid]) / beginOpen[sid]
            
    return p, v

def getBestStock(context, p, v, volume):
           
    #if context.metricPeriodMeanCount <= context.metricPeriod / context.metricPeriod
    performances = {}; volatilities = {}; stockRanks = {}; bestStock = None
    
    basket = context.basketStocksActive
    period = context.metricPeriod
    pFactor = context.factorPerformance
    vFactor = context.factorVolatility    
                
    for s in basket:
        performances[s.sid] = p[s.sid]
        volatilities[s.sid] = sum(v[s.sid]) / period
        volume
        #print('[%s] PERIOD : p %s, v %s' % (s, p[s.sid], v[s.sid]))
                  
        # Determine min/max of each.  NOTE: volatility is switched
        # since a low volatility should be weighted highly.
        minP, maxP = getMinMax(performances)
        maxV, minV = getMinMax(volatilities)
                    
    # Normalize the performance and volatility values to a range
    # between [0..1] then rank them based on a 70/30 weighting.
    for s in basket:
        rank = None
        pNorm = (performances[s.sid] - minP) / (maxP - minP)
        vNorm = (volatilities[s.sid] - minV) / (maxV - minV)

        if context.logDebug is True:
                log.debug('[%s] normP %s, normV %s' % (s.sid, pNorm, vNorm))

        if not math.isnan(pNorm) and not math.isnan(vNorm):
            # Adjust volatility for EDV by 50%
            if s.sid == 22887:
                rank = (pNorm * pFactor) + ((vNorm * 0.5) * vFactor)
            else:
                rank = (pNorm * pFactor) + (vNorm * vFactor)
                    
            stockRanks[s] = rank
    
        if len(stockRanks) > 0:
            if context.logDebug is True and len(stockRanks) < len(basket):
                log.debug('FEWER STOCK RANKINGS THAN IN STOCK BASKET!')
            if context.logRank is True:
                for s in sorted(stockRanks, key=stockRanks.get, reverse=True):
                    log.info('RANK [%s] %s' % (s, stockRanks[s]))
                
            bestStock = max(stockRanks, key=stockRanks.get)
        else:
            if context.logDebug is True:
                log.debug('NO STOCK RANKINGS FOUND IN BASKET; BEST STOCK IS: NONE')
                    
    return bestStock

def hasPositions(context):
    hasPositions = False
    for p in context.portfolio.positions.values():
        if (p.amount > 0):
            hasPositions = True
            break
                
    return hasPositions   

def sellPositions(context):
    oid = None
    positions = context.portfolio.positions
           
    try:
        priceSellStop = context.priceSellStop
    except:
        priceSellStop = 0.0
        
    try:
        priceSellLimit = context.priceSellLimit
    except:
        priceSellLimit = 0.0

    for p in positions.values():
        if (p.amount > 0):

            amount = p.amount
            price = p.last_sale_price
            orderValue = price * amount
                
            stop = price - priceSellStop
            limit = stop - priceSellLimit

            if context.orderSellLimits is True:
                if context.logSell is True:
                    log.info('SELL [%s] (%s) @ $%s (%s) STOP $%s LIMIT $%s' % (p.sid, -amount, price, orderValue, stop, limit))
                oid = order(p.sid, -amount, limit_price = limit, stop_price = stop)
            else:
                if context.logSell is True:
                    log.info('SELL [%s] (%s) @ $%s (%s) MARKET' % (p.sid, -amount, price, orderValue))
                oid = order(p.sid, -amount)
                
            context.sellCount += 1
            
    return oid

def buyPositions(context, data):
    oid = None

    cash = context.portfolio.cash   
    s = context.basketStockBest
    
    try:
        priceBuyFactor = context.priceBuyFactor
    except:
        priceBuyFactor = 0.0
        
    try:
        priceBuyStop = context.priceBuyStop
    except:
        priceBuyStop = 0.0
        
    try:
        priceBuyLimit = context.priceBuyLimit
    except:
        priceBuyLimit = 0.0

    price = data[s.sid].open_price            
    amount = math.floor(cash / (price + priceBuyFactor))
    orderValue = price * amount

    stop = price + priceBuyStop
    limit = stop + priceBuyLimit

    if cash <= 0 or cash < orderValue:
        log.info('BUY ABORT! cash $%s < orderValue $%s' % (cash, orderValue))
    else:           
        if context.logBuy is True: 
            if context.orderBuyLimits is True:
                log.info('BUY [%s] %s @ $%s ($%s of $%s) STOP $%s LIMIT $%s' % (s, amount, price, orderValue, cash, stop, limit))
            else:    
                log.info('BUY [%s] %s @ $%s ($%s of $%s) MARKET' % (s, amount, price, orderValue, cash))

        if context.orderBuyLimits is True:
            oid = order(s, amount, limit_price = limit, stop_price = stop)
        else:
            oid = order(s, amount)
            
        context.buyCount += 1

    return oid

                            
'''
  The main proccessing function.  This is called and passed data
'''
def handle_data(context, data):  
    
    now = get_datetime()
    buyTime = now + dt.timedelta(hours=2)
    
    # Warn on negative portfolio cash
    if context.logWarn is True and context.portfolio.cash < 0:
        log.warn('NEGATIVE CASH %s' % context.portfolio.cash)
    
    # Process the PREVIOUS DAY
    if context.nextDate is not None:                    
        if now >= context.nextDate or now == context.lastDate:
            if now == context.lastDate:
                context.bars = accumulateData(data)
            
            # Collect OCHLV prices for the period
            context.basketPeriodOchlv.append(basketPeriodOchlv(context))            
            
            # Fill the lookback period and then get best stock once filled
            if len(context.basketPeriodOchlv) == (context.metricPeriod + context.metricBuyPeriod):
                p, v = getBasketPeriodMetrics(context)
                context.basketStockBest = getBestStock(context, p, v)
                context.basketAnalyzed = True                
                del context.basketPeriodOchlv[:context.metricBuyPeriod]
                
            del context.basketStocksActive[:]
            context.nextDate = None
            
    # Check if SELL completed        
    if context.oidSell is not None:
        orderObj = get_order(context.oidSell)
        if orderObj.filled == orderObj.amount:
            # Good to buy next holding
            if context.logSell is True:
                log.info('SELL ORDER COMPLETED %s' % now)
            context.oidSell = None
            context.oidBuy = buyPositions(context, data)
            context.currentStock = context.basketStockBest
            context.basketStockBest = None
        else:
            if context.logSell is True:
                log.info('SELL ORDER *NOT* COMPLETED')
            #return
    
    # Check if BUY completed
    if context.oidBuy is not None:
        orderObj = get_order(context.oidBuy)
        if orderObj.filled == orderObj.amount:
            if context.logBuy is True:
                log.info('BUY ORDER COMPLETED %s' % now)
            context.oidBuy = None
        else:
            if context.logBuy is True:
                log.info('BUY ORDER *NOT* COMPLETED')
            #return
                        
    if context.basketAnalyzed is True:
        if context.basketStockBest is not None:
            if (context.currentStock == context.basketStockBest):
                # Hold current
                if context.logHold is True:
                    log.info('HOLD [%s]' % context.currentStock)                
            elif (context.currentStock is None):
                # Buy best
                log.info('BUYING [%s]' % context.basketStockBest)
                context.currentStock = context.basketStockBest
                context.oidBuy = buyPositions(context, data)
            else:
                # Sell ALL and Buy best
                log.info('BUYING [%s]' % context.basketStockBest)
                if hasPositions(context):
                    context.oidSell = sellPositions(context)
                else:
                    context.oidBuy = buyPositions(context, data)
        else:
            if context.logWarn is True:
                log.warn('COULD NOT FIND A BEST STOCK! BEST STOCK IS *NONE*')
                
        context.basketAnalyzed = False
        
    # NOTE: record() can ONLY handle five elements in the graph. Any more than that will runtime error once 5 are exceeded.      
    record(buy=context.buyCount, sell=context.sellCount, cash=context.portfolio.cash, pnl=context.portfolio.pnl)
    
        
    context.bars = accumulateData(data)

    if context.bars is None:
        return
    
    if context.nextDate is None:
        # Ensure stocks are only traded if possible.  
        # (e.g) EDV doesn't start trading until late 2007, without
        # this, any backtest run before that date would fail.
        for s in context.basket.values():
            if now > s.security_start_date:
                context.basketStocksActive.append(s)
                
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