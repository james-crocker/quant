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
import numpy
import pandas
import pytz
import datetime as dt
from datetime import datetime, timedelta
from pandas import concat
from collections import defaultdict
#from pandas import Series, TimeSeries, DataFrame, Panel

#2013-11-04 :: 2013-11-06 (3 Trading Days)

# window_length SHOULD EQUAL context.metric_period
# BUG with getting daily windows of minute data:
# https://www.quantopian.com/posts/batch-transform-in-minute-backtests

@batch_transform(window_length=1, refresh_period=0)
def accumulate_data(data):
    return data

def tree():
    return defaultdict(tree)

def initialize(context):
    context.date_backtest_end = dt.datetime(2013, 11, 29, 20, 0, 0, 0, pytz.utc) # NOTE May be 20 or 21 depending
    context.algo_volatility = 'RS' # Process previous days metrics 'RS|GK|PA|CE'
    # Period Volatility and Performance period in DAYS
    context.metric_period = 42 # 3 months (days) LOOKBACK
    context.metric_period_buy = 21 # Buy period (days)
    ##context.metric_periodMean = 2 # Volatility period. Chose a MULTIPLE of metric_period
    # Set Performance vs. Volatility factors (7.0, 3.0 from Grossman GMRE
    context.factor_performance = 0.7
    context.factor_volatility = 0.3
    # Re-enact pricing from original Quast code
    context.order_limits_buy = False
    context.order_limits_sell = False
    context.factor_price_buy = 0.0
      
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
    context.log_warn = False
    context.log_buy = False
    context.log_sell = False
    context.log_hold = True
    context.log_rank = True
    context.log_debug = False
         
    context.date_next = None
    context.bars = None
    context.basket_stocks_best = None
    context.basket_period_ochlv = []
    context.basket_stocks_active = []
    context.oid_buy = None
    context.oid_sell = None
    context.begin_cash = None
    context.begin_date = None
    context.count_buy = 0
    context.count_sell = 0
    context.basket_analyzed = False
    context.stock_current = None
    
def get_min_max(arr):
    return min(arr.values()), max(arr.values())
            
def get_finite_bars(context):
    basket = context.basket_stocks_active
    bars = context.bars
    bars_finite = tree()
    
    for s in basket:
        for item in bars:
            bars_finite[s.sid][item] = [price for price in bars[item][s.sid] if not math.isnan(price)]
            
    if context.log_warn is True:
        for s in basket:
            for item in bars:
                count = 0
                for val in bars[item][s.sid]:
                    if math.isnan(val):
                        print('[%s] FOUND %s AT %s' % (s.sid, val, bars[item][s.sid].index[count]))
                    count += 1
            
    return bars_finite
        
def get_basket_period_ochlv(context):    
    # Converts the MINUTE intra-day trading data into a DAYS OCHL and VOLUME data
    # Removes NaN data using finite data only
    basket = context.basket_stocks_active
    ochlv = tree()
    
    bars_finite = get_finite_bars(context)
    
    for s in basket:
        ochlv[s.sid]['price']['open'] = bars_finite[s.sid]['open_price'][0] # Closest to actual open_price at market open
        ochlv[s.sid]['price']['close'] = bars_finite[s.sid]['close_price'][-1]
        ochlv[s.sid]['price']['high'] = max(bars_finite[s.sid]['high'])
        ochlv[s.sid]['price']['low'] = min(bars_finite[s.sid]['low'])

        ochlv[s.sid]['volume']['open'] = bars_finite[s.sid]['volume'][0] # Closest to actual open_price at market open
        ochlv[s.sid]['volume']['close'] = bars_finite[s.sid]['volume'][-1]
        ochlv[s.sid]['volume']['high'] = max(bars_finite[s.sid]['volume'])
        ochlv[s.sid]['volume']['low'] = min(bars_finite[s.sid]['volume'])        

        #p = ochlv[s.sid]['price']
        #v = ochlv[s.sid]['volume']        
        #log.info('[%s] PRICE O %s, C %s, H %s, L %s VOLUME O %s, C %s, H %s, L %s' % (s.sid, p['open'], p['close'], p['high'], p['low'], v['open'], v['close'], v['high'], v['low']))
            
    return ochlv

def get_basket_price_performance(context):
    values = context.basket_period_ochlv
    basket = context.basket_stocks_active
    performances = tree()
    
    # Gather period performance
    for s in basket:
        price_begin = values[0][s.sid]['price']['close']
        price_end = values[-1][s.sid]['price']['close']
        performances[s.sid]['price']['performance'] = (price_begin - price_end) / price_begin
        
    return performances

def get_volatility(algo, stock, metric, ochlv):
    # http://www.tsresearch.com/public/volatility/historical/
    # http://www.morningstar.com/InvGlossary/historical_volatility.aspx
    # http://en.wikipedia.org/wiki/Volatility_(finance)   
    O = ochlv[stock][metric]['open']
    C = ochlv[stock][metric]['close']
    H = ochlv[stock][metric]['high']
    L = ochlv[stock][metric]['low']
                        
    if algo == 'RS':
        # Calculate the daily Roger and Satchell volatility
        return (math.log(H / C) * math.log(H / O)) + (math.log(L / C) * math.log(L / O))
    elif algo == 'GK':
        # Calculate the daily Garman & Klass volatility
        a = 0.511 * math.pow((math.log(H / L)), 2)
        b = 0.019 * math.log(C / O) * math.log((H * L) / math.pow(O, 2))
        c = 2.0 * math.log(H / O) * math.log(L / O)
        return a - b - c
    elif algo == 'PA':
        # Calculate the daily Parkinson volatility 
        return math.pow(math.log(H / L), 2)
    elif algo == 'CE':
        return C
    else:
        return None

def get_volatility_period(algo, period, v):
    # http://www.tsresearch.com/public/volatility/historical/
    # http://www.morningstar.com/InvGlossary/historical_volatility.aspx
    # http://en.wikipedia.org/wiki/Volatility_(finance)
    annualization = math.sqrt(252)
     
    if algo == 'CE':
        return numpy.std(v) * annualization
    elif algo == 'PA':
        return math.sqrt((sum(v) / (math.pow(4, period) * math.log(2)))) * annualization
    else:
        return math.sqrt(sum(v) / period) * annualization
                                      
def get_basket_period_metrics(context):
    # Generate volatility data for a given period then average over the period
    values = context.basket_period_ochlv
    basket = context.basket_stocks_active
    period_buy = context.metric_period_buy # Shorter '21'
    period_lookback = context.metric_period + period_buy # Longer '63'
    period = period_lookback / period_buy
    metric_group = ['price', 'volume']
    algo_group = ['RS', 'GK', 'PA', 'CE']

    # Gather period lookback performance
    metrics = get_basket_price_performance(context)
    #for s in [12915, 23134]:
    #    print('[%s] %s PERFORMANCE %s' % (s, 'price', metrics[s]['price']['performance']))

    # Gather the buy period volatility
    vp = tree() # period volatility
    for vol_period in xrange(0, period):

        begin = vol_period * period_buy
        end = begin + period_buy
                    
        # Get PRICE and VOLUME volatilities
        for metric in metric_group:            
            for s in basket:
                for algo in algo_group:
                    vd = [] # daily volatility
                    for ochlv in values[begin:end]:                                                        
                        vd.append(get_volatility(algo, s.sid, metric, ochlv))
            
                    if not vp[s.sid][metric][algo]:
                        vp[s.sid][metric][algo] = []
                    
                    vp[s.sid][metric][algo].append(get_volatility_period(algo, period_buy, vd)) # Accumulate the period volatility (generally 20 days)   
                
    # Average of period volatilities               
    for metric in metric_group:     
        for s in basket:
            for algo in algo_group:
                metrics[s.sid][metric]['volatility'][algo] = sum(vp[s.sid][metric][algo]) / period
                #if s.sid == 12915 or s.sid == 23134:
                #    print('[%s] %s VOLATILITY [%s] %s' % (s.sid, metric, algo, metrics[s.sid][metric]['volatility'][algo]))
                
    return metrics

def get_stock_best(context, metrics):
    #if context.metric_periodMeanCount <= context.metric_period / context.metric_period
    basket = context.basket_stocks_active
    algo = context.algo_volatility
    p_factor = context.factor_performance
    v_factor = context.factor_volatility
    
    stock_ranks = {}
    stock_best = None   
    
    performances = {}
    volatilities = {} 
                
    for s in basket:
        performances[s.sid] = metrics[s.sid]['price']['performance']
        volatilities[s.sid] = metrics[s.sid]['price']['volatility'][algo]       
        
    # Determine min/max of each.  NOTE: volatility is switched
    # since a low volatility should be weighted highly.
    min_p, max_p = get_min_max(performances)
    max_v, min_v = get_min_max(volatilities)
                    
    # Normalize the performance and volatility values to a range
    # between [0..1] then rank them based on a 70/30 weighting.
    for s in basket:
        rank = None
        p_norm = (performances[s.sid] - min_p) / (max_p - min_p)
        v_norm = (volatilities[s.sid] - min_v) / (max_v - min_v)

        if context.log_debug is True:
                log.debug('[%s] normP %s, normV %s' % (s.sid, p_norm, v_norm))

        if not math.isnan(p_norm) and not math.isnan(v_norm):
            # Adjust volatility for EDV by 50%
            if s.sid == 22887:
                rank = (p_norm * p_factor) + ((v_norm * 0.66) * v_factor)
            else:
                rank = (p_norm * p_factor) + (v_norm * v_factor)
                    
            stock_ranks[s] = rank
    
    if len(stock_ranks) > 0:
        if context.log_debug is True and len(stock_ranks) < len(basket):
            log.debug('FEWER STOCK RANKINGS THAN IN STOCK BASKET!')
        if context.log_rank is True:
            for s in sorted(stock_ranks, key=stock_ranks.get, reverse=True):
                log.info('RANK [%s] %s' % (s, stock_ranks[s]))
            log.info('---')
        stock_best = max(stock_ranks, key=stock_ranks.get)
    else:
        if context.log_debug is True:
            log.debug('NO STOCK RANKINGS FOUND IN BASKET; BEST STOCK IS: NONE')
                    
    return stock_best

def positions(context):
    positions = False
    for p in context.portfolio.positions.values():
        if (p.amount > 0):
            positions = True
            break
                
    return positions   

def positions_sell(context):
    oid = None
    positions = context.portfolio.positions
           
    try:
        price_sell_stop = context.price_sell_stop
    except:
        price_sell_stop = 0.0
        
    try:
        price_sell_limit = context.price_sell_limit
    except:
        price_sell_limit = 0.0

    for p in positions.values():
        if (p.amount > 0):

            amount = p.amount
            price = p.last_sale_price
            orderValue = price * amount
                
            stop = price - price_sell_stop
            limit = stop - price_sell_limit

            if context.order_limits_sell is True:
                if context.log_sell is True:
                    log.info('SELL [%s] (%s) @ $%s (%s) STOP $%s LIMIT $%s' % (p.sid, -amount, price, orderValue, stop, limit))
                oid = order(p.sid, -amount, limit_price = limit, stop_price = stop)
            else:
                if context.log_sell is True:
                    log.info('SELL [%s] (%s) @ $%s (%s) MARKET' % (p.sid, -amount, price, orderValue))
                oid = order(p.sid, -amount)
                
            context.count_sell += 1
            
    return oid

def positions_buy(context, data):
    oid = None

    cash = context.portfolio.cash   
    s = context.basket_stocks_best
    
    try:
        factor_price_buy = context.factor_price_buy
    except:
        factor_price_buy = 0.0
        
    try:
        price_buy_stop = context.price_buy_stop
    except:
        price_buy_stop = 0.0
        
    try:
        price_buy_limit = context.price_buy_limit
    except:
        price_buy_limit = 0.0

    price = data[s.sid].open_price            
    amount = math.floor(cash / (price + factor_price_buy))
    orderValue = price * amount

    stop = price + price_buy_stop
    limit = stop + price_buy_limit

    if cash <= 0 or cash < orderValue:
        log.info('BUY ABORT! cash $%s < orderValue $%s' % (cash, orderValue))
    else:           
        if context.log_buy is True: 
            if context.order_limits_buy is True:
                log.info('BUY [%s] %s @ $%s ($%s of $%s) STOP $%s LIMIT $%s' % (s, amount, price, orderValue, cash, stop, limit))
            else:    
                log.info('BUY [%s] %s @ $%s ($%s of $%s) MARKET' % (s, amount, price, orderValue, cash))

        if context.order_limits_buy is True:
            oid = order(s, amount, limit_price = limit, stop_price = stop)
        else:
            oid = order(s, amount)
            
        context.count_buy += 1

    return oid

                            
'''
  The main proccessing function.  This is called and passed data
'''
def handle_data(context, data):  
    
    now = get_datetime()
    #buyTime = now + dt.timedelta(hours=2)
    
    # Warn on negative portfolio cash
    if context.log_warn is True and context.portfolio.cash < 0:
        log.warn('NEGATIVE CASH %s' % context.portfolio.cash)
    
    # Process the PREVIOUS DAY
    if context.date_next is not None:                    
        if now >= context.date_next or now == context.date_backtest_end:
            if now == context.date_backtest_end:
                context.bars = accumulate_data(data)
            
            # Collect OCHLV prices for the period
            context.basket_period_ochlv.append(get_basket_period_ochlv(context))      
            #print context.basket_period_ochlv      
            
            # Fill the lookback period and then get best stock once filled
            if len(context.basket_period_ochlv) == (context.metric_period + context.metric_period_buy):
                context.basket_stocks_best = get_stock_best(context, get_basket_period_metrics(context))
                context.basket_analyzed = True                
                del context.basket_period_ochlv[:context.metric_period_buy]
                
            del context.basket_stocks_active[:]
            context.date_next = None
            
    # Check if SELL completed        
    if context.oid_sell is not None:
        orderObj = get_order(context.oid_sell)
        if orderObj.filled == orderObj.amount:
            # Good to buy next holding
            if context.log_sell is True:
                log.info('SELL ORDER COMPLETED %s' % now)
            context.oid_sell = None
            context.oid_buy = positions_buy(context, data)
            context.stock_current = context.basket_stocks_best
            context.basket_stocks_best = None
        else:
            if context.log_sell is True:
                log.info('SELL ORDER *NOT* COMPLETED')
            #return
    
    # Check if BUY completed
    if context.oid_buy is not None:
        orderObj = get_order(context.oid_buy)
        if orderObj.filled == orderObj.amount:
            if context.log_buy is True:
                log.info('BUY ORDER COMPLETED %s' % now)
            context.oid_buy = None
        else:
            if context.log_buy is True:
                log.info('BUY ORDER *NOT* COMPLETED')
            #return
                        
    if context.basket_analyzed is True:
        if context.basket_stocks_best is not None:
            if (context.stock_current == context.basket_stocks_best):
                # Hold current
                if context.log_hold is True:
                    log.info('HOLD [%s]' % context.stock_current)                
            elif (context.stock_current is None):
                # Buy best
                log.info('BUYING [%s]' % context.basket_stocks_best)
                context.stock_current = context.basket_stocks_best
                context.oid_buy = positions_buy(context, data)
            else:
                # Sell ALL and Buy best
                log.info('BUYING [%s]' % context.basket_stocks_best)
                if positions(context):
                    context.oid_sell = positions_sell(context)
                else:
                    context.oid_buy = positions_buy(context, data)
        else:
            if context.log_warn is True:
                log.warn('COULD NOT FIND A BEST STOCK! BEST STOCK IS *NONE*')
                
        context.basket_analyzed = False
        
    # NOTE: record() can ONLY handle five elements in the graph. Any more than that will runtime error once 5 are exceeded.      
    record(buy=context.count_buy, sell=context.count_sell, cash=context.portfolio.cash, pnl=context.portfolio.pnl)    
        
    context.bars = accumulate_data(data)

    if context.bars is None:
        return
    
    if context.date_next is None:
        # Ensure stocks are only traded if possible.  
        for s in context.basket.values():
            if now > s.security_start_date:
                context.basket_stocks_active.append(s)
                
        context.date_next = dt.datetime(int(now.year), int(now.month), int(now.day), 0, 0, 0, 0, pytz.utc) + dt.timedelta(days=1)
    
    #print type(bars['open_price'][12915]) # TimeSeries
    #print type(bars['open_price']) # DataFrame
    #print type(bars) # Panel
    
    #Dimensions: 6 (items) x 390 (major_axis) x 7 (minor_axis)
    #Items axis: close_price to volume
    #Major_axis axis: 2013-11-04 14:31:00+00:00 to 2013-11-04 21:00:00+00:00
    #Minor_axis axis: 24705 to 23134
    
    return