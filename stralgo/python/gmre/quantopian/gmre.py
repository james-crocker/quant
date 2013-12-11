# Global Market Rotation Enhanced (GMRE) - Roger & Satchell Volatility

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

# window_length SHOULD EQUAL context.metricPeriod
@batch_transform(window_length=63)
def accumulateData(data):
    return data

def initialize(context):
    
    # Trade on boundary of first trading day of MONTH or set DAYS
    # DAYS|MONTH
    #context.boundaryTrade = 'DAYS'
    #context.boundaryDays = 21
    context.boundaryTrade = 'MONTH'
    
    # Set the last date for the FORECAST BEST Buy
    context.lastForecastYear = 2013
    context.lastForecastMonth = 8
    context.lastForecastDay = 30
       
    # Set Performance vs. Volatility factors (7.0, 3.0 from Grossman GMRE
    context.factorPerformance = 0.7
    context.factorVolatility = 0.3
      
    # Period Volatility and Performance period in DAYS
    context.metricPeriod = 63 # 3 months LOOKBACK
    context.periodVolatility = 21 # Volatility period. Chose a MULTIPLE of metricPeriod

    # To prevent going 'negative' on cash account set stop, limit and price factor >= stop
    #context.orderBuyLimits = False
    #context.orderSellLimits = False
    ##context.priceBuyStop = None
    ##context.priceBuyLimit = None
    ##context.priceSellStop = None
    ##context.priceSellLimit = None
    #context.priceBuyFactor = 3.03 # Buffering since buys and sells DON'T occur on the same day.
    
    # Re-enact pricing from original Quast code
    context.orderBuyLimits = False
    context.orderSellLimits = False
    context.priceBuyFactor = 0.0
    
    # Factor commission cost
    #set_commission(commission.PerShare(cost=0.03))
    #set_commission(commission.PerTrade(cost=15.00))
        
    context.basket = {
        12915: sid(12915), # MDY (SPDR S&P MIDCAP 400)
        21769: sid(21769), # IEV (ISHARES EUROPE ETF)
        24705: sid(24705), # EEM (ISHARES MSCI EMERGING MARKETS)
        23134: sid(23134), # ILF (ISHARES LATIN AMERICA 40)
        23118: sid(23118), # EPP (ISHARES MSCI PACIFIC EX JAPAN)
        22887: sid(22887), # EDV (VANGUARD EXTENDED DURATION TREASURY)
        40513: sid(40513), # ZIV (VelocityShares Inverse VIX Medium-Term)
        #26432: sid(26432), # FEZ
        #23911: sid(23911), # SHY
    } 
    
    # Set/Unset logging features for verbosity levels
    context.logWarn = False
    context.logBuy = False
    context.logSell = False
    context.logHold = True
    context.logRank = False
    context.logDebug = False
        
    # SHOULDN'T NEED TO MODIFY REMAINING VARIABLES        
    # Keep track of the current month.
    context.cashStart = None
    context.dateStart = None
    context.currentDayNum = None
    context.currentMonth = None
    context.currentStock = None
    context.nextStock = None
    context.oidBuy = None
    context.oidSell = None
    
    context.buyCount = 0
    context.sellCount = 0
    
def getMinMax(arr):
    return min(arr.values()), max(arr.values())

def rsVolatility(period, openPrices, closePrices, highPrices, lowPrices):
    # Rogers and Satchell (1991)
    r = []
    
    for i in xrange(0, period):
        a = math.log(highPrices[i] / closePrices[i])
        b = math.log(highPrices[i] / openPrices[i])
        c = math.log(lowPrices[i] / closePrices[i])
        d = math.log(lowPrices[i] / openPrices[i])
        r.append( a*b + c*d )
        
    # Take the square root of the sum over the period - 1.  Then multiply
    # that by the square root of the number of trading days in a year
    vol = math.sqrt(sum(r) / period) * math.sqrt(252/period)
    
    return vol
    
def getStockMetrics(context, openPrices, closePrices, highPrices, lowPrices):
    # Get the prices
    
    # Frank GrossmannComments (114) 
    # You can use the 20 day volatility averaged over 3 month.
    # For the ranking I calculate the 3 month performance of all ETF's and normalise between 0-1.
    # The best will have 1. Then I calculate the medium 3 month 20 day volatility and also normalize from 0-1.
    # Then I used Ranking= 0.7*performance +0.3*volatility.
    # This will give me a ranking from 0-1 from which I will take the best.
    
    period = context.metricPeriod
    periodV = context.periodVolatility
    volDays = periodV - 1
    periodRange = period / volDays

    # Calculate the period performance
    start = closePrices[-period] # First item
    end = closePrices[-1] # Last item

    performance = (end - start) / start    
        
    # Calculate 20-day volatility for the given period
    v = []
    x = 0
    for i in xrange(-periodRange, 0):            
        x = i * periodV
        y = x + volDays
        if context.logDebug is True:
            log.debug('period %s, pV %s, volDays %s, i %s, x %s, y %s, lenopenprices %s' % (period, periodV, volDays, i, x, y, len(openPrices)))
        v.append(rsVolatility(volDays, openPrices[x:y], closePrices[x:y], highPrices[x:y], lowPrices[x:y]))
    
    volatility = sum(v) / periodRange
    
    return performance, volatility

def getBestStock(context, data, stocks):
    
    # Frank GrossmannComments (114)
    # For the ranking, I also use the volatility of the ETFs. 
    # While this is not so important for the 5 Global market ETFs, it is important to lower the EDV ranking
    # a little bit, according to the higher volatility of the EDV ETF. EDV has a medium 20-day volatility, 
    # which is roughly 50% higher than the volatility of the 5 global market ETFs. This results in higher 
    # spikes during small market turbulence and the model would switch too early between shares (our 5 ETFs)
    # and treasuries .
    
    performances = {}
    volatilities = {}
        
    # Get performance and volatility for all the stocks
    for s in stocks:
        p, v = getStockMetrics(context, data['open_price'][s.sid], data['close_price'][s.sid], data['high'][s.sid], data['low'][s.sid])
        performances[s.sid] = p
        volatilities[s.sid] = v
    
    # Determine min/max of each.  NOTE: volatility is switched
    # since a low volatility should be weighted highly.
    minP, maxP = getMinMax(performances)
    maxV, minV = getMinMax(volatilities)
    
    # Normalize the performance and volatility values to a range
    # between [0..1] then rank them based on a 70/30 weighting.
    stockRanks = {}
    for s in stocks:
        p = (performances[s.sid] - minP) / (maxP - minP)
        v = (volatilities[s.sid] - minV) / (maxV - minV)

        if context.logDebug is True:
            log.debug('[%s] p %s, v %s' % (s, p, v))

        pFactor = context.factorPerformance
        vFactor = context.factorVolatility
       
        if math.isnan(p) or math.isnan(v):
            rank = None 
        else:
            # Adjust volatility for EDV by 50%
            if s.sid == 22887:
                rank = (p * pFactor) + ((v * 0.5) * vFactor)
            else:
                rank = (p * pFactor) + (v * vFactor)
        
        if rank is not None:
            stockRanks[s] = rank

    bestStock = None
    if len(stockRanks) > 0:
        if context.logDebug is True and len(stockRanks) < len(stocks):
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
    s = context.nextStock
    
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
    
    date = get_datetime()
    month = int(date.month)
    day = int(date.day)
    year = int(date.year)
    #dayNum = int(date.strftime("%j"))
    
    fYear = context.lastForecastYear
    fMonth = context.lastForecastMonth
    fDay = context.lastForecastDay
    
    if context.logWarn is True and context.portfolio.cash < 0:
        log.warn('NEGATIVE CASH %s' % context.portfolio.cash)
            
    if context.oidSell is not None:
        orderObj = get_order(context.oidSell)
        if orderObj.filled == orderObj.amount:
            # Good to buy next holding
            if context.logSell is True:
                log.info('SELL ORDER COMPLETED')
            context.oidSell = None
            context.oidBuy = buyPositions(context, data)
            context.currentStock = context.nextStock
            context.nextStock = None
        else:
            if context.logSell is True:
                log.info('SELL ORDER *NOT* COMPLETED')
            return
            
    if context.oidBuy is not None:
        orderObj = get_order(context.oidBuy)
        if orderObj.filled == orderObj.amount:
            if context.logBuy is True:
                log.info('BUY ORDER COMPLETED')
            context.oidBuy = None
        else:
            if context.logBuy is True:
                log.info('BUY ORDER *NOT* COMPLETED')
            return
   
    datapanel = accumulateData(data)
    
    if datapanel is None:
        # There is insufficient data accumulated to process
        if context.logWarn is True:
            log.warn('INSUFFICIENT DATA!')
        return
    
    if not context.dateStart:
        context.dateStart = date
        context.cashStart = context.portfolio.cash
            
    if not context.currentMonth or context.currentMonth != month or (year == fYear and month == fMonth and day == fDay):
        #context.currentDayNum = dayNum
        context.currentMonth = month
    else:
        return
    
    # At this point the stocks need to be ranked.
    
    # Ensure stocks are only traded if possible.  
    # (e.g) EDV doesn't start trading until late 2007, without
    # this, any backtest run before that date would fail.
    stocks = []
    for s in context.basket.values():
        if date > s.security_start_date:
            stocks.append(s)
    
    best = getBestStock(context, datapanel, stocks)
    
    if best is not None:
        if (context.currentStock == best):
            # Hold current
            if context.logHold is True:
                log.info('HOLD [%s]' % context.currentStock)
            return
        elif (context.currentStock is None):
            # Buy best
            context.currentStock = best
            context.nextStock = best
            context.oidBuy = buyPositions(context, data)
        else:
            # Sell ALL and Buy best
            context.nextStock = best
            log.info('BUYING [%s]' % best)
            if hasPositions(context):
                context.oidSell = sellPositions(context)
            else:
                context.oidBuy = buyPositions(context, data)
    else:
        if context.logWarn is True:
            log.warn('COULD NOT FIND A BEST STOCK! BEST STOCK IS *NONE*')
                    
    # NOTE: record() can ONLY handle five elements in the graph. Any more than that will runtime error once 5 are exceeded.      
    record(buy=context.buyCount, sell=context.sellCount, cash=context.portfolio.cash, pnl=context.portfolio.pnl)
    
    if (year == fYear and month == fMonth and day == fDay):
        
        # Calculate Compound Annual Growth Rate (CAGR)
        dateDelta = date - context.dateStart
        inverseYears = 1.0 / (dateDelta.days/365.0)
        performance = (context.portfolio.portfolio_value / context.cashStart)
        cagr = pow(performance, inverseYears) - 1.0
        
        log.info('CAGR %s%%, PNL $%s, CASH $%s, PORTFOLIO $%s' % ((cagr * 100), context.portfolio.pnl, context.portfolio.cash, context.portfolio.portfolio_value))
