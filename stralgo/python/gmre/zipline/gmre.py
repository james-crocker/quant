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

# TRADING PLATFORM: Quantopian's Zipline

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

from zipline import TradingAlgorithm
from zipline.transforms import batch_transform
from zipline.finance.commission import * 
from zipline.utils.factory import load_bars_from_yahoo
from zipline.finance import trading, performance
from zipline.finance.blotter import Blotter

from datetime import datetime

import pytz
import math
from subprocess import call

ziplineDataPath = '/home/<userName>/.zipline/data/*'
removeCommand = '/bin/rm'

# NOTE: The stocks in the basket must exist in the date range. If not it will error.
# Looking for Zipline code similar to Quantopian "security_start_date"
startDateTime = [2011, 01, 01, 0, 0, 0, 0, pytz.utc]
endDateTime = [2013, 11, 29, 0, 0, 0, 0, pytz.utc]
basket = ['MDY', 'IEV', 'EEM', 'ILF', 'EPP', 'EDV', 'ZIV']
priceAdjusted = True # Load Yahoo Bars with adjusted prices or not

@batch_transform
def accumulateData(data):
    return data
            
class GMRE(TradingAlgorithm):

    def initialize(self):        
        
        # Trade on boundary of first trading day of MONTH or set DAYS
        # DAYS|MONTH
        #self.boundaryTrade = 'DAYS'
        #self.boundaryDays = 21
        self.boundaryTrade = 'MONTH'
    
        # Set the last date for the FORECAST BEST Buy
        self.lastForecastYear = 2013
        self.lastForecastMonth = 11
        self.lastForecastDay = 29
       
        # Set Performance vs. Volatility factors (7.0, 3.0 from Grossman GMRE
        self.factorPerformance = 0.7
        self.factorVolatility = 0.3
      
        # Period Volatility and Performance period in DAYS
        self.metricPeriod = 63 # 3 months LOOKBACK
        self.periodVolatility = 21 # Volatility period. Chose a MULTIPLE of metricPeriod

        # To prevent going 'negative' on cash account set stop, limit and price factor >= stop
        #self.orderBuyLimits = False
        #self.orderSellLimits = False
        ##self.priceBuyStop = None
        ##self.priceBuyLimit = None
        ##self.priceSellStop = None
        ##self.priceSellLimit = None
        #self.priceBuyFactor = 3.03 # Buffering since buys and sells DON'T occur on the same day.
    
        # Re-enact pricing from original Quast code
        self.orderBuyLimits = False
        self.orderSellLimits = False
        self.priceBuyFactor = 0.0
    
        # Factor commission cost
        #self.set_commission(commission.PerShare(cost=0.03))
        #self.set_commission(commission.PerTrade(cost=15.00))
        
        # Set the basket of stocks
        self.basket = {
            12915: 'MDY', # MDY (SPDR S&P MIDCAP 400)
            21769: 'IEV', # IEV (ISHARES EUROPE ETF)
            24705: 'EEM', # EEM (ISHARES MSCI EMERGING MARKETS)
            23134: 'ILF', # ILF (ISHARES LATIN AMERICA 40)
            23118: 'EPP', # EPP (ISHARES MSCI PACIFIC EX JAPAN)
            22887: 'EDV', # EDV (VANGUARD EXTENDED DURATION TREASURY)
            40513: 'ZIV', # ZIV (VelocityShares Inverse VIX Medium-Term)
           #23911: 'SHY', # SHY (iShares 1-3 Year Treasury Bond ETF)
        } 
        
        self.logWarn = False
        self.logBuy = False
        self.logSell = False
        self.logHold = True
        self.logRank = False
        self.logDebug = False
        
        # SHOULDN'T NEED TO MODIFY REMAINING VARIABLES
        
        self.accumulateData = accumulateData(window_length=self.metricPeriod)

        # Keep track of the current period
        self.cashStart = None
        self.dateStart = None
        self.currentDayNum = None
        self.currentMonth = None
        self.currentStock = None
        self.nextStock = None
        self.oidBuy = None
        self.oidSell = None
                
        self.buyCount = 0
        self.sellCount = 0
    
    def getMinMax(self, arr):
        return min(arr.values()), max(arr.values())
    
    def rsVolatility(self, period, openPrices, closePrices, highPrices, lowPrices):
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
        
    def getStockMetrics(self, openPrices, closePrices, highPrices, lowPrices):
        # Get the prices
        
        # Frank GrossmannComments (114) 
        # You can use the 20 day volatility averaged over 3 month.
        # For the ranking I calculate the 3 month performance of all ETF's and normalise between 0-1.
        # The best will have 1. Then I calculate the medium 3 month 20 day volatility and also normalize from 0-1.
        # Then I used Ranking= 0.7*performance +0.3*volatility.
        # This will give me a ranking from 0-1 from which I will take the best.

        period = self.metricPeriod
        periodV = self.periodVolatility
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
            v.append(self.rsVolatility(volDays, openPrices[x:y], closePrices[x:y], highPrices[x:y], lowPrices[x:y]))

            x += 1
        
        volatility = sum(v) / periodRange
        
        return performance, volatility
    
    def getBestStock(self, date, data, stocks):
    
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
            p, v = self.getStockMetrics(data['open'][s], data['close'][s], data['high'][s], data['low'][s])
            performances[s] = p
            volatilities[s] = v
        
        # Determine min/max of each.  NOTE: volatility is switched
        # since a low volatility should be weighted highly.
        minP, maxP = self.getMinMax(performances)
        maxV, minV = self.getMinMax(volatilities)
        
        # Normalize the performance and volatility values to a range
        # between [0..1] then rank them based on a 70/30 weighting.
        stockRanks = {}
        for s in stocks:
            p = (performances[s] - minP) / (maxP - minP)
            v = (volatilities[s] - minV) / (maxV - minV)
            if self.logDebug is True:
                print('[%s] p %s, v %s' % (s, p, v))
            
            pFactor = self.factorPerformance
            vFactor = self.factorVolatility
            
            if math.isnan(p) or math.isnan(v):
                rank = None
            else:
                # Adjust volatility for EDV by 50%
                if s == 'EDV':
                    rank = (p * pFactor) + ((v * 0.5) * vFactor)
                else:
                    rank = (p * pFactor) + (v * vFactor)
                    
            if rank is not None:
                stockRanks[s] = rank
            
        bestStock = None
        if len(stockRanks) > 0:
            if self.logDebug is True and len(stockRanks) < len(stocks):
                    print('%s: FEWER STOCK RANKINGS THAN IN STOCK BASKET!' % date)                    
            if self.logRank is True:
                for s in sorted(stockRanks, key=stockRanks.get, reverse=True):
                    print('%s: RANK [%s] %s' % (date, s, stockRanks[s]))
                    
            bestStock = max(stockRanks, key=stockRanks.get)
        else:
            if self.logDebug is True:
                print('%s: NO STOCK RANKINGS FOUND IN BASKET; BEST STOCK IS: NONE' % date)
            
        return bestStock
    
    def hasPositions(self):
        hasPositions = False
        for p in self.portfolio.positions.values():
            if (p.amount > 0):
                hasPositions = True
                break
                
        return hasPositions        
    
    def sellPositions(self, date):
        oid = None
        positions = self.portfolio.positions
        
        try:
            priceSellStop = self.priceSellStop
        except:
            priceSellStop = 0.0
        
        try:
            priceSellLimit = self.priceSellLimit
        except:
            priceSellLimit = 0.0
        
        for p in positions.values():
            if (p.amount > 0):
                
                amount = p.amount
                price = p.last_sale_price
                orderValue = price * amount
                    
                stop = price - priceSellStop
                limit = stop - priceSellLimit

                if self.orderSellLimits is True:
                    if self.logSell is True:
                        print('%s: SELL [%s] (%s) @ $%s (%s) STOP $%s LIMIT $%s' % (date, p.sid, -amount, price, orderValue, stop, limit))
                    oid = self.order(p.sid, -amount, limit_price = limit, stop_price = stop)
                else:
                    if self.logSell is True:
                        print('%s: SELL [%s] (%s) @ $%s (%s) MARKET' % (date, p.sid, -amount, price, orderValue))
                    oid = self.order(p.sid, -amount)
                
                self.sellCount += 1
            
        return oid

    def buyPositions(self, data, date):
        oid = None
        
        cash = self.portfolio.cash   
        s = self.nextStock        
        
        try:            
            priceBuyFactor = self.priceBuyFactor
        except:
            priceBuyFactor = 0.0
        
        try:
            priceBuyStop = self.priceBuyStop
        except:
            priceBuyStop = 0.0
        
        try:
            priceBuyLimit = self.priceBuyLimit
        except:
            priceBuyLimit = 0.0
        
        price = data[s]['price']            
        amount = math.floor(cash / (price + priceBuyFactor))                      
        orderValue = price * amount
        
        stop = price + priceBuyStop
        limit = stop + priceBuyLimit
        
        #print('%s: BUY Cash $%s' % (date, self.portfolio.cash))
        #print('%s: BUY Positions Value $%s' % (date, self.portfolio.positions_value))  
                
        if cash <= 0 or cash < orderValue:
            print('%s: BUY ABORT! cash $%s < orderValue $%s' % (date, cash, orderValue))
        else:
            if self.logBuy is True:
                if self.orderBuyLimits is True:            
                    print('%s: BUY [%s] %s @ $%s ($%s of $%s) STOP $%s LIMIT $%s' % (date, s, amount, price, orderValue, cash, stop, limit))
                else:
                    print('%s: BUY [%s] %s @ $%s ($%s of $%s) MARKET' % (date, s, amount, price, orderValue, cash))
                
                
            if self.orderBuyLimits is True:
                oid = self.order(s, amount, limit_price = limit, stop_price = stop)
            else:
                oid = self.order(s, amount)
             
            self.buyCount += 1

        return oid    
    
    def handle_data(self, data):               
    
        # Default appears to be 100000.0 to start - trying to find way to set that...
        date = self.get_datetime()
        month = int(date.month)
        day = int(date.day)
        year = int(date.year)
        #dayNum = int(date.strftime("%j"))        
        dateStr = date.strftime('%Y-%m-%d')
        
        fYear = self.lastForecastYear
        fMonth = self.lastForecastMonth
        fDay = self.lastForecastDay
    
        if self.logWarn is True and self.portfolio.cash < 0:
            print('%s: NEGATIVE CASH %s' % (dateStr, self.portfolio.cash))        
        
        if self.oidSell is not None:
            orderObj = self.blotter.orders[self.oidSell]
            if orderObj.filled == orderObj.amount:
                # Good to buy next holding
                if self.logSell is True:
                    print('%s: SELL ORDER COMPLETED' % dateStr)
                self.oidSell = None  
                self.oidBuy = self.buyPositions(data, dateStr)
                self.currentStock = self.nextStock
                self.nextStock = None
            else:
                if self.logSell is True:
                    print('%s: SELL ORDER *NOT* COMPLETED' % dateStr)
                return
            
        if self.oidBuy is not None:
            orderObj = self.blotter.orders[self.oidBuy]
            if orderObj.filled == orderObj.amount:
                if self.logBuy is True:
                    print('%s: BUY ORDER COMPLETED' % dateStr)
                self.oidBuy = None
            else:
                if self.logBuy is True:
                    print('%s: BUY ORDER *NOT* COMPLETED' % dateStr)
                return            

        datapanel = self.accumulateData.handle_data(data)          
        
        if datapanel is None:
            # There is insufficient data accumulated to process
            if self.logWarn is True:
                print('%s: INSUFFICIENT DATA!' % dateStr)
            return
        
        #if int(year) == 2013 and int(month) == 11 and int(day) > 25:
        #print('CurrentDayNum %s, DayNum %s, Year %s, day %s, month %s' % (self.currentDayNum, dayNum, year, day, month))
        #if self.currentDayNum != None:
        #    print('PlusDaynum %s + 29 = %s' % (self.currentDayNum, (self.currentDayNum + 29)))
        
        if not self.dateStart:
            self.dateStart = date
            self.cashStart = self.portfolio.cash
        
        #if not self.currentDayNum or dayNum < self.currentDayNum or (self.currentDayNum + 29) <= dayNum or (year == 2013 and month == 11 and day == 27):
        if not self.currentMonth or self.currentMonth != month or (year == fYear and month == fMonth and day == fDay):
            #self.currentDayNum = dayNum
            self.currentMonth = month
        else:
            return
        
        # At this point the stocks need to be ranked
        
        # Ensure stocks are only traded if possible.  
        # (e.g) EDV doesn't start trading until late 2007, without
        # this, any backtest run before that date would fail.
        stocks = []
        for s in self.basket.values():
            #if date > s.security_start_date:
            stocks.append(s)      
    
        best = self.getBestStock(dateStr, datapanel, stocks)     
        
        if best is not None:
            if (self.currentStock == best):
                # Hold current
                if self.logHold is True:
                    print('%s: HOLD [%s]' % (dateStr, self.currentStock))
                return
            elif (self.currentStock is None):
                # Buy best
                self.currentStock = best
                self.nextStock = best
                self.oidBuy = self.buyPositions(data, dateStr)
            else:
                # Sell ALL and Buy best
                self.nextStock = best
                print('%s: BUYING [%s]' % (dateStr, best))
                if self.hasPositions():
                    self.oidSell = self.sellPositions(dateStr)
                else:
                    self.oidBuy = self.buyPositions(data, dateStr)
        else:
            print('%s: COULD NOT FIND A BEST STOCK! BEST STOCK IS *NONE*' % dateStr)
                        
        # NOTE: record() can ONLY handle five elements in the graph. Any more than that will runtime error once 5 are exceeded.
        self.record(buy=self.buyCount, sell=self.sellCount, cash=self.portfolio.cash, pnl=self.portfolio.pnl)
        
        if (year == fYear and month == fMonth and day == fDay):
            
            # Calculate Compound Annual Growth Rate (CAGR)
            dateDelta = date - self.dateStart
            inverseYears = 1.0 / (dateDelta.days/365.0)
            performance = (self.portfolio.portfolio_value / self.cashStart)
            cagr = pow(performance, inverseYears) - 1.0
            
            print('%s: CAGR %s%%, PNL $%s, CASH $%s, PORTFOLIO $%s' % (dateStr, (cagr * 100), self.portfolio.pnl, self.portfolio.cash, self.portfolio.portfolio_value))

        ## END OF CLASS GMRE

# Remove previous Yahoo download content to assure clean backtest        
cmd = removeCommand + ' ' + ziplineDataPath
returnCode = call(cmd, shell=True)
#if returnCode != 0:
#    print("Couldn't %s :: %s" % (cmd, returnCode))
#    sys.exit()

start = datetime(*startDateTime)
end = datetime(*endDateTime)

data = load_bars_from_yahoo(stocks=basket, indexes={}, start=start, end=end, adjusted=priceAdjusted)
    
gmre = GMRE()
perf = gmre.run(data)