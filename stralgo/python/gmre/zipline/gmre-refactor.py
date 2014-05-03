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

from collections import defaultdict
from datetime import datetime, timedelta
from subprocess import call
from zipline import TradingAlgorithm
from zipline.transforms import batch_transform
from zipline.finance.commission import * 
from zipline.utils.factory import load_bars_from_yahoo
from zipline.finance import trading, performance
from zipline.finance.blotter import Blotter

import datetime as dt
import math
import pytz

#zipline_data_path = '/home/<userName>/.zipline/data/*'
zipline_data_path = '/home/j13/.zipline/data/*'
command_remove = '/bin/rm'

# NOTE: The stocks in the basket must exist in the date range. If not it will error.
# Looking for Zipline code similar to Quantopian "security_start_date"
backtest_start_datetime = [2011, 01, 01, 0, 0, 0, 0, pytz.utc]
backtest_end_datetime = [2013, 11, 29, 0, 0, 0, 0, pytz.utc]
basket = ['MDY', 'IEV', 'EEM', 'ILF', 'EPP', 'EDV', 'ZIV']
priceAdjusted = True # Load Yahoo Bars with adjusted prices or not

@batch_transform
def accumulate_data(data):
    return data
            
def tree(): return defaultdict(tree)

class GMRE(TradingAlgorithm):

    def initialize(self):                    
        # Set Performance vs. Volatility factors (7.0, 3.0 from Grossman GMRE
        self.factor_performance = 0.7
        self.factor_volatility = 0.3
      
        # Period Volatility and Performance period in DAYS
        self.metric_period = 63 # 3 months LOOKBACK
        self.metric_period_volatility = 21 # Volatility period. Chose a MULTIPLE of metric_period

        # To prevent going 'negative' on cash account set stop, limit and price factor >= stop
        #self.order_limits_buy = False
        #self.order_limits_sell = False
        ##self.price_stop_buy = None
        ##self.price_limit_buy = None
        ##self.price_stop_sell = None
        ##self.price_limit_sell = None
        #self.price_factor_buy = 3.03 # Buffering since buys and sells DON'T occur on the same day.
    
        # Re-enact pricing from original Quast code
        self.order_limits_buy = False
        self.order_limits_sell = False
        self.price_factor_buy = 0.0
    
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
        
        self.log_warn = False
        self.log_buy = False
        self.log_sell = False
        self.log_hold = True
        self.log_rank = False
        self.log_debug = False
        
        # SHOULDN'T NEED TO MODIFY REMAINING VARIABLES
        
        self.accumulate_data = accumulate_data(window_length=self.metric_period)

        # Keep track of the current period
        self.begin_cash = None
        self.begin_date = None
        self.current_month = None
        self.current_stock = None
        self.stock_best = None
        self.oid_buy = None
        self.oid_sell = None
                
        self.count_buy = 0
        self.count_sell = 0
    
    def get_min_max(self, arr):
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
        
    def get_basket_period_metrics(self, date, data, basket):
        # Get the prices
        
        # Frank GrossmannComments (114) 
        # You can use the 20 day volatility averaged over 3 month.
        # For the ranking I calculate the 3 month performance of all ETF's and normalise between 0-1.
        # The best will have 1. Then I calculate the medium 3 month 20 day volatility and also normalize from 0-1.
        # Then I used Ranking= 0.7*performance +0.3*volatility.
        # This will give me a ranking from 0-1 from which I will take the best.

        period = self.metric_period
        periodV = self.metric_period_volatility
        periodRange = period / periodV

        metrics = tree()
        
        for s in basket:
            # Calculate the period performance
            priceBegin = closePrices[-period] # First item
            priceEnd = closePrices[-1] # Last item
    
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
    
        performances = tree()
        volatilities = tree()
            
        # Get performance and volatility for all the stocks            
        performances, volatilities = self.get_basket_period_metrics(date, data, stocks)
        
        # Determine min/max of each.  NOTE: volatility is switched
        # since a low volatility should be weighted highly.
        minP, maxP = self.get_min_max(performances)
        maxV, minV = self.get_min_max(volatilities)
        
        # Normalize the performance and volatility values to a range
        # between [0..1] then rank them based on a 70/30 weighting.
        stockRanks = {}
        for s in stocks:
            p = (performances[s] - minP) / (maxP - minP)
            v = (volatilities[s] - minV) / (maxV - minV)
            if self.log_debug is True:
                print('[%s] p %s, v %s' % (s, p, v))
            
            pFactor = self.factor_performance
            vFactor = self.factor_volatility
            
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
            if self.log_debug is True and len(stockRanks) < len(stocks):
                    print('%s: FEWER STOCK RANKINGS THAN IN STOCK BASKET!' % date)                    
            if self.log_rank is True:
                for s in sorted(stockRanks, key=stockRanks.get, reverse=True):
                    print('%s: RANK [%s] %s' % (date, s, stockRanks[s]))
                    
            bestStock = max(stockRanks, key=stockRanks.get)
        else:
            if self.log_debug is True:
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
            price_stop_sell = self.price_stop_sell
        except:
            price_stop_sell = 0.0
        
        try:
            price_limit_sell = self.price_limit_sell
        except:
            price_limit_sell = 0.0
        
        for p in positions.values():
            if (p.amount > 0):
                
                amount = p.amount
                price = p.last_sale_price
                orderValue = price * amount
                    
                stop = price - price_stop_sell
                limit = stop - price_limit_sell

                if self.order_limits_sell is True:
                    if self.log_sell is True:
                        print('%s: SELL [%s] (%s) @ $%s (%s) STOP $%s LIMIT $%s' % (date, p.sid, -amount, price, orderValue, stop, limit))
                    oid = self.order(p.sid, -amount, limit_price = limit, stop_price = stop)
                else:
                    if self.log_sell is True:
                        print('%s: SELL [%s] (%s) @ $%s (%s) MARKET' % (date, p.sid, -amount, price, orderValue))
                    oid = self.order(p.sid, -amount)
                
                self.count_sell += 1
            
        return oid

    def buyPositions(self, data, date):
        oid = None
        
        cash = self.portfolio.cash   
        s = self.stock_best        
        
        try:            
            price_factor_buy = self.price_factor_buy
        except:
            price_factor_buy = 0.0
        
        try:
            price_stop_buy = self.price_stop_buy
        except:
            price_stop_buy = 0.0
        
        try:
            price_limit_buy = self.price_limit_buy
        except:
            price_limit_buy = 0.0
        
        price = data[s]['price']            
        amount = math.floor(cash / (price + price_factor_buy))                      
        orderValue = price * amount
        
        stop = price + price_stop_buy
        limit = stop + price_limit_buy
        
        #print('%s: BUY Cash $%s' % (date, self.portfolio.cash))
        #print('%s: BUY Positions Value $%s' % (date, self.portfolio.positions_value))  
                
        if cash <= 0 or cash < orderValue:
            print('%s: BUY ABORT! cash $%s < orderValue $%s' % (date, cash, orderValue))
        else:
            if self.log_buy is True:
                if self.order_limits_buy is True:            
                    print('%s: BUY [%s] %s @ $%s ($%s of $%s) STOP $%s LIMIT $%s' % (date, s, amount, price, orderValue, cash, stop, limit))
                else:
                    print('%s: BUY [%s] %s @ $%s ($%s of $%s) MARKET' % (date, s, amount, price, orderValue, cash))
                
                
            if self.order_limits_buy is True:
                oid = self.order(s, amount, limit_price = limit, stop_price = stop)
            else:
                oid = self.order(s, amount)
             
            self.count_buy += 1

        return oid    
    
    def handle_data(self, data):               
    
        # Default appears to be 100000.0 to start - trying to find way to set that...        
        now = self.get_datetime() # Zipline/Yahoo data is aligned to TIME 00:00:00 UTC
        month = int(now.month)    
        dateNowStr = now.strftime('%Y-%m-%d')        
    
        if self.log_warn is True and self.portfolio.cash < 0:
            print('%s: NEGATIVE CASH %s' % (dateNowStr, self.portfolio.cash))        
        
        if self.oid_sell is not None:
            orderObj = self.blotter.orders[self.oid_sell]
            if orderObj.filled == orderObj.amount:
                # Good to buy next holding
                if self.log_sell is True:
                    print('%s: SELL ORDER COMPLETED' % dateNowStr)
                self.oid_sell = None  
                self.oid_buy = self.buyPositions(data, dateNowStr)
                self.current_stock = self.stock_best
                self.stock_best = None
            else:
                if self.log_sell is True:
                    print('%s: SELL ORDER *NOT* COMPLETED' % dateNowStr)
                return
            
        if self.oid_buy is not None:
            orderObj = self.blotter.orders[self.oid_buy]
            if orderObj.filled == orderObj.amount:
                if self.log_buy is True:
                    print('%s: BUY ORDER COMPLETED' % dateNowStr)
                self.oid_buy = None
            else:
                if self.log_buy is True:
                    print('%s: BUY ORDER *NOT* COMPLETED' % dateNowStr)
                return            

        datapanel = self.accumulate_data.handle_data(data)          
        
        if datapanel is None:
            # There is insufficient data accumulated to process
            if self.log_warn is True:
                print('%s: INSUFFICIENT DATA!' % dateNowStr)
            return
        
        #if int(year) == 2013 and int(month) == 11 and int(day) > 25:
        #print('CurrentDayNum %s, DayNum %s, Year %s, day %s, month %s' % (self.currentDayNum, dayNum, year, day, month))
        #if self.currentDayNum != None:
        #    print('PlusDaynum %s + 29 = %s' % (self.currentDayNum, (self.currentDayNum + 29)))
        
        if not self.begin_date:
            self.begin_date = now
            self.begin_cash = self.portfolio.cash
        
        #if not self.currentDayNum or dayNum < self.currentDayNum or (self.currentDayNum + 29) <= dayNum or (year == 2013 and month == 11 and day == 27):
        if not self.current_month or self.current_month != month or now == self.dateLastBacktest:
            #self.currentDayNum = dayNum
            self.current_month = month
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
    
        best = self.getBestStock(dateNowStr, datapanel, stocks)     
        
        if best is not None:
            if (self.current_stock == best):
                # Hold current
                if self.log_hold is True:
                    print('%s: HOLD [%s]' % (dateNowStr, self.current_stock))
                return
            elif (self.current_stock is None):
                # Buy best
                self.current_stock = best
                self.stock_best = best
                self.oid_buy = self.buyPositions(data, dateNowStr)
            else:
                # Sell ALL and Buy best
                self.stock_best = best
                print('%s: BUYING [%s]' % (dateNowStr, best))
                if self.hasPositions():
                    self.oid_sell = self.sellPositions(dateNowStr)
                else:
                    self.oid_buy = self.buyPositions(data, dateNowStr)
        else:
            print('%s: COULD NOT FIND A BEST STOCK! BEST STOCK IS *NONE*' % dateNowStr)
                        
        # NOTE: record() can ONLY handle five elements in the graph. Any more than that will runtime error once 5 are exceeded.
        self.record(buy=self.count_buy, sell=self.count_sell, cash=self.portfolio.cash, pnl=self.portfolio.pnl)
        
        if (now == self.dateLastBacktest):
            
            # Calculate Compound Annual Growth Rate (CAGR)
            dateDelta = now - self.begin_date
            inverseYears = 1.0 / (dateDelta.days/365.0)
            performance = (self.portfolio.portfolio_value / self.begin_cash)
            cagr = pow(performance, inverseYears) - 1.0
            
            print('%s: CAGR %s%%, PNL $%s, CASH $%s, PORTFOLIO $%s' % (dateNowStr, (cagr * 100), self.portfolio.pnl, self.portfolio.cash, self.portfolio.portfolio_value))

        ## END OF CLASS GMRE

# Remove previous Yahoo download content to assure clean backtest        
cmd = command_remove + ' ' + zipline_data_path
returnCode = call(cmd, shell=True)
#if returnCode != 0:
#    print("Couldn't %s :: %s" % (cmd, returnCode))
#    sys.exit()

start = datetime(*backtest_start_datetime)
end = datetime(*backtest_end_datetime)

data = load_bars_from_yahoo(stocks=basket, indexes={}, start=start, end=end, adjusted=priceAdjusted)
    
gmre = GMRE()
perf = gmre.run(data)