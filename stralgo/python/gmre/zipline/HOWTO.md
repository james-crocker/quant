Install Python. (http://www.python.org/)

Install Quantopian Zipline. (https://github.com/quantopian/zipline)

Install Eclipse IDE with PyDev integration. (http://www.eclipse.org/, http://marketplace.eclipse.org/content/pydev-python-ide-eclipse)

Import the existing project from the github source. (https://github.com/james-crocker/quant)
stalgo/python

Open zipline/gmre.py

Change ziplineDataPath = '/home/<userName>/.zipline/data/*' to the location of your data directory. The script will remove previous downloads to assure clean backtests.

Set self.lastForecastYear, self.lastForecastMonth, self.lastForecastDay to the last trading day of the current month for the forecasted BEST stock for investment. This should be less than or equal to the end date.

Change start date, end date, adjusted pricing and stock basket to suite your needs.

Enable/Disable logging verbosity as needed with logWarn, logBuy, etc.

Save any changes.

Then 'Run' gmre.py

