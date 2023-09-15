import pandas as pd
import time

"""
Does growth of a stock price by 10/20/30% or more without correcting for at least 1/2/5% indicate future decrease?
Does drop in a stock price by 10/20/30% or more without correcting for at least 1/2/5% indicate future increase?

For future research, replace fixed correction amounts with fixed fractions of moving standard deviations
"""

store = pd.HDFStore('store.h5')
df = store['Combined_adjclose']
df = df.reorder_levels([1, 0], axis=1)['Adj Close']

s = time.perf_counter()

change = df.ffill().pct_change()
cum_change = (change + 1).cumprod() - 1

# Let's enumerate continuous upward and downward changes in prices without major corrections
# I will first create marks that equal the most recent price high (low) when the price goes up (down),
# and change between these after price goes in the opposite direction at least certain amount of points
# Here's the algorithm
# 

marks

e = time.perf_counter() - s
print(e)
store.close()