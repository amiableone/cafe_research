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

# change = df.ffill().pct_change()
# cum_change = (change + 1).cumprod() - 1

# Let's enumerate continuous upward and downward changes in prices without major corrections
# I will first create marks that equal the most recent price high (low) when the price goes up (down),
# and change between these after price goes in the opposite direction at least certain amount of points

def assign_directions(hist, offset=5):
    md = []    # List of tuples of previous marks and directions
    for i, p in enumerate(hist):
        prev_p = hist.shift().iloc[i]
        preprev_p = hist.shift(2).iloc[i]
        if len(md) > 0:
            m, d = md[-1]
        if p != p or p is None:
            md.append((None, None))
        elif prev_p != prev_p or prev_p is None:
            md.append((p, None))
        elif p > prev_p:
            if preprev_p != preprev_p or preprev_p is None or d is None:
                md.append((p, 1))
            elif d % 2 == 1:
                md.append((max(p, m), d))
            elif (p - m) / m * 100 > offset:
                md.append((p, d + 1))
            else:
                md.append((m, d))
        elif p < prev_p:
            if preprev_p != preprev_p or preprev_p is None or d is None:
                md.append((p, 2))
            elif d % 2 == 0:
                md.append((min(p, m), d))
            elif (p - m) / m * 100 < -offset:
                md.append((p, d + 1))
            else:
                md.append((m, d))
        else:
            md.append((m, d))
    return pd.DataFrame(md, columns=['mark', 'dir'], index=hist.index)

pd.set_option('display.max_rows', None)
acro = df[['ACRO']]
acro = pd.concat([acro, assign_directions(acro['ACRO'])], axis=1)
print(acro.tail(600))

e = time.perf_counter() - s
print(e)
store.close()