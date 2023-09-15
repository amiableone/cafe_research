import pandas as pd
import time

s = time.perf_counter()

store = pd.HDFStore('store.h5')

if '/Combined' not in store.keys():
    dfs = []

    for key in store.keys():
        if key != '/nasdaq_symbols':
            dfs.append(store[key])

    combined = pd.concat(dfs, axis=1).sort_index()
    combined = combined.dropna(axis=1, how='all')
    store.put('Combined', combined)

elif '/Combined_adjclose' not in store.keys():
    combined = store['Combined']
    adjclose = combined[[(t, p) for t, p in combined.columns if p == 'Adj Close']]
    store.put('Combined_adjclose', adjclose)

print(adjclose)

e = time.perf_counter() - s
print(e)

store.close()