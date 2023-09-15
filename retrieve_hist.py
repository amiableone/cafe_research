import yfinance as yf
import pandas as pd
import asyncio
import logging

store = pd.HDFStore('store.h5')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Let the keys for multiple tickers be strings of tickers delimeted by underscores

nasdaq_symbols = store['nasdaq_symbols']
symbols = nasdaq_symbols[nasdaq_symbols['Share mentioned']]['Symbol'].to_list()
slices = [symbols[i:i+30] for i in range(0, len(symbols), 30)]

async def retrieve_hist_prices(symbols, key, i):
            df = yf.download(symbols)
            logger.info('Retrieved batch %d', i)
            store.put(key, df)
            if f'/{key}' in store.keys():
                logger.info('Stored batch %d successfully', i)
            else:
                logger.error('Batch %d not stored', i)

async def main():
    try:
        tasks = []
        for i, batch in enumerate(slices):
            batch = [item for item in batch if isinstance(item, str)]
            key = '_'.join(batch)
            if f'/{key}' not in store.keys():
                tasks.append(asyncio.create_task(retrieve_hist_prices(batch, key, i + 1)))
                logger.info('Created task for batch %d out of %d', i + 1, len(slices))
                await asyncio.sleep(30 * (i % 3 == 2))
            else:
                logger.info(f'Batch %d already in store', i + 1)
        await asyncio.gather(*tasks)

    except Exception as exc:
        store.close()
        logger.exception("Exception in main: %s", exc)

if __name__ == '__main__':
    asyncio.run(main())