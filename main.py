import logging
import time
from datetime import datetime, timedelta
import json
import pytz
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
import pandas as pd

async def get_coinbase_ohlc(endtime, cols, symbol=None, granularity=3600, interval="1m"):
    try:
        end = datetime.fromtimestamp(endtime, tz=pytz.utc) 
        start = end - timedelta(seconds=(granularity * 200)) 
        url = "https://api.pro.coinbase.com/products/ETH-USD/candles?start={start}&end={end}&granularity={gran}".format(
            start=start.strftime("%Y-%m-%d %H:%M:%S"),
            end=end.strftime("%Y-%m-%d %H:%M:%S"),
            gran=granularity
        )
        print(url)
        resp = requests.get(url)
        df = pd.DataFrame(resp.json(), columns=cols)
        next_endtime=resp.json()[-1][0]
        return df, next_endtime
    except Exception as e:
        logging.error(e)
        time.sleep(10)
        return get_chart(
            symbol,
            endtime,
            cols,
            granularity,
            interval
        )

async def get_bitstamp_ohlc(endtime, symbol=None, limit=100, granularity=3600):
    try:
        browser = await launch()
        page = await browser.newPage()
        end = endtime
        start = endtime - timedelta(minutes=(limit*granularity)) 
        url = "https://www.bitstamp.net/ajax/tradeview/price-history/?step={granularity}&currency_pair=LTC%2FUSD&start_datetime={start}&end_datetime={end}".format(
            start=start.strftime("%Y-%m-%dT%H:%M:%S"),
            end=end.strftime("%Y-%m-%dT%H:%M:%S"),
            granularity=granularity
        )
        print(url)
        await page.goto(url)
        content = await page.content()
        soup = BeautifulSoup(content)
        data = json.loads(soup.get_text())['data']
        df = pd.DataFrame(data=data)
        df.set_index('time', inplace=True)
        df.sort_index(inplace=True)
        for cl in df.columns:
            df[cl]=pd.to_numeric(df[cl])
        next_endtime = datetime.strptime(df.index[0], "%Y-%m-%dT%H:%M:%S")
        await browser.close()
        
        return df, next_endtime
    except Exception as e:
        logging.error(e)
        time.sleep(2)
        return await get_chart(
            endtime=endtime,
            symbol=symbol,
            limit=limit,
            granularity=granularity
        )

async def get_blockchain_chart():
    for c in charts:
        url = "https://api.blockchain.info/charts/{chart}?timespan=all&format=json".format(chart=c)
        res = requests.get(url)
        df = pd.DataFrame(res.json()['values'])
        df.rename(columns={'x':'time','y':c}, inplace=True)
        df.set_index('time', inplace=True)
        dfs.append(df)
        print(c)
        print(len(dfs))
        time.sleep(5)

async def get_bitmex_ohlc():
    pass

async def get_bitmex_funding():
    pass

async def main():
    pass