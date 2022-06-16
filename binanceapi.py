import asyncio
import aiohttp
import os
import pandas as pd
import boto3
from datetime import date, datetime
from io import StringIO
from io import BytesIO
import time
import requests

url = 'https://api.binance.com/api/v3/klines'
ACCESS_KEY = {your_key}
SECRET_KEY = {your_secret}

columns = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore']
results = []

def get_all_symbols():
    symbols_list = []
    symbols_result = requests.get(url="https://api.binance.com/api/v1/exchangeInfo")
    for node in symbols_result.json()['symbols']:
        symbols_list.append(node['symbol'])
    return symbols_list
    
    return {"State": "Success", "lista":lista}

def get_tasks(session, par_intervals, par_limit, par_starttime, par_endtime, par_symbol):
    tasks = []
    for par_interval in par_intervals:
        tasks.append(session.get('{}?interval={}{}{}{}&symbol={}'.format(url, par_interval, par_limit, par_starttime, par_endtime, par_symbol), ssl=False))
    return tasks

async def get_symbols(par_intervals, par_limit, par_starttime, par_endtime, par_symbols):
    async with aiohttp.ClientSession() as session:
        for par_symbol in par_symbols:
            tasks = get_tasks(session, par_intervals, par_limit, par_starttime, par_endtime, par_symbol)
        response = await asyncio.gather(*tasks)
        for result in response:
            json_response = await result.json()
        return json_response
    
def lambda_handler(event, context):
    t1 = time.time()
    #####Get the current Enriched file
    key = 'Enriched/binance.csv'
    bucket_name = 'binance-hist' # already created on S3
    s3_resource = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    objs = list(s3_resource.Bucket(bucket_name).objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        obj = s3_resource.Object(bucket_name, key)
        with BytesIO(obj.get()['Body'].read()) as bio:
            df_old = pd.read_csv(bio)
            last_startdate = df_old.max(axis=1)[0] if (df_old.count() > 1).all() else datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S').timestamp()
    else:
        last_startdate = int(datetime.strptime(datetime.now().strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d 00:00:00').timestamp())*1000
        df_old = None
    
    ###Set parameters
    interval = event["interval"] if event["interval"] != "" else "[1m,5m,15m,30m,1h,4h,1d]"
    limit= event["limit"] if event["limit"] != "" else 10
    endtime= event["endtime"] if event["endtime"] != "" else datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    starttime= event["starttime"]
    symbol= event["symbol"] if event["symbol"] != "" else 'ALL'
    
    par_intervals = interval.replace(" ", "").replace("[", "").replace("]", "").split(',') if isinstance(interval, str) else interval
    par_limit= '&limit={}'.format(limit)
    par_endtime= '&endTime={}'.format(str(int(endtime.timestamp())*1000))
    par_starttime= '&startTime={}'.format(int(datetime.strptime(starttime, '%Y-%m-%d %H:%M:%S').timestamp())*1000 if event['starttime'] != "" else last_startdate)
    
    if symbol == 'ALL':
        par_symbols = get_all_symbols()
    elif isinstance(symbol, str):
        par_symbols = symbol.replace(" ", "").replace("[", "").replace("]", "").split(',')
    else:
        par_symbols = symbol

    
#    ######Get the Binance API
    result = asyncio.run(get_symbols(par_intervals, par_limit, par_starttime, par_endtime, par_symbols))
    
    df = pd.DataFrame(data= result, columns=columns)

    #####Store Raw file
    records = df.Open_time.count()
    if records > 0:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(bucket_name, 'Raw/binance_{}_{}_{}_{}_{}_{}.csv'.format(date.today(), symbol, interval, limit, str(starttime), str(endtime))).put(Body=csv_buffer.getvalue())
    
        ########Store on Enriched zone
        if isinstance(df_old, pd.DataFrame):
            merged = pd.concat([df_old, df])
            merged = merged.drop_duplicates(['Open_time'], keep=False)#####Keep=False will remove all duplications
        else:
            merged = df
    
        csv_buffer = StringIO()
        merged.to_csv(csv_buffer, index=False)
    
        s3_resource.Object(bucket_name, key).put(Body=csv_buffer.getvalue())
    
    return {"State": "Success", "records":str(records), "duration":time.time()-t1}