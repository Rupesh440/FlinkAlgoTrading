import json
from datetime import datetime
from typing import List

import pytz
from kafka.errors import KafkaTimeoutError
from logzero import logger
import pyotp
from kafka import KafkaProducer
from angel_config.keys import config
from SmartApi.smartConnect import SmartConnect
import pandas as pd


# Function to serialize a row to JSON
'''def serialize_row(row):
    row_dict = row.to_dict()
    row_dict['timestamp'] = datetime.fromisoformat(row_dict['timestamp'])
    print("Parsed timestamp:", row_dict['timestamp'])
    row_dict['timestamp'] = row_dict['timestamp'].astimezone(pytz.UTC)
    print("Converted to UTC:", row_dict['timestamp'])
    #row_dict['timestamp'] = str(row_dict['timestamp'])  # Explicitly convert timestamp to string
    return json.dumps(row_dict)'''


def get_producer(brokers: List[str]):
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks='all',
        batch_size=16384,
        linger_ms=5,
        buffer_memory=33554432,
        max_block_ms=60000
    )
    return producer

def produce_trading_data(
        redpanda_client: KafkaProducer,
        topic: str,
        start_date:str,
        end_date:str,
        symboltoken: str,
        exchange: str,
        interval:str
    ):
    key_id = config["key_id"]
    secret_key = config["secret_key"]
    base_url = config['base_url']
    username = config["username"]
    pwd = config["pwd"]

    api = SmartConnect(key_id,secret_key)
    try:
        token = config["totp"]
        totp = pyotp.TOTP(token).now()
    except Exception as e:
        logger.error("Invalid Token: The provided token is not valid.")
        raise e

    correlation_id = "abcde"
    data = api.generateSession(username, pwd, totp)
    # print(data)

    #start_date = datetime.strptime(start_date,'yyyy-MM-dd hh:mm')
    #end_date = datetime.strptime(end_date, 'yyyy-MM-dd hh:mm')


    historicParam = {
        "exchange": exchange,
        "symboltoken": symboltoken,
        "interval": interval,
        "fromdate": start_date,
        "todate": end_date
    }

    prices_df = api.getCandleData(historicParam)
    #print(prices_df)
    prices_df = pd.DataFrame(prices_df['data'])
    #print(prices_df.head())
    prices_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    prices_df.insert(0, 'symboltoken', symboltoken)
    # Convert the 'timestamp' column to datetime objects and set the timezone
    prices_df['timestamp'] = pd.to_datetime(prices_df['timestamp'])
    # Convert the timezone to UTC
    prices_df['timestamp'] = prices_df['timestamp'].dt.tz_convert('UTC')
    #print("Converted to UTC:", prices_df['timestamp'])
    print(prices_df.head())
    #print(prices_df.dtypes)

    records = json.loads(prices_df.to_json(orient="records"))
    #print(records)
    for idx,record in enumerate(records):
        record["provider"] = 'angelone'

        try:
            future = redpanda_client.send(
                key=record['symboltoken'],
                topic=topic,
                value=record,
                timestamp_ms=record['timestamp']
            )

            _ = future.get(timeout=10)
            print(f'Record sent sucessfully')
        except KafkaTimeoutError as e:
            print(f'Error sending message for symbol {record["symboltoken"]}: KafkaTimeoutError - {e}')
        except TypeError as e:
            print(f'Error sending message for symbol {record["symboltoken"]}: TypeError - {e}')

#print(prices_df)

if __name__ == '__main__':
    redpanda_client = get_producer(config['redpanda_brokers'])
    produce_trading_data(
        redpanda_client,
        topic= 'trading_data',
        start_date= '2023-06-01 12:00',
        end_date= '2024-06-01 12:00',
        symboltoken='3045',
        exchange = 'NSE',
        interval='ONE_MINUTE'
    )

    redpanda_client.close()