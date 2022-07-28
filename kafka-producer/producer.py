# -*- coding: utf-8 -*- 

from dotenv import load_dotenv
import os
from tweepy import Stream
from kafka import KafkaProducer
from json import loads
from time import sleep

# load .env
load_dotenv()

#Twitter API Key
access_token = os.environ.get("access_token")
access_token_secret = os.environ.get("access_token_secret")
api_key = os.environ.get("api_key")
api_secret = os.environ.get("api_secret")

producer = KafkaProducer(
    bootstrap_servers=["{}:9091".format(os.environ.get("bootstrap_servers_ip"))]
)

#Topic
topic_name = "twitter"

class StdOutListener(Stream):

    def on_data(self, data):
        raw_data = loads(data)
        print('raw_data ::', raw_data)
        try:
            producer.send(topic_name, bytes(str(raw_data), 'utf-8'))
            sleep(0.5)
        except Exception as e:
            print(e)

twitter_stream = StdOutListener(api_key, api_secret, access_token, access_token_secret)
twitter_stream.filter(track=["ukraine", "russia", "war"])