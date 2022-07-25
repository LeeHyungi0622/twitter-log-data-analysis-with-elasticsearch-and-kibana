# -*- coding: utf-8 -*- 

from dotenv import load_dotenv
import os
from tweepy import Stream
from kafka import KafkaProducer
import json
from time import sleep

# load .env
load_dotenv()

#Twitter API Key
access_token = os.environ.get("access_token")
access_token_secret = os.environ.get("access_token_secret")
api_key = os.environ.get("api_key")
api_secret = os.environ.get("api_secret")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9091']
)

#Topic
topic_name = "__confluent.support.metrics"

# https://stackoverflow.com/questions/69338089/cant-import-streamlistener
class StdOutListener(Stream):
    def on_data(self, data):
        raw_data = json.loads(data)
        print('log data ::', raw_data['text'])
        try:
            producer.send(topic_name, raw_data['text'])
            sleep(0.5)
        except Exception as e:
            print(e)
        # producer.send(topic_name, raw_data['text'].encode('utf-8'))
        return True

    def on_error(self, status):
        print(status)

twitter_stream = StdOutListener(api_key, api_secret, access_token, access_token_secret)
twitter_stream.filter(track=["house"])