# -*- coding: utf-8 -*- 

from dotenv import load_dotenv
import os
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

#Twitter API Key
access_token = os.environ.get("access_token")
access_token_secret = os.environ.get("access_token_secret")
api_key = os.environ.get("api_key")
api_secret = os.environ.get("api_secret")

#Topic
topic_name = "twitter_topic"

# https://stackoverflow.com/questions/69338089/cant-import-streamlistener
class StdOutListener(Stream):
    def on_data(self, data):
        raw_data = json.loads(data)
        producer.send(topic_name, raw_data["text"].encode('utf-8'))
        return True

    def on_error(self, status):
        print(status)

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
outlisten = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, outlisten)
stream.filter(track=["곰"])