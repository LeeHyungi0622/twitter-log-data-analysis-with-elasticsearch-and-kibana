from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
# consumer -> ELK docker container로 연결
import socket
import json
import sys
import ast
from bs4 import BeautifulSoup

# load .env
load_dotenv()

def getConsumerObj():
    consumerObj = KafkaConsumer(
    topic_name,
    bootstrap_servers = ["{}:9091".format(os.environ.get("bootstrap_servers_ip"))],
    auto_offset_reset='earliest',
    group_id = 'group1',
    enable_auto_commit = True,
    consumer_timeout_ms = 10000
    )

    return consumerObj

def sendMessageToLogstash(message):
    HOST = "192.168.200.169"
    PORT = 50000

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as msg:
        sys.stderr.write("[ERROR] %s\n" % msg[1])

    try:
        sock.connect((HOST, PORT))
    except socket.error as msg:
        sys.stderr.write("[ERROR] %s\n" % msg[1])
    
    msg = message.value.decode("utf-8")

    message_obj = ast.literal_eval(msg)

    source = BeautifulSoup(message_obj.get('source'), "html.parser") 
    
    msg = {
        "source": [s for s in source.strings], 
        "lan": message_obj.get('lang'),
        "location": message_obj.get('user').get('location'),
        "hash_tags": [h for h in message_obj.get('entities').get('hashtags')],
        "text": message_obj.get('text'), 
        "description": message_obj.get('description'),
        "reply_count": message_obj.get('reply_count'), 
        "retweet_count": message_obj.get('retweet_count'),
        "followers_count": message_obj.get('followers_count'),
        "favorite_count": message_obj.get('favorite_count')
    }
    
    json_obj = json.dumps(msg)
    message = bytes(json_obj, 'utf-8')
    sock.sendall(message)

if __name__ == '__main__':
    #Topic
    topic_name = "twitter"
    # Get consumer object from getConsumerObj method
    consumer = getConsumerObj()

    for message in consumer:
        # print('message :::', message)
        sendMessageToLogstash(message)
    
    # for message in consumer:
    #     print ("[consumer] %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                         message.offset, message.key,
    #                                         message.value.decode('utf-8')))
