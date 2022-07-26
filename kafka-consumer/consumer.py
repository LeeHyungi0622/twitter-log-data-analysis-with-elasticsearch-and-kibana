from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
# consumer -> ELK docker container로 연결
import logging
from logging.config import fileConfig

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

if __name__ == '__main__':
    #Topic
    topic_name = "twitter"
    # Get consumer object from getConsumerObj method
    consumer = getConsumerObj()

    for message in consumer:
        print ("[consumer] %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value.decode('utf-8')))