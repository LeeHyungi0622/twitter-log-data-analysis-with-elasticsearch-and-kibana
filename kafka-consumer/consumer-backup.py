from dotenv import load_dotenv
import os
from kafka import KafkaConsumer

# load .env
load_dotenv()

#Topic
topic_name = "twitter"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers = ["{}:9091".format(os.environ.get("bootstrap_servers_ip"))],
    auto_offset_reset='earliest',
    group_id = 'group1',
    enable_auto_commit = True,
    consumer_timeout_ms = 10000
)

for message in consumer:
    print ("[consumer] %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode('utf-8')))
