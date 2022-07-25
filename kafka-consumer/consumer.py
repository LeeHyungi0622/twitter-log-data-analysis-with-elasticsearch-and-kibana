from json import loads
from kafka import KafkaConsumer
from time import sleep

#Topic
topic_name = "twitter"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers = ['localhost:9091'],
    auto_offset_reset='earliest',
    group_id = 'group1',
    enable_auto_commit = True,
    consumer_timeout_ms = 5000
)

for message in consumer:
    print(message)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
