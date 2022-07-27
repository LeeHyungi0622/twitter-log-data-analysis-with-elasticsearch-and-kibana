from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
# consumer -> ELK docker container로 연결
import socket
import json
import sys


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

def sendMessageToLogstash():
    HOST = "192.168.200.169"
    PORT = 50000

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as msg:
        sys.stderr.write("[ERROR] %s\n" % msg[1])
        sys.exit(1)

    try:
        sock.connect((HOST, PORT))
    except socket.error as msg:
        sys.stderr.write("[ERROR] %s\n" % msg[1])
        sys.exit(2)

    msg = {"@message": "python test message from hyungi", "@tags": "python"}

    json_obj = json.dumps(msg)

    message = bytes(json_obj, 'utf-8')

    sock.sendall(message)

    sock.close()
    sys.exit(0)

if __name__ == '__main__':
    #Topic
    topic_name = "twitter"
    # Get consumer object from getConsumerObj method
    consumer = getConsumerObj()

    
    for message in consumer:
        print ("[consumer] %s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value.decode('utf-8')))

    sendMessageToLogstash()