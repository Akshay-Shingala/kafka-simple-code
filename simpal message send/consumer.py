from kafka import KafkaConsumer
import json
import msgpack
# To consume latest messages and auto-commit offsets
# give the topic name a unique and give the server port by default i will be :9092
consumer = KafkaConsumer('my_topic', bootstrap_servers=['127.0.0.1:9092'], group_id='my-group')


#get data from consumer this loop executes continuously
for message in consumer:
    try:
        # print data from consumer message data get in message.value
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    except Exception as e:
        print(e,"except")
 
