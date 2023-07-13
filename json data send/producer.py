from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import msgpack  
import logging
# producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],value_serializer=msgpack.dumps)
producer.send('my_topic', {'key': 'value'})
producer.flush()
producer.close()
