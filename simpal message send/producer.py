from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import msgpack  
import logging
# Configure the logger
logging.basicConfig(level=logging.ERROR)  # Set the desired log level

# Create a logger instance
log = logging.getLogger(__name__)

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])

# Asynchronous by default
future = producer.send('my_topic', b'send data')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)

producer.flush()
producer.close()
