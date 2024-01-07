
import json
from kafka import KafkaConsumer


################################################################################
# Consume value from producer
################################################################################

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('euromil-topic',
                         group_id='consumer-euromil',
                         bootstrap_servers='localhost:9092',
                         consumer_timeout_ms=5000)

print('Consumer init, info : %s / type : %s'%(consumer.bootstrap_connected(), consumer.partitions_for_topic('test-topic')))
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s val=%s " % (message.topic, message.partition,
                                         message.offset, message.key, message.value))


################################################################################
# Store in kafka-data as parquet file
################################################################################

