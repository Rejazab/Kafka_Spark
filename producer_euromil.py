
from kafka import KafkaProducer, KafkaClient
import json

################################################################################
# Get Data from EuroMillion
################################################################################


################################################################################
# Set up producer
################################################################################

KAFKA = KafkaClient(bootstrap_servers='localhost:9092')
PRODUCER = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='producer-euromil'
)

TOPIC = 'euromil-topic'


################################################################################
# Loop, add to kafka
################################################################################


