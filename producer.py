import json
import time
from kafka import KafkaProducer
from  streamingconfig import *
#from config import *

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def stream_data():
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

        for record in data:
            print("Sending:", record)
            producer.send(KAFKA_TOPIC, record)
            time.sleep(0.01)

    producer.flush()
    print("Done producing")

if __name__ == "__main__":
    stream_data()