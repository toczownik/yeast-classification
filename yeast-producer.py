import json
import random
import string
import sys
from time import sleep

from kafka import KafkaProducer


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"

    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(2, 7, 0),
    )

    try:
        while True:
            message = {
                "name":
                    ''.join(random.choice(string.ascii_uppercase) for x in range(3)) +
                    random.choice(string.digits) + "_YEAST",
                "mcg": random.randint(0, 100) / 100,
                "gvh": random.randint(0, 100) / 100,
                "alm": random.randint(0, 100) / 100,
                "mit": random.randint(0, 100) / 100,
                "erl": random.randint(0, 100) / 100,
                "pox": random.randint(0, 100) / 100,
                "vac": random.randint(0, 100) / 100,
                "nuc": random.randint(0, 100) / 100
            }
            producer.send("yeast", value=message)
            sleep(1)
    except KeyboardInterrupt:
        producer.close()
