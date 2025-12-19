from flask import Flask
from kafka import KafkaProducer
import time
import threading
import os

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    value_serializer=lambda v: v.encode("utf-8"),
)

def publish_loop():
    while True:
        topic="demo-topic"
        msg="hello from producer app"
        producer.send(topic, msg)
        print(f"Published message: {msg}: to topic: {topic}")
        producer.flush()
        time.sleep(2)

threading.Thread(target=publish_loop, daemon=True).start()

@app.route("/health")
def health():
    return "healthy producer app", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
