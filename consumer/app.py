from flask import Flask
from kafka import KafkaConsumer
import threading
import os





app = Flask(__name__)


consumer = KafkaConsumer(
"demo-topic",
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
auto_offset_reset="earliest",
value_deserializer=lambda v: v.decode("utf-8")
)


def consume_loop():
    for msg in consumer:
        print(f"Consumed: {msg.value}")

threading.Thread(target=consume_loop, daemon=True).start()


@app.route("/health")
def health():
    return "healthy consumer app", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)