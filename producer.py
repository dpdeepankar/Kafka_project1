from confluent_kafka import Producer
import time

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

topics = ['Sports','Tech','Politics']

while True:
    for topic in topics:
        message = f"Breaking news in {topic} at {time.strftime('%X')}"
        producer.produce(
                topic="notifications",
                key=topic,
                value=message,
                callback=delivery_report
                )
        producer.poll(0)
        time.sleep(2)
