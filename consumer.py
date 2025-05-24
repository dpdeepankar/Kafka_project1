from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
    })

consumer.subscribe(['notifications'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        print(f"Received: {msg.value().decode()} (from partition {msg.partition()})")
except KeyboardInterrupts:
    pass
finally:
    consumer.close()

