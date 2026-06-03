from kafka import KafkaProducer
from data_generate import generate_order_data, generate_payment_data, generate_return_data,generate_order_corrupt_data,generate_payment_corrupt_data
import time
import json
import random
import uuid
producer=KafkaProducer(bootstrap_servers='localhost:29092',
                       acks='all',
                       enable_idempotence=True,
                       key_serializer=lambda x: x.encode('utf-8'),
                       value_serializer=lambda x: json.dumps(x).encode('utf-8')
                       )

def send_order_event():
    while True:
        order_data=generate_order_data()
        order_data=generate_order_corrupt_data(order_data)
        producer.send('order_event', key=order_data["order_id"], value=order_data)
        
        payment_data=None
        order_statuses=[step["order_status"] for step in order_data["order_status"]]
        if "CANCELLED" not in order_statuses:
                payment_data=generate_payment_data(order_data)
                payment_data=generate_payment_corrupt_data(payment_data)
                producer.send('payment_event', key=order_data["order_id"], value=payment_data)
            
        if "DELIVERED" in order_statuses:
            if random.random() < 0.2:
                return_data=generate_return_data(order_data)
                producer.send('return_event', key=order_data["order_id"], value=return_data)
        
        producer.flush()
        time.sleep(random.uniform(0.5, 2))
        
send_order_event()
        