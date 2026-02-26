from kafka import KafkaProducer
import json
import uuid
from datetime import datetime
import random
import time

BROKER = 'localhost:19092'
TOPIC_NAME = 'client_tickets'

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TYPES = ['incident', 'question', 'feature_request']
PRIORITIES = ['low', 'medium', 'high']

print(f"Producing tickets to topic '{TOPIC_NAME}' at {BROKER}...")

for _ in range(10):
    ticket = {
        'ticket_id': str(uuid.uuid4()),
        'client_id': str(uuid.uuid4()),
        'created_at': datetime.now().isoformat(),
        'subject': ' '.join(random.choices([
            'Issue', 'Login', 'Feature', 'Request', 'Bug', 'Payment', 'Refund',
            'Account', 'Error', 'Support'
        ], k=3)) + '.',
        'type': random.choice(TYPES),
        'priority': random.choice(PRIORITIES),
    }
    print("Producing:", ticket)
    producer.send(TOPIC_NAME, ticket)
    time.sleep(1)

producer.flush()
