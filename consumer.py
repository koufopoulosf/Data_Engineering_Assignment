from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import fastavro, io, os

# Load environment variables from .env
load_dotenv()

# Initialize Kafka producer
kafka_brokers = os.getenv("KAFKA_BROKERS")
kafka_consumer_id = os.getenv("KAFKA_CONSUMER_ID")
kafka_topic = os.getenv("KAFKA_TOPIC")
batch_max_records = int(os.getenv("BATCH_MAX_RECORDS"))

# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': kafka_brokers,  # Replace with your Kafka broker's IP and port
    'group.id': kafka_consumer_id,           # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start consuming from the beginning of the topic
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic where Avro messages are published
consumer.subscribe([kafka_topic])

# Define Avro schema
avro_schema = {
    "type": "record",
    "name": "MyRecord",
    "fields": [
        {"name": "created_timestamp", "type": "string"},
        {"name": "game_instance_id", "type": "int"},
        {"name": "user_id", "type": "string"},
        {"name": "game_id", "type": "int"},
        {"name": "real_amount_bet", "type": "double"},
        {"name": "bonus_amount_bet", "type": ["double", "null"]},
        {"name": "real_amount_win", "type": ["double", "null"]},
        {"name": "bonus_amount_win", "type": "double"},
        {"name": "game_name", "type": "string"},
        {"name": "provider", "type": "string"}
    ]
}

# Initialize an empty batch list
message_batch = []

# Poll for messages
while True:
    message = consumer.poll(1.0)  # Increased polling timeout to 10 seconds

    if message is None:
        # No more messages available, exit the loop
        break

    if message.error():
        print(f"Error while consuming message: {message.error()}")
    else:
        # Deserialize the Avro message
        avro_bytes = message.value()

        # Check if the Avro bytes are not empty
        if avro_bytes:
            try:
                avro_bytes_io = io.BytesIO(avro_bytes)  # Wrap Avro bytes in BytesIO
                avro_record = fastavro.schemaless_reader(avro_bytes_io, avro_schema)
                # Add the Avro record to the batch list
                message_batch.append(avro_record)
            except Exception as e:
                print(f"Error while deserializing Avro record: {e}")
        else:
            print("Received empty Avro record.")

    # Check if the batch size is reached, and process the batch
    if len(message_batch) >= batch_max_records:
        for record in message_batch:
            print(record)
        # Process the batch here as needed
        message_batch = []  # Clear the batch list after processing

# Process any remaining messages in the last batch
if message_batch:
    for record in message_batch:
        print(record)
    # Process the last batch here as needed

# Close the Kafka consumer gracefully
consumer.close()