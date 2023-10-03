from confluent_kafka import Consumer, KafkaError
from clickhouse_driver import Client
from dotenv import load_dotenv
import fastavro, io, os, json

# Initialize Kafka consumer
def initialize_kafka_consumer():
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': os.getenv("KAFKA_BROKERS"),
        'group.id': os.getenv("KAFKA_GROUP_ID"),
        'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
    }
    # Create and return a Kafka consumer instance
    consumer = Consumer(consumer_config)
    consumer.subscribe([os.getenv("KAFKA_TOPIC")])  # Subscribe to the Kafka topic
    return consumer

# Initialize ClickHouse client
def initialize_clickhouse_client():
    # Configure ClickHouse connection parameters and return a ClickHouse client instance
    client = Client(host=os.getenv("CLICKHOUSE_HOST"), port=int(os.getenv("CLICKHOUSE_PORT")))
    client.execute(f'CREATE DATABASE IF NOT EXISTS {os.getenv("CLICKHOUSE_DATABASE")}')
    client.execute(f'USE {os.getenv("CLICKHOUSE_DATABASE")}')
    return client

# Create ClickHouse table
def create_clickhouse_table(client):
    # Define the ClickHouse table schema and create the table if it doesn't exist
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS game_rounds (
        created_timestamp DateTime,
        game_instance_id Int32,
        user_id String,
        game_id Int32,
        real_amount_bet Float64,
        bonus_amount_bet Nullable(Float64),
        real_amount_win Nullable(Float64),
        bonus_amount_win Float64,
        game_name String,
        provider String
    ) ENGINE = MergeTree()
    ORDER BY (game_id, user_id, toHour(created_timestamp));
    '''
    client.execute(create_table_query)

# Insert batch of records into ClickHouse
def insert_records_into_clickhouse(client, records):
    if not records:
        return

    insert_data_query = 'INSERT INTO game_rounds VALUES '
    values_list = []

    for avro_record in records:
        print(avro_record)
        # Extract values from Avro record
        created_timestamp = f"toDateTime('{avro_record['created_timestamp']}')"
        game_instance_id = avro_record['game_instance_id']
        user_id = avro_record['user_id']
        game_id = avro_record['game_id']
        real_amount_bet = avro_record['real_amount_bet']
        bonus_amount_bet = avro_record['bonus_amount_bet'] if avro_record['bonus_amount_bet'] is not None else 'NULL'
        real_amount_win = avro_record['real_amount_win'] if avro_record['real_amount_win'] is not None else 'NULL'
        bonus_amount_win = avro_record['bonus_amount_win']
        game_name = avro_record['game_name']
        provider = avro_record['provider'].replace("'", "''")  # Fix Escape Character for Play'n GO

        # Format values for insertion
        values = f"({created_timestamp}, {game_instance_id}, '{user_id}', {game_id}, {real_amount_bet}, {bonus_amount_bet}, {real_amount_win}, {bonus_amount_win}, '{game_name}', '{provider}')"
        values_list.append(values)

    # Join all values into a single string for batch insert
    insert_data_query += ', '.join(values_list)

    # Execute the batch insert query
    client.execute(insert_data_query)

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Load the maximum batch size for processing
    batch_max_records = int(os.getenv("BATCH_MAX_RECORDS"))

    # Parse the Avro schema string into a Python dictionary
    avro_schema = json.loads(os.getenv("AVRO_SCHEMA"))

    # Initialize Kafka consumer and ClickHouse client
    consumer = initialize_kafka_consumer()
    client = initialize_clickhouse_client()
    
    try:
        # Create the ClickHouse table if it doesn't exist
        create_clickhouse_table(client)

        # Initialize an empty batch list to hold Avro records
        message_batch = []

        while True:
            # Poll for Kafka messages
            message = consumer.poll(1.0)

            if message is None:
                if message_batch:
                    # Process any remaining messages in the last batch
                    insert_records_into_clickhouse(client, message_batch)
                    message_batch.clear()
                continue  # Continue polling for more messages
                
            if message.error():
                print(f"Error while consuming message: {message.error()}")
            else:
                avro_bytes = message.value()
                if avro_bytes:
                    try:
                        # Deserialize Avro bytes using fastavro and the provided Avro schema
                        avro_bytes_io = io.BytesIO(avro_bytes)
                        avro_record = fastavro.schemaless_reader(avro_bytes_io, avro_schema)
                        # Append the deserialized Avro record to the batch list
                        message_batch.append(avro_record)
                    except Exception as e:
                        print(f"Error while deserializing Avro record: {e}")
                else:
                    print("Received empty Avro record.")

            if len(message_batch) >= batch_max_records:
                # Insert the batch of records into ClickHouse
                insert_records_into_clickhouse(client, message_batch)
                message_batch.clear()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the Kafka consumer and ClickHouse client gracefully
        consumer.close()
        client.disconnect()

if __name__ == '__main__':
    main()
