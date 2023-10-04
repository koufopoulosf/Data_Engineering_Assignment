import fastavro, io
from config import (
    BATCH_MAX_RECORDS,
    AVRO_SCHEMA_CONFIG,
    configure_kafka_consumer,
    configure_clickhouse_client
)

# Insert batch of records into ClickHouse
def insert_records_into_clickhouse(client, records):
    if not records:
        return

    insert_data_query = 'INSERT INTO game_rounds VALUES '
    values_list = []
    for avro_record in records:
        # print(avro_record) # For debug purposes
        # Extract values from Avro record and apply transformations based on the SQL table
        created_timestamp = f"toDateTime('{avro_record['created_timestamp']}')"
        game_instance_id = avro_record['game_instance_id']
        user_id = avro_record['user_id']
        game_id = avro_record['game_id']
        real_amount_bet = avro_record['real_amount_bet'] if avro_record['real_amount_bet'] is not None else 'NULL'
        bonus_amount_bet = avro_record['bonus_amount_bet'] if avro_record['bonus_amount_bet'] is not None else 'NULL'
        real_amount_win = avro_record['real_amount_win'] if avro_record['real_amount_win'] is not None else 'NULL'
        bonus_amount_win = avro_record['bonus_amount_win'] if avro_record['bonus_amount_win'] is not None else 'NULL'
        game_name = avro_record['game_name']
        provider = avro_record['provider'].replace("'", "''") # Fix Escape Character for Play'n GO

        # Format values for insertion
        values = f"({created_timestamp}, {game_instance_id}, '{user_id}', {game_id}, {real_amount_bet}, {bonus_amount_bet}, {real_amount_win}, {bonus_amount_win}, '{game_name}', '{provider}')"
        values_list.append(values)

    # Join all values into a single string for batch insert
    insert_data_query += ', '.join(values_list)

    # Execute the batch insert query
    client.execute(insert_data_query)

def main():
    # Configure Kafka consumer and ClickHouse client
    consumer = configure_kafka_consumer()
    client = configure_clickhouse_client()
    try:

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
                        avro_record = fastavro.schemaless_reader(avro_bytes_io, AVRO_SCHEMA_CONFIG)
                        # Append the deserialized Avro record to the batch list
                        message_batch.append(avro_record)
                    except Exception as e:
                        print(f"Error while deserializing Avro record: {e}")
                else:
                    print("Received empty Avro record.")

            if len(message_batch) >= BATCH_MAX_RECORDS:
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
