from confluent_kafka import Producer, Consumer
from clickhouse_driver import Client
import sys

# ClickHouse configuration
CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 9500

# Kafka configuration
KAFKA_GROUP_ID = "my-consumer-group"
KAFKA_BROKERS = "127.0.0.1:9092"
KAFKA_TOPIC = "staging-bets"
QUEUE_BUFFERING_MAX_KBYTES = 51200 # 50 MB buffer size, greater than BATCH_MAX_RECORDS size (in MB)
BATCH_MAX_RECORDS = 50000 # Each record is ~320 bytes * 50.000 => ~ 15 MB

# Avro Schema Configuration
AVRO_SCHEMA_CONFIG = {
    "type": "record",
    "name": "RiverTechRecord",
    "fields": [
        {"name": "created_timestamp", "type": "string"},
        {"name": "game_instance_id", "type": "int"},
        {"name": "user_id", "type": "string"},
        {"name": "game_id", "type": "int"},
        {"name": "real_amount_bet", "type": ["double", "null"]},
        {"name": "bonus_amount_bet", "type": ["double", "null"]},
        {"name": "real_amount_win", "type": ["double", "null"]},
        {"name": "bonus_amount_win", "type": ["double", "null"]},
        {"name": "game_name", "type": "string"},
        {"name": "provider", "type": "string"}
    ]
}

# ClickHouse Queries
CLICKHOUSE_QUERIES= [
    "DROP DATABASE IF EXISTS rivertechdb;"
    "CREATE DATABASE IF NOT EXISTS rivertechdb;",
    "USE rivertechdb;",
    "DROP TABLE IF EXISTS game_rounds;",
    "CREATE TABLE IF NOT EXISTS game_rounds (created_timestamp DateTime, game_instance_id Int32, user_id String, game_id Int32, real_amount_bet Nullable(Float64), bonus_amount_bet Nullable(Float64), real_amount_win Nullable(Float64), bonus_amount_win Nullable(Float64), game_name String, provider String) ENGINE = MergeTree() ORDER BY (game_instance_id, game_id, user_id, toHour(created_timestamp)) PRIMARY KEY game_instance_id;",
    "DROP TABLE IF EXISTS hourly_aggregated_game_rounds;",
    "CREATE TABLE IF NOT EXISTS hourly_aggregated_game_rounds (created_hour DateTime, game_id Int32, user_id String, total_real_amount_bet Nullable(Float64), total_bonus_amount_bet Nullable(Float64), total_real_amount_win Nullable(Float64), total_bonus_amount_win Nullable(Float64), game_name String, provider String) ENGINE = MergeTree() ORDER BY (game_id, user_id, created_hour);",
    "DROP MATERIALIZED VIEW IF EXISTS mv_hourly_aggregated_game_rounds;",
    "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_aggregated_game_rounds TO hourly_aggregated_game_rounds AS SELECT toStartOfHour(created_timestamp) AS created_hour, game_id, user_id, sum(real_amount_bet) AS total_real_amount_bet, sum(bonus_amount_bet) AS total_bonus_amount_bet, sum(real_amount_win) AS total_real_amount_win, sum(bonus_amount_win) AS total_bonus_amount_win, game_name, provider FROM game_rounds GROUP BY created_hour, game_id, user_id, game_name, provider;"
]


###################
# For producer.py #
###################

# Initialize Kafka producer
def initialize_kafka_producer():
    # Configure Kafka producer
    producer_config = {
        "bootstrap.servers": KAFKA_BROKERS,
        "queue.buffering.max.kbytes": QUEUE_BUFFERING_MAX_KBYTES,
        "batch.num.messages": BATCH_MAX_RECORDS
    }
    try:
        producer = Producer(producer_config)
        return producer
    except Exception as e:
        print(f"Error initializing Kafka producer: {str(e)}")
        sys.exit(1)

# Initialize ClickHouse client
def initialize_clickhouse_client():
    try:
        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        client.execute(CLICKHOUSE_QUERIES[2])
        return client
    except Exception as e:
        print(f"Error initializing ClickHouse client: {str(e)}")
        sys.exit(1)


###################
# For consumer.py #
###################

# Initialize Kafka consumer
def initialize_kafka_consumer():
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
    }
    # Create and return a Kafka consumer instance
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC])  # Subscribe to the Kafka topic
        return consumer
    except Exception as e:
        print(f"Error initializing Kafka consumer: {str(e)}")
        sys.exit(1)

# Configure ClickHouse client
def configure_clickhouse_client():
    try:
        client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        for query in CLICKHOUSE_QUERIES:
            client.execute(query)
        return client
    except Exception as e:
        print(f"Error configuring ClickHouse client: {str(e)}")
        sys.exit(1)
