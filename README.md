# Large CSV File Processing Python API

A Python API for processing large CSV files, publishing them to Kafka, and storing them in ClickHouse. This project is designed to facilitate the ingestion and analysis of large datasets efficiently.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Installation](#installation)
  - [Configuration](#configuration)
- [Endpoints](#endpoints)
- [Improvements](#improvements)

## <a name="features"></a>Features

- **Producer:** Accepts large CSV files, processes them, and publishes Avro records to a Kafka topic.
- **Consumer:** Listens to the Kafka topic, deserializes Avro records, and stores them in ClickHouse.
- **API Endpoints:** Provides a FastAPI endpoint for uploading CSV files and querying aggregated data from ClickHouse.
- **Flexible Configuration:** Easily configure ClickHouse, Kafka, and Avro schema through the `config.py` file.
- **Error Handling:** Comprehensive error handling and logging.


## <a name="getting-started"></a>Getting Started

### <a name="installation"></a>Installation

```
apt update && apt upgrade -y
apt install python3-pip docker-compose -y
git clone https://github.com/River-iGaming/hera.candidates-task && cd hera.candidates-task
docker-compose up -d
cd ~/
git clone https://github.com/koufopoulosf/Data_Engineering_Assignment && cd Data_Engineering_Assignment
pip install -r requirements.txt
python3 consumer.py &
python3 producer.py &
```

The documentation for the Python REST API service is available at http://localhost:8000/docs or http://localhost:8000/redoc

## <a name="configuration"></a>Configuration

The goal is to make this solution modular and flexible to handle various data format requirements. To achieve this:

1. Edit the ```config.py``` file to configure ClickHouse, Kafka, and Avro schema according to your setup. Update the following variables:

- **CLICKHOUSE_HOST** and **CLICKHOUSE_PORT** for ClickHouse configuration.
- **KAFKA_BROKERS**, **KAFKA_GROUP_ID**, **KAFKA_TOPIC**, and other Kafka settings.
- **AVRO_SCHEMA_CONFIG** to match your Avro schema.

2. Next, edit the ```consumer.py``` file, specifically the ```def insert_records_into_clickhouse(client, records)``` function, to align the AVRO records with the SQL table and apply necessary transformations based on the provided data.

3. Lastly, ensure that the SQL queries in the ```config.py``` file match those in the ```producer.py``` file ([GET] Endpoint) to align the data structure and format with other datasets.

## <a name="endpoints"></a>Endpoints

1. **[POST] /csv_upload**: Upload a CSV file for processing. The API will convert it to Avro format and publish it to Kafka.

2. **[GET] /aggregated_game_rounds**: Query aggregated game round data from ClickHouse. Supports filters for player ID, game ID, date range, and pagination.


## <a name="improvements"></a>Improvements

In this proposed solution, I prioritize the simplicity, modularity, and flexibility of the codebase, allowing it to easily adapt to other datasets and schema requirements.

However, for those seeking enhanced performance, with a willingness to trade simplicity for a more complex codebase, several improvements are available:

- In ```producer.py```, we use the csv library for reading the CSV file. Other libraries, such as [polars](https://github.com/pola-rs/polars) (implemented in Rust), could result in faster parsing and processing of large CSV files.
- In ```producer.py```, we load the complete file into memory and then process it. Instead, we could read the file in chunks to facilitate more of a streaming approach and even apply asynchronous methods to process all the chunks even faster. I tried such approaches, however, I noticed that the produced data faced data loss and inconsistency issues.
- Besides the hardware/service configuration part of Kafka and ClickHouse instances (or ClickHouse engines), another potential improvement would be the use of prepared statements.
- Ideally, for the CSV uploading part (not the custom [GET] endpoints), we should only configure the ```config.py``` file. However, dynamically matching the AVRO schema with the SQL table in the ```def insert_records_into_clickhouse(client, records)``` function, without making manual adjustments based on the requirements of the given dataset, would result in lower performance.

If you identify any additional potential improvements that I haven't listed, please don't hesitate to inform me!

### Made with ❤️ by Filippos
