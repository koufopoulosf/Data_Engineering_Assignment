import csv, fastavro, io, sys
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import ORJSONResponse
from clickhouse_driver.errors import Error as ClickHouseError
from confluent_kafka.error import KafkaError
from typing import Optional
from loguru import logger
from config import (
    KAFKA_TOPIC,
    BATCH_MAX_RECORDS,
    AVRO_SCHEMA_CONFIG,
    initialize_kafka_producer,
    initialize_clickhouse_client
)

# Initialize the FastAPI application instance
app = FastAPI()

# Configure logger
logger.remove()
logger.add(sys.stdout, colorize=True, format='<green>{level}</green> | {time:DD-MM-YYYY HH:mm:ss} | <level>{message}</level>')

# Function to process a CSV row and convert it to Avro record
def process_csv_row_to_avro(row, avro_schema):
    avro_record = {}
    for i, field in enumerate(avro_schema["fields"]):
        cell_value = row[i].strip() if i < len(row) else None
        try:
            if field["type"] == ["double", "null"] or field["type"] == "double":
                avro_record[field["name"]] = float(cell_value) if cell_value else None
            elif field["type"] == "int":
                avro_record[field["name"]] = int(cell_value) if cell_value else None
            elif field["type"] == "string":
                avro_record[field["name"]] = str(cell_value) if cell_value else None
        except (ValueError, TypeError) as e:
            avro_record[field["name"]] = None  # Handle invalid or None values
            logger.error(f"Error processing CSV row: {str(e)}")
    return avro_record

# Function to send records to Kafka
def send_records_to_kafka(records, kafka_producer, avro_schema):
    try:
        avro_bytes_list = []
        for avro_record in records:
            # print(avro_record) # For debug purposes
            # Serialize Avro records to Avro binary format
            avro_bytes_io = io.BytesIO()
            fastavro.schemaless_writer(avro_bytes_io, avro_schema, avro_record)
            avro_bytes = avro_bytes_io.getvalue()
            avro_bytes_list.append(avro_bytes)

        # Send the batched Avro records to Kafka
        for avro_bytes in avro_bytes_list:
            kafka_producer.produce(KAFKA_TOPIC, value=avro_bytes)
        
        kafka_producer.flush()  # Flush Kafka messages here

    except Exception as e:
        logger.error(f"Error sending records to Kafka: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error sending records to Kafka: {str(e)}")

# Function to process an uploaded CSV file and publish Avro records
async def process_uploaded_csv_to_avro(file, kafka_producer, avro_schema):
    try:
        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="File type is not allowed")

        # Read the entire CSV file | Reading in chunks (streaming approach) resulted in data loss/inconsistency
        csv_content = await file.read()
        csv_lines = csv_content.decode("ISO-8859-1").replace('\x00', '')
        csv_lines = csv_lines.splitlines()
        csv_reader = csv.reader(csv_lines)

        avro_records_buffer = []

        is_header = True  # Flag to indicate if the current row is the header
        for row in csv_reader:
            if is_header:
                is_header = False
                continue  # Skip the header row

            if not row:
                continue  # Skip empty rows

            avro_record = process_csv_row_to_avro(row, avro_schema)
            avro_records_buffer.append(avro_record)

            # Check if the buffer size exceeds the batch size
            if len(avro_records_buffer) >= BATCH_MAX_RECORDS:
                send_records_to_kafka(avro_records_buffer, kafka_producer, avro_schema)
                avro_records_buffer.clear()

        # Send any remaining records to Kafka
        if avro_records_buffer:
            send_records_to_kafka(avro_records_buffer, kafka_producer, avro_schema)

        return {"message": "CSV data uploaded, processed, converted to Avro, and published to Kafka."}

    except Exception as e:
        logger.error(f"An error occurred during processing the uploaded CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred during processing the uploaded CSV: {str(e)}")

@app.post("/csv_upload")
async def csv_upload(csv_file: UploadFile = File(...)):
    try:
        kafka_producer = initialize_kafka_producer()
        return await process_uploaded_csv_to_avro(file=csv_file, kafka_producer=kafka_producer, avro_schema=AVRO_SCHEMA_CONFIG)
    except KafkaError as kafka_error:
        logger.error(f"[POST Endpoint] Kafka Error: {str(kafka_error)}")
        raise HTTPException(status_code=500, detail="Kafka encountered an error. Please check your topic and Kafka configuration.")
    except Exception as e:
        logger.error(f"[POST Endpoint] An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"[POST Endpoint] An error occurred: {str(e)}")

@app.get("/aggregated_game_rounds")
async def get_aggregated_game_rounds(
    player_id: Optional[str] = Query(None, description="Player ID"),
    game_id: Optional[int] = Query(None, description="Game ID"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD HH:mm:ss format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD HH:mm:ss format"),
    page_num: int = Query(1, description="Page number"),
    page_size: int = Query(10, description="Page size")
):
    try:
        # Initialize ClickHouse client instance
        client = initialize_clickhouse_client()

        # Base query for data retrieval
        data_query = """
            SELECT
                created_hour,
                game_id,
                user_id,
                total_real_amount_bet,
                total_bonus_amount_bet,
                total_real_amount_win,
                total_bonus_amount_win,
                game_name,
                provider
            FROM
                hourly_aggregated_game_rounds
        """
        # Base query for counting records
        count_query = "SELECT COUNT(*) FROM hourly_aggregated_game_rounds"

        # Add WHERE clause if any filter is provided
        conditions = []
        if player_id:
            conditions.append(f"user_id = '{player_id}'")
        if game_id:
            conditions.append(f"game_id = {game_id}")
        if start_date:
            conditions.append(f"created_hour >= '{start_date}'")
        if end_date:
            conditions.append(f"created_hour < '{end_date}'")

        if conditions:
            data_query += " WHERE " + " AND ".join(conditions)
            count_query += " WHERE " + " AND ".join(conditions)
        
        # Add pagination
        data_query += f" ORDER BY created_hour LIMIT {page_size} OFFSET {(page_num - 1) * page_size};"

        # Execute the data query against ClickHouse
        results = client.execute(data_query)

        # Execute the count query to get the total results count
        total_results = client.execute(count_query)[0][0]
  
        pagination = {}
        if (page_num - 1) * page_size + page_size >= total_results:
            pagination["next"] = None
            if page_num > 1:
                pagination["previous"] = f"/aggregated_game_rounds?page_num={page_num-1}&page_size={page_size}"
            else:
                pagination["previous"] = None
        else:
            pagination["next"] = f"/aggregated_game_rounds?page_num={page_num+1}&page_size={page_size}"
            if page_num > 1:
                pagination["previous"] = f"/aggregated_game_rounds?page_num={page_num-1}&page_size={page_size}"
            else:
                pagination["previous"] = None

        response_data = {
            "data": [
                {
                    "created_hour": row[0],
                    "game_id": row[1],
                    "user_id": row[2],
                    "total_real_amount_bet": row[3],
                    "total_bonus_amount_bet": row[4],
                    "total_real_amount_win": row[5],
                    "total_bonus_amount_win": row[6],
                    "game_name": row[7],
                    "provider": row[8]
                }
                for row in results
            ],
            "total_results": total_results,
            "pagination": pagination
        }

        return ORJSONResponse(content=response_data)

    except ClickHouseError as clickhouse_error:
        logger.error(f"[GET Endpoint] ClickHouse Error: {str(clickhouse_error)}")
        raise HTTPException(status_code=500, detail="ClickHouse encountered an error. Please check your query and ClickHouse configuration.")

    except Exception as e:
        logger.error(f"[GET Endpoint] An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"[GET Endpoint] An error occurred: {str(e)}")
        
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
