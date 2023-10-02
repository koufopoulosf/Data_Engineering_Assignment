from fastapi import FastAPI, UploadFile, File, HTTPException
from confluent_kafka import Producer
import csv, fastavro, io, os, sys, time, json
from dotenv import load_dotenv
from loguru import logger

# Initialize the FastAPI application instance
app = FastAPI()

# Configure logger
logger.remove()
logger.add(sys.stdout, colorize=True, format='<green>{level}</green> | {time:DD-MM-YYYY HH:mm:ss} | <level>{message}</level>')

# Load environment variables from .env file
load_dotenv()
batch_max_records = int(os.getenv("BATCH_MAX_RECORDS"))
kafka_topic = os.getenv("KAFKA_TOPIC")

# Parse the Avro schema string into a Python dictionary
avro_schema = json.loads(os.getenv("AVRO_SCHEMA"))

# Initialize Kafka producer
def initialize_kafka_producer():
    producer_config = {
        "bootstrap.servers": os.getenv("KAFKA_BROKERS"),
        "client.id": os.getenv("KAFKA_CLIENT_ID"),
        "queue.buffering.max.kbytes": int(os.getenv("QUEUE_BUFFERING_MAX_KBYTES")),
        "batch.num.messages": batch_max_records
    }
    try:
        producer = Producer(producer_config)
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {str(e)}")
        sys.exit(1)

producer = initialize_kafka_producer()

# Function to send records to Kafka
def send_records_to_kafka(records):
    try:
        avro_bytes_list = []
        for avro_record in records:
            print(avro_record)
            # Serialize Avro records to Avro binary format
            avro_bytes_io = io.BytesIO()
            fastavro.schemaless_writer(avro_bytes_io, avro_schema, avro_record)
            avro_bytes = avro_bytes_io.getvalue()
            avro_bytes_list.append(avro_bytes)

        # Send the batched Avro records to Kafka
        for avro_bytes in avro_bytes_list:
            producer.produce(kafka_topic, value=avro_bytes)
        
        producer.flush()  # Flush Kafka messages here

    except Exception as e:
        logger.error(f"Error sending records to Kafka: {str(e)}")

# Function to process a CSV row and convert it to Avro record
def process_csv_row_to_avro(row):
    avro_record = {}
    for i, field in enumerate(avro_schema["fields"]):
        cell_value = row[i].strip() if i < len(row) else None
        try:
            if field["type"] == ["double", "null"] or field["type"] == "double":
                avro_record[field["name"]] = float(cell_value) if cell_value else None
            elif field["type"] == "int":
                avro_record[field["name"]] = int(cell_value) if cell_value else None
            elif field["type"] == "string":
                avro_record[field["name"]] = str(cell_value)
        except (ValueError, TypeError):
            avro_record[field["name"]] = None  # Handle invalid or None values
    return avro_record

# Function to process an uploaded CSV file and publish Avro records
async def process_uploaded_csv_to_avro(file):
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

            avro_record = process_csv_row_to_avro(row)
            avro_records_buffer.append(avro_record)

            # Check if the buffer size exceeds the batch size
            if len(avro_records_buffer) >= batch_max_records:
                send_records_to_kafka(avro_records_buffer)
                avro_records_buffer.clear()

        # Send any remaining records to Kafka
        if avro_records_buffer:
            send_records_to_kafka(avro_records_buffer)

        return {"message": "CSV data uploaded, processed, converted to Avro, and published to Kafka."}

    except (HTTPException, Exception) as err:
        logger.error(f"An error occurred: {str(err)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(err)}")

@app.post("/upload_and_process_to_avro")
async def upload_and_process_csv_to_avro(file: UploadFile = File(...)):
    try:
        start_time = time.perf_counter()
        result = await process_uploaded_csv_to_avro(file)
        end_time = time.perf_counter()
        processing_time = end_time - start_time
        logger.info(f"Processing completed in {processing_time} seconds.")
        return result
    except HTTPException as http_err:
        return http_err
    except Exception as err:
        logger.error(f"An error occurred: {str(err)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(err)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
