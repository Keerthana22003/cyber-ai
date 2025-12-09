from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
from io import BytesIO
from kafka.errors import KafkaError

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ingest"
GROUP_ID = "minio-file-consumer"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
MINIO_BUCKET = "cyber-ai-logs"

# MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

def upload_data_to_minio():

    # Ensure bucket
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"Bucket created: {MINIO_BUCKET}")

    # Create Kafka consumer
    try:
        print("Creating Kafka consumer...")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP_ID,
            request_timeout_ms=15000,
            session_timeout_ms=10000,
            value_deserializer=lambda x: x,  # KEEP RAW BYTES
        )
        print("Kafka consumer created successfully!")
    except KafkaError as e:
        print("Kafka connection failed:", e)
        return

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    file_buffers = {}

    # Consume messages
    for message in consumer:
        raw_key = message.key.decode("utf-8")
        print("Received:", raw_key)

        # Split key (filename | chunkID or EOF)
        filename, chunk_part = raw_key.split("|")

        # -------------------------
        # Handle EOF Message
        # -------------------------
        if chunk_part == "EOF":
            print(f"EOF received for file: {filename}")

            if filename not in file_buffers:
                print("Error: EOF received but no chunks exist yet")
                continue

            # Combine all chunks in order
            try:
                ordered_chunk_ids = sorted(file_buffers[filename].keys())
                ordered_chunks = [file_buffers[filename][cid] for cid in ordered_chunk_ids]

                final_content = b"".join(ordered_chunks)
                print(f"File reconstructed. Size: {len(final_content)} bytes")

                # Build MinIO path
                y = datetime.utcnow().year
                m = datetime.utcnow().month
                d = datetime.utcnow().day
                now = datetime.utcnow()
                ts = now.strftime('%Y%m%dT%H%M%S') + f"{int(now.microsecond / 1000):03d}"

                extension = filename.split(".")[-1]
                base_name = filename.rsplit(".", 1)[0]
                minio_path = f"{y}/{m}/{d}/{base_name}_{ts}.{extension}"

                # Upload to MinIO
                minio_client.put_object(
                    MINIO_BUCKET,
                    minio_path,
                    data=BytesIO(final_content),
                    length=len(final_content),
                    content_type="text/plain"
                )

                print(f"Uploaded to MinIO: {minio_path}")

                # Remove buffer after upload
                del file_buffers[filename]

            except Exception as e:
                print("ERROR during file reconstruction/upload:", e)

            continue  # proceed to next Kafka message

        # -------------------------
        # Normal Chunk Message
        # -------------------------
        try:
            chunk_id = int(chunk_part)
        except ValueError:
            print("Invalid chunk ID:", chunk_part)
            continue

        if filename not in file_buffers:
            file_buffers[filename] = {}

        # Store chunk as bytes
        file_buffers[filename][chunk_id] = message.value

        print(f"Stored chunk {chunk_id} for {filename}")
