from watchdog.events import FileSystemEventHandler
from file_converters import evtx_to_xml, log_to_csv
from minio import Minio
from minio.error import S3Error
from datetime import datetime
import logging
import os
 
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
MINIO_BUCKET = "cyberai"
 
# MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)
 
class EventHandler(FileSystemEventHandler):
    def on_created(self, event):
        print(f"File created: {event.src_path}")
        file_path = event.src_path
 
        file_ext = file_path.lower().split(".")[-1]
 
        # ------------- FILE TYPE HANDLING -------------
        if file_ext == "evtx":
            output_file_path = evtx_to_xml(file_path)
 
        elif file_ext == "log":
            output_file_path = log_to_csv(file_path)
 
        elif file_ext in ["json", "csv", "xml", "txt"]:
            output_file_path = file_path          
 
        else:
            print(f"Unsupported file type. Skipping: {file_path}")
            return
       
        filename = os.path.basename(output_file_path)
 
        # Build MinIO path
        y = datetime.utcnow().year
        m = datetime.utcnow().month
        d = datetime.utcnow().day
        now = datetime.utcnow()
        ts = now.strftime('%Y%m%dT%H%M%S') + f"{int(now.microsecond / 1000):03d}"
 
        extension = filename.split(".")[-1]
        base_name = filename.rsplit(".", 1)[0]
        minio_path = f"{y}/{m}/{d}/{base_name}_{ts}.{extension}"
 
        # Dynamic content type
        CONTENT_TYPES = {
            "xml": "application/xml",
            "csv": "text/csv",
            "json": "application/json",
            "txt": "text/plain",
            "log": "text/plain",
            "evtx": "application/octet-stream",
        }
 
        content_type = CONTENT_TYPES.get(extension, "application/octet-stream")
        # file_size = os.path.getsize(output_file_path)
 
        # Ensure bucket
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logging.info(f"Bucket created: {MINIO_BUCKET}")
 
        # Upload to MinIO (correct API)
        try:
            logging.info(f"Uploading file {minio_path} to MinIO")
            minio_client.fput_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=minio_path,      
                    file_path=output_file_path,              
                    content_type=content_type
                )
            logging.info(f"Uploaded {minio_path} to MinIO")

        except S3Error as e:
            logging.info("MinIO upload failed: {e}")
            raise
            
 
 
        logging.info(f"Uploaded to MinIO: {minio_path}")
 
        if output_file_path != file_path:   # Don't delete original
            os.remove(output_file_path)
            logging.info("Deleted temp file: {output_file_path}")
 
        logging.info("Processed: {output_file_path}")
 
 
 
    def on_modified(self, event):
        print(f"File modified: {event.src_path}")
 
    def on_moved(self, event):
        print(f"File moved: {event.src_path} → {event.dest_path}")