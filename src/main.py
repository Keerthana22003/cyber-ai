from file_event_handlers import EventHandler
from kafka_consumer import upload_data_to_minio
from watchdog.observers import Observer
import time
import threading

if __name__ == "__main__":
    path = "..\data\\raw\input"

    # Start Kafka consumer in background thread
    # consumer = threading.Thread(target=upload_data_to_minio, daemon=True)
    # consumer.start()
        

    # Start Watchdog observer for file changes
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    print(f"Monitoring directory: {path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
