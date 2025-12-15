#!/usr/bin/env python3
"""
Unified CyberAI Data Pipeline
- Watches local directory for new files (file ingestion)
- Processes and uploads to MinIO via Kafka
- Monitors MinIO for new objects (tokenization service)
- Streams objects, tokenizes, and generates PDFs
"""

import os
import sys
import time
import signal
import logging
import argparse
import threading
from pathlib import Path

# Import modules from both projects
from file_event_handlers import EventHandler
from watchdog.observers import Observer

# Import tokenization service functions
from tokenizer import (
    init_db,
    create_minio_client,
    listen_loop as tokenizer_listen_loop,
    flush_buffer,
    # _token_parts,
    _buffer_tokens
)

# Global flags
_running = True
_observer = None

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# In main.py, update the graceful_shutdown function:
def graceful_shutdown(signum, frame):
    """Handle graceful shutdown for both services"""
    global _running, _observer
    logger.info(f"Signal {signum} received - shutting down gracefully...")
    _running = False
    
    # Stop file watcher
    if _observer:
        logger.info("Stopping file watcher...")
        _observer.stop()
    
    # Flush tokenizer buffer (updated for new format-preserving version)
    from tokenizer import _current_format
    if _buffer_tokens > 0 and _current_format:
        logger.info(f"Flushing remaining buffer (~{_buffer_tokens} tokens) to file...")
        try:
            paths = flush_buffer(_current_format)
            for p in paths:
                logger.info(f"Shutdown file created: {p}")
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
    
    logger.info("Shutdown complete")


def start_file_watcher(watch_path: str):
    """Start the file watcher service (Project 1)"""
    global _observer, _running
    
    logger.info(f"Starting file watcher on: {watch_path}")
    
    # Ensure directory exists
    Path(watch_path).mkdir(parents=True, exist_ok=True)
    
    event_handler = EventHandler()
    _observer = Observer()
    _observer.schedule(event_handler, watch_path, recursive=True)
    _observer.start()
    
    logger.info(f"File watcher started - monitoring: {watch_path}")
    
    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if _observer:
            _observer.stop()
            _observer.join()


def start_tokenizer_service():
    """Start the MinIO tokenizer service (Project 2)"""
    global _running
    
    logger.info("Starting MinIO tokenizer service...")
    
    # Initialize database
    init_db()
    
    # Start listening/polling loop
    try:
        tokenizer_listen_loop()
    except Exception as e:
        logger.error(f"Tokenizer service error: {e}")
    finally:
        logger.info("Tokenizer service stopped")


def run_both_services(watch_path: str):
    """Run both file watcher and tokenizer services concurrently"""
    global _running
    
    logger.info("="*60)
    logger.info("Starting Unified CyberAI Data Pipeline")
    logger.info("="*60)
    logger.info(f"Watch Path: {watch_path}")
    logger.info(f"MinIO Host: {os.getenv('MINIO_HOST', 'localhost:9000')}")
    logger.info(f"Bucket: {os.getenv('BUCKET_NAME', 'cyberai')}")
    logger.info("="*60)
    
    # Start tokenizer service in background thread
    tokenizer_thread = threading.Thread(
        target=start_tokenizer_service,
        daemon=True,
        name="TokenizerService"
    )
    tokenizer_thread.start()
    
    # Give tokenizer a moment to initialize
    time.sleep(2)
    
    # Run file watcher in main thread (blocks until shutdown)
    try:
        start_file_watcher(watch_path)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        _running = False
        tokenizer_thread.join(timeout=5)


def main():
    """Main entry point with CLI arguments"""
    parser = argparse.ArgumentParser(
        description="Unified CyberAI Data Pipeline - File Ingestion & Tokenization"
    )
    parser.add_argument(
        "--mode",
        choices=["both", "watcher", "tokenizer"],
        default="both",
        help="Service mode: 'both' (default), 'watcher' only, or 'tokenizer' only"
    )
    parser.add_argument(
        "--watch-path",
        default="../data/raw/input",
        help="Path to watch for new files (default: ../data/raw/input)"
    )
    
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    # Run based on mode
    if args.mode == "both":
        run_both_services(args.watch_path)
    elif args.mode == "watcher":
        logger.info("Running in WATCHER-ONLY mode")
        start_file_watcher(args.watch_path)
    elif args.mode == "tokenizer":
        logger.info("Running in TOKENIZER-ONLY mode")
        start_tokenizer_service()


if __name__ == "__main__":
    main()