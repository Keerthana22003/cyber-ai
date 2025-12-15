#!/usr/bin/env python3
"""
MinIO Tokenizer Module - Format-Preserving Version with Subdirectory Output

Enhanced Features:
- Creates subdirectories per input file: processed_tokens/<input_base>_<timestamp>_<format>/
- Numbered output files: 001.json, 002.json, etc.
- All other functionality remains the same
"""

import os
import io
import re
import json
import csv
import time
import logging
import sqlite3
import codecs
import copy
from datetime import datetime
from typing import Any, Optional, List, Dict
from urllib.parse import unquote_plus

from minio import Minio
from minio.error import S3Error

from xml.etree.ElementTree import XMLPullParser, Element, tostring

import ijson
import lxml.etree as LET

# Configuration
MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
ACCESS_KEY = os.getenv("ACCESS_KEY", "admin")
SECRET_KEY = os.getenv("SECRET_KEY", "admin12345")
BUCKET_NAME = os.getenv("BUCKET_NAME", "cyberai")
SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes")

LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR", "./processed_tokens")
TOKEN_THRESHOLD = int(os.getenv("TOKEN_THRESHOLD", "10000"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
SQLITE_DB = os.getenv("SQLITE_DB", "./processed_objects.db")

# Internal state
_json_buffer: List[Dict] = []
_csv_buffer: List[List[str]] = []
_xml_buffer: List[Element] = []
_jsonl_buffer: List[Dict] = []

_buffer_tokens = 0
_file_counter = 1
_running = True
_db_conn = None
_current_format = None
_current_object_base: Optional[str] = None
_current_output_subdir: Optional[str] = None  # NEW: Current output subdirectory

# CSV preservation state
_csv_header: Optional[List[str]] = None
_csv_header_line: Optional[str] = None
_csv_dialect = None
_csv_header_written = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Database functions
def init_db(path: str = SQLITE_DB):
    global _db_conn
    _db_conn = sqlite3.connect(path, check_same_thread=False)
    cur = _db_conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS processed (object_name TEXT PRIMARY KEY, etag TEXT, last_modified TEXT)"
    )
    _db_conn.commit()
    return _db_conn


def is_processed(object_name: str) -> bool:
    cur = _db_conn.cursor()
    cur.execute("SELECT 1 FROM processed WHERE object_name = ?", (object_name,))
    return cur.fetchone() is not None


def mark_processed(object_name: str, etag: Optional[str], last_modified: Optional[str]):
    cur = _db_conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO processed (object_name, etag, last_modified) VALUES (?, ?, ?)",
        (object_name, etag, last_modified),
    )
    _db_conn.commit()


# MinIO client
def create_minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)


# Tokenization functions
def tokenize_text(text: str) -> List[str]:
    if not text:
        return []
    token_pattern = re.compile(r"\w+|\d+|[^\s\w]", flags=re.UNICODE)
    return token_pattern.findall(text)


def count_tokens(obj: Any) -> int:
    """Count tokens for an item. Non-strings -> compact JSON; fallback to str()"""
    if isinstance(obj, str):
        text = obj
    else:
        try:
            text = json.dumps(obj, separators=(',', ':'), ensure_ascii=False)
        except Exception:
            text = str(obj)
    return len(tokenize_text(text))

# Add this new function after create_minio_client():
def get_current_timestamp() -> str:
    """Get current timestamp in format YYYYMMDDTHHMMSSMMM"""
    return datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3]


# Output directory management - MODIFIED
def create_output_subdir_initial(base_name: str, format_type: str) -> str:
    """Create initial subdirectory placeholder without timestamp.
    
    Format: processed_tokens/<base>_<format>/
    Will be renamed with timestamp when first file is written.
    """
    safe_base = re.sub(r"[^A-Za-z0-9_.-]", "_", base_name)
    subdir_name = f"{safe_base}_{format_type}"
    subdir_path = os.path.join(LOCAL_OUTPUT_DIR, subdir_name)
    os.makedirs(subdir_path, exist_ok=True)
    return subdir_path

def update_subdir_with_timestamp(format_type: str, timestamp: str):
    """Rename subdirectory with actual dump timestamp when first file is written."""
    global _current_output_subdir, _current_object_base
    
    if _current_output_subdir:
        # Check if timestamp is already in the path
        if re.search(r'\d{8}T\d{9}', _current_output_subdir):
            # Already has timestamp, don't rename
            return
            
        safe_base = re.sub(r"[^A-Za-z0-9_.-]", "_", _current_object_base)
        new_subdir_name = f"{safe_base}_{timestamp}_{format_type}"
        new_subdir_path = os.path.join(LOCAL_OUTPUT_DIR, new_subdir_name)
        
        try:
            os.rename(_current_output_subdir, new_subdir_path)
            logging.info(f"Renamed output subdirectory to: {new_subdir_path}")
            _current_output_subdir = new_subdir_path
        except Exception as e:
            logging.error(f"Failed to rename subdirectory: {e}")


def get_output_filename(format_type: str, counter: int) -> str:
    """Generate simple numbered filename: 001.json, 002.csv, etc."""
    extensions = {
        'json': '.json',
        'jsonl': '.jsonl',
        'csv': '.csv',
        'xml': '.xml'
    }
    ext = extensions.get(format_type, '.txt')
    return f"{counter:03d}{ext}"


# Format-specific output writers - MODIFIED
def write_json_output(items: List[Dict], filename: str) -> str:
    """Write JSON output to current subdirectory"""
    filepath = os.path.join(_current_output_subdir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(items, f, separators=(',', ':'), ensure_ascii=False)
    logging.info(f"JSON file written: {filepath} ({len(items)} items)")
    return filepath


def write_jsonl_output(items: List[Dict], filename: str) -> str:
    """Write JSONL output to current subdirectory"""
    filepath = os.path.join(_current_output_subdir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        for item in items:
            f.write(json.dumps(item, separators=(',', ':'), ensure_ascii=False) + '\n')
    logging.info(f"JSONL file written: {filepath} ({len(items)} lines)")
    return filepath


def write_csv_output(rows: List[List[str]], filename: str) -> str:
    """Write CSV output to current subdirectory"""
    filepath = os.path.join(_current_output_subdir, filename)
    global _csv_header_line, _csv_dialect, _csv_header_written

    writer_kwargs = {}
    try:
        if _csv_dialect:
            writer_kwargs['dialect'] = _csv_dialect
    except Exception:
        writer_kwargs = {}

    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        if _csv_header_line is not None and not _csv_header_written:
            f.write(_csv_header_line + '\n')
            _csv_header_written = True
            
        writer = csv.writer(f, **writer_kwargs)
        for row in rows:
            writer.writerow(row)

    logging.info(f"CSV file written: {filepath} ({len(rows)} rows)")
    return filepath


def write_xml_output_fallback(elements: List[Element], filename: str) -> str:
    """ElementTree fallback writer for XML output to current subdirectory"""
    filepath = os.path.join(_current_output_subdir, filename)

    try:
        with open(filepath, 'wb') as f:
            f.write(b'<Events>\n')
            
            for elem in elements:
                xml_bytes = tostring(elem, encoding='utf-8')
                f.write(b'  ')
                f.write(xml_bytes)
                f.write(b'\n')
            
            f.write(b'</Events>\n')
            
        logging.info(f"XML file written (fallback): {filepath} ({len(elements)} elements)")
    except Exception as e:
        logging.error(f"Failed to write XML fallback file {filepath}: {e}")
        raise

    return filepath


def write_xml_output(elements: List[Element], filename: str) -> str:
    """Write XML output to current subdirectory using lxml"""
    filepath = os.path.join(_current_output_subdir, filename)

    if LET is None:
        return write_xml_output_fallback(elements, filename)

    with open(filepath, 'wb') as f:
        root = LET.Element('Events')
        for elem in elements:
            try:
                if isinstance(elem, LET._Element):
                    child = elem
                else:
                    raw = tostring(elem, encoding='utf-8')
                    child = LET.fromstring(raw)
            except Exception:
                raw = tostring(elem, encoding='utf-8')
                child = LET.fromstring(raw)
            root.append(child)
        
        try:
            xml_bytes = LET.tostring(root, encoding='utf-8', xml_declaration=False, pretty_print=False)
            f.write(xml_bytes)
            f.write(b'\n')
        except Exception as e:
            logging.error(f"Failed to serialize XML with lxml: {e}; falling back to ElementTree writer.")
            f.write(b'<Events>\n')
            for elem in elements:
                xml_bytes = tostring(elem, encoding='utf-8')
                f.write(b'  ')
                f.write(xml_bytes)
                f.write(b'\n')
            f.write(b'</Events>\n')

    logging.info(f"XML file written: {filepath} ({len(elements)} elements)")
    return filepath


# Buffer management
def flush_buffer(format_type: str) -> List[str]:
    global _json_buffer, _csv_buffer, _xml_buffer, _jsonl_buffer
    global _buffer_tokens, _file_counter

    created = []

    # Get timestamp when actually writing the file
    timestamp = get_current_timestamp()

    if format_type == 'csv' and _csv_buffer:
        filename = get_output_filename('csv', _file_counter)
        # Update subdirectory name with current timestamp if this is first file
        if _file_counter == 1:
            update_subdir_with_timestamp(format_type, timestamp)
        path = write_csv_output(_csv_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _csv_buffer = []

    elif format_type == 'json' and _json_buffer:
        filename = get_output_filename('json', _file_counter)
        if _file_counter == 1:
            update_subdir_with_timestamp(format_type, timestamp)
        path = write_json_output(_json_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _json_buffer = []

    elif format_type == 'jsonl' and _jsonl_buffer:
        filename = get_output_filename('jsonl', _file_counter)
        if _file_counter == 1:
            update_subdir_with_timestamp(format_type, timestamp)
        path = write_jsonl_output(_jsonl_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _jsonl_buffer = []

    elif format_type == 'xml' and _xml_buffer:
        filename = get_output_filename('xml', _file_counter)
        if _file_counter == 1:
            update_subdir_with_timestamp(format_type, timestamp)
        path = write_xml_output(_xml_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _xml_buffer = []

    _buffer_tokens = 0
    return created


def add_to_buffer(item: Any, format_type: str):
    """Add item to buffer with proper threshold handling"""
    global _json_buffer, _csv_buffer, _xml_buffer, _jsonl_buffer
    global _buffer_tokens, _current_format, _file_counter

    if _current_format and _current_format != format_type and _buffer_tokens > 0:
        logging.info(f"Format changed from {_current_format} to {format_type}, flushing old buffer")
        flush_buffer(_current_format)

    _current_format = format_type

    # Calculate tokens for the new item
    if format_type == 'json':
        item_tokens = count_tokens(item)
    elif format_type == 'jsonl':
        item_tokens = count_tokens(item)
    elif format_type == 'csv':
        row_text = ", ".join(item)
        item_tokens = len(tokenize_text(row_text))
    elif format_type == 'xml':
        elem_str = tostring(item, encoding='unicode')
        item_tokens = len(tokenize_text(elem_str))
    else:
        return

    # Check if single item exceeds threshold
    if item_tokens >= TOKEN_THRESHOLD:
        logging.warning(f"Single {format_type} item has {item_tokens} tokens (>= {TOKEN_THRESHOLD}). "
                       f"Flushing current buffer and writing item as standalone file.")
        
        if _buffer_tokens > 0:
            flush_buffer(format_type)
        
        # Write this large item as its own file immediately
        if format_type == 'json':
            filename = get_output_filename('json', _file_counter)
            path = write_json_output([item], filename)
            logging.info(f"Wrote large JSON item ({item_tokens} tokens) to: {path}")
        elif format_type == 'jsonl':
            filename = get_output_filename('jsonl', _file_counter)
            path = write_jsonl_output([item], filename)
            logging.info(f"Wrote large JSONL item ({item_tokens} tokens) to: {path}")
        elif format_type == 'csv':
            filename = get_output_filename('csv', _file_counter)
            path = write_csv_output([item], filename)
            logging.info(f"Wrote large CSV row ({item_tokens} tokens) to: {path}")
        elif format_type == 'xml':
            filename = get_output_filename('xml', _file_counter)
            path = write_xml_output([item], filename)
            logging.info(f"Wrote large XML element ({item_tokens} tokens) to: {path}")
        
        _file_counter += 1
        return

    # Normal case: add to buffer
    if format_type == 'json':
        _json_buffer.append(item)
    elif format_type == 'jsonl':
        _jsonl_buffer.append(item)
    elif format_type == 'csv':
        _csv_buffer.append(item)
    elif format_type == 'xml':
        _xml_buffer.append(item)

    _buffer_tokens += item_tokens
    logging.debug(f"Added {item_tokens} tokens to {format_type} buffer (total: {_buffer_tokens})")

    if _buffer_tokens >= TOKEN_THRESHOLD:
        logging.info(f"Buffer threshold reached ({_buffer_tokens} >= {TOKEN_THRESHOLD}), flushing...")
        created = flush_buffer(format_type)
        for p in created:
            logging.info(f"Created file: {p}")


# File extension helper
def get_file_extension(object_name: str) -> str:
    return os.path.splitext(object_name)[1].lower()


# Streaming functions (same as before, no changes needed)
def stream_json_with_ijson(client: Minio, object_name: str) -> bool:
    """Stream JSON with ijson, preserving structure"""
    got_any = False
    try:
        obj = client.get_object(BUCKET_NAME, object_name)
    except Exception as e:
        logging.error(f"Failed to get object {object_name}: {e}")
        return False

    prefix_bytes = b""
    try:
        prefix_bytes = obj.read(8192)
    except Exception as e:
        logging.error(f"Error reading prefix of {object_name}: {e}")
        try:
            obj.close()
            obj.release_conn()
        except Exception:
            pass
        return False

    class PrefixedStream:
        def __init__(self, prefix: bytes, obj):
            self._prefix = prefix
            self._obj = obj
            self._prefix_consumed = False

        def read(self, n=-1):
            if not self._prefix_consumed:
                if n == -1:
                    data = self._prefix + self._obj.read()
                    self._prefix_consumed = True
                    return data
                else:
                    if len(self._prefix) <= n:
                        data = self._prefix
                        self._prefix = b""
                        self._prefix_consumed = True
                        rest = self._obj.read(n - len(data))
                        return data + rest
                    else:
                        data = self._prefix[:n]
                        self._prefix = self._prefix[n:]
                        return data
            else:
                return self._obj.read(n)

        def close(self):
            try:
                self._obj.close()
            except Exception:
                pass

    stream = PrefixedStream(prefix_bytes, obj)

    try:
        start_char = None
        decoded_prefix = prefix_bytes.decode('utf-8', errors='ignore')
        
        lines = decoded_prefix.strip().split('\n')
        is_jsonl = False
        
        if len(lines) > 1:
            non_empty_lines = [ln.strip() for ln in lines if ln.strip()]
            if len(non_empty_lines) >= 2:
                if non_empty_lines[0].startswith('{') and non_empty_lines[1].startswith('{'):
                    is_jsonl = True
                    logging.info(f"Detected JSONL format in {object_name}")
        
        if not is_jsonl:
            for ch in decoded_prefix:
                if not ch.isspace():
                    start_char = ch
                    break
    except Exception:
        start_char = None
        is_jsonl = False

    try:
        if is_jsonl:
            stream.close()
            try:
                obj.close()
                obj.release_conn()
            except Exception:
                pass
            return stream_jsonl(client, object_name)
            
        elif start_char == '[':
            logging.info(f"Detected JSON array in {object_name}")
            for item in ijson.items(stream, ''):
                add_to_buffer(item, 'json')
                got_any = True

        elif start_char == '{':
            logging.info(f"Detected single JSON object in {object_name}")
            full_obj = {}
            for key, value in ijson.kvitems(stream, ''):
                full_obj[key] = value
            if full_obj:
                add_to_buffer(full_obj, 'json')
                got_any = True

        else:
            logging.error(f"Unknown or non-JSON leading character for {object_name}; skipping.")
            return False

    except Exception as e:
        logging.error(f"Error while streaming JSON for {object_name}: {e}")
    finally:
        try:
            stream.close()
        except Exception:
            pass
        try:
            obj.close()
        except Exception:
            pass
        try:
            obj.release_conn()
        except Exception:
            pass

    return got_any


def stream_jsonl(client: Minio, object_name: str) -> bool:
    got_any = False
    try:
        obj = client.get_object(BUCKET_NAME, object_name)
    except Exception as e:
        logging.error(f"Failed to get object {object_name}: {e}")
        return False

    decoder = codecs.getincrementaldecoder('utf-8')()
    line_buf = ""
    try:
        while True:
            chunk = obj.read(64 * 1024)
            if not chunk:
                break
            try:
                s = decoder.decode(chunk)
            except Exception:
                s = chunk.decode('utf-8', errors='replace')
            line_buf += s
            lines = line_buf.splitlines(keepends=True)
            if lines and not lines[-1].endswith('\n'):
                line_buf = lines[-1]
                lines = lines[:-1]
            else:
                line_buf = ""
            for ln in lines:
                ln_text = ln.strip()
                if not ln_text:
                    continue
                try:
                    parsed = json.loads(ln_text)
                    add_to_buffer(parsed, 'jsonl')
                    got_any = True
                except Exception:
                    pass

        if line_buf.strip():
            try:
                parsed = json.loads(line_buf.strip())
                add_to_buffer(parsed, 'jsonl')
                got_any = True
            except Exception:
                pass

    except Exception as e:
        logging.error(f"Error streaming JSONL {object_name}: {e}")
    finally:
        try:
            obj.close()
        except Exception:
            pass
        try:
            obj.release_conn()
        except Exception:
            pass
    return got_any


def stream_csv(client: Minio, object_name: str) -> bool:
    """Stream CSV preserving header verbatim and row boundaries"""
    got_any = False
    obj = None

    global _csv_header_line, _csv_header, _csv_dialect

    try:
        obj = client.get_object(BUCKET_NAME, object_name)
        buffered = io.BufferedReader(obj)
        text_stream = io.TextIOWrapper(buffered, encoding='utf-8', errors='replace', newline='')

        try:
            _csv_header_line = None
            _csv_header = None
            _csv_dialect = None

            while True:
                header_line = text_stream.readline()
                if not header_line:
                    break
                if header_line.strip() == '':
                    continue
                _csv_header_line = header_line.rstrip('\r\n')
                try:
                    _csv_dialect = csv.Sniffer().sniff(_csv_header_line)
                except Exception:
                    _csv_dialect = None
                break

            if _csv_dialect:
                reader = csv.reader(text_stream, dialect=_csv_dialect)
            else:
                reader = csv.reader(text_stream)

            for row in reader:
                if not row:
                    continue
                if _csv_header is None:
                    _csv_header = row
                    continue
                add_to_buffer(row, 'csv')
                got_any = True
        except Exception as e_inner:
            logging.error(f"CSV reader failed while iterating {object_name}: {e_inner}")
            raise
        finally:
            try:
                text_stream.close()
            except Exception:
                pass

    except Exception as e:
        logging.error(f"CSV streaming failed for {object_name}: {e}")
        if obj:
            try:
                obj.close()
            except Exception:
                pass
            try:
                obj.release_conn()
            except Exception:
                pass
            obj = None

        # Fallback
        try:
            obj = client.get_object(BUCKET_NAME, object_name)
            decoder = codecs.getincrementaldecoder('utf-8')()
            line_buf = ""
            _csv_header_line = None
            _csv_header = None
            _csv_dialect = None

            while True:
                chunk = obj.read(64 * 1024)
                if not chunk:
                    break
                try:
                    s = decoder.decode(chunk)
                except Exception:
                    s = chunk.decode('utf-8', errors='replace')
                line_buf += s
                lines = line_buf.splitlines(keepends=True)
                if lines and not lines[-1].endswith('\n'):
                    line_buf = lines[-1]
                    lines = lines[:-1]
                else:
                    line_buf = ""

                for ln in lines:
                    ln_text = ln.rstrip('\r\n')
                    if not ln_text:
                        continue
                    if _csv_header_line is None:
                        _csv_header_line = ln_text
                        try:
                            parsed = next(csv.reader([ln_text]))
                            _csv_header = parsed
                        except Exception:
                            _csv_header = [ln_text]
                        continue
                    try:
                        row = next(csv.reader([ln_text]))
                        add_to_buffer(row, 'csv')
                        got_any = True
                    except Exception:
                        add_to_buffer([ln_text], 'csv')
                        got_any = True

            if line_buf.strip():
                leftover = line_buf.strip()
                if _csv_header_line is None:
                    _csv_header_line = leftover
                    try:
                        parsed = next(csv.reader([leftover]))
                        _csv_header = parsed
                    except Exception:
                        _csv_header = [leftover]
                else:
                    try:
                        row = next(csv.reader([leftover]))
                        add_to_buffer(row, 'csv')
                        got_any = True
                    except Exception:
                        add_to_buffer([leftover], 'csv')
                        got_any = True

        except Exception as e2:
            logging.error(f"CSV fallback streaming also failed for {object_name}: {e2}")

    finally:
        if obj:
            try:
                obj.close()
            except Exception:
                pass
            try:
                obj.release_conn()
            except Exception:
                pass

    return got_any


def stream_xml(client: Minio, object_name: str) -> bool:
    """Stream EVTX XML by extracting <Event>...</Event> blocks"""
    got_any = False

    try:
        obj = client.get_object(BUCKET_NAME, object_name)
    except Exception as e:
        logging.error(f"Failed to get object {object_name}: {e}")
        return False

    decoder = codecs.getincrementaldecoder("utf-8")()
    buffer = ""

    try:
        while True:
            chunk = obj.read(64 * 1024)
            if not chunk:
                break

            try:
                text = decoder.decode(chunk)
            except Exception:
                text = chunk.decode("utf-8", errors="replace")

            buffer += text

            while True:
                start = buffer.find("<Event")
                if start == -1:
                    break

                end = buffer.find("</Event>", start)
                if end == -1:
                    break

                event_xml = buffer[start:end + len("</Event>")]
                buffer = buffer[end + len("</Event>"):]

                try:
                    elem = LET.fromstring(event_xml.encode("utf-8"))
                    add_to_buffer(elem, "xml")
                    got_any = True
                except Exception as e:
                    logging.warning(f"Failed to parse Event XML block: {e}")

    except Exception as e:
        logging.error(f"Error streaming XML {object_name}: {e}")

    finally:
        try:
            obj.close()
        except Exception:
            pass
        try:
            obj.release_conn()
        except Exception:
            pass

    return got_any


# Top-level processing - MODIFIED
def stream_and_process_object(client: Minio, object_name: str) -> bool:
    """Detect extension and process accordingly"""
    ext = get_file_extension(object_name).lower()

    if ext == '.jsonl':
        return stream_jsonl(client, object_name)
    elif ext == '.json':
        try:
            result = stream_json_with_ijson(client, object_name)
            if not result:
                logging.error(f"Failed to parse {object_name} as JSON with ijson.")
            return result
        except Exception as e:
            logging.error(f"ijson failed for {object_name}: {e}")
            return False
    elif ext == '.xml':
        return stream_xml(client, object_name)
    elif ext == '.csv':
        return stream_csv(client, object_name)
    else:
        try:
            obj = client.get_object(BUCKET_NAME, object_name)
            prefix = obj.read(4096)
            s = prefix.decode('utf-8', errors='ignore').lstrip()
            obj.close()
            obj.release_conn()

            if s.startswith('{') or s.startswith('['):
                return stream_json_with_ijson(client, object_name)
            elif s.startswith('<'):
                return stream_xml(client, object_name)
            else:
                return stream_csv(client, object_name)
        except Exception:
            return False


def process_minio_record(client: Minio, record: dict):
    """Process MinIO record - MODIFIED to create subdirectory per input file"""
    global _current_object_base, _buffer_tokens, _current_format
    global _csv_header, _csv_header_line, _csv_dialect, _csv_header_written
    global _current_output_subdir, _file_counter

    try:
        key = record["s3"]["object"]["key"]
    except Exception:
        logging.warning("Record missing expected key structure; skipping")
        return
    key = unquote_plus(key)

    if is_processed(key):
        logging.debug(f"Already processed: {key}; skipping")
        return

    logging.info(f"Processing new object: {key}")
    try:
        # Reset state for new file
        _current_object_base = os.path.splitext(os.path.basename(key))[0]
        _csv_header_written = False
        _file_counter = 1  # Reset counter for each input file
        
        # Detect format
        ext = get_file_extension(key).lower()
        if ext == '.json':
            format_type = 'json'
        elif ext == '.jsonl':
            format_type = 'jsonl'
        elif ext == '.csv':
            format_type = 'csv'
        elif ext == '.xml':
            format_type = 'xml'
        else:
            format_type = 'unknown'
        
        # Create subdirectory for this input file
        _current_output_subdir = create_output_subdir_initial(_current_object_base, format_type)

        # Process the file
        produced = stream_and_process_object(client, key)
        if not produced:
            logging.info(f"No tokens produced for {key}")

        # Flush remaining buffer
        if _buffer_tokens > 0 and _current_format:
            logging.info(f"Object processing complete for {key}; flushing remaining buffer ({_buffer_tokens} tokens)")
            created = flush_buffer(_current_format)
            for p in created:
                logging.info(f"Created file: {p}")

        etag = record.get("s3", {}).get("object", {}).get("eTag")
        lm = record.get("eventTime") or record.get("s3", {}).get("object", {}).get("lastModified")
        mark_processed(key, etag, lm)

    except Exception as e:
        logging.error(f"Error processing object {key}: {e}")
    finally:
        _csv_header = None
        _csv_header_line = None
        _csv_dialect = None
        _csv_header_written = False
        _current_object_base = None
        _current_output_subdir = None

def poll_and_process(client: Minio):
    try:
        objects = client.list_objects(BUCKET_NAME, recursive=True)
        for obj in objects:
            key = unquote_plus(obj.object_name)
            if not is_processed(key):
                record = {
                    "s3": {
                        "object": {
                            "key": key,
                            "eTag": getattr(obj, 'etag', None),
                            "lastModified": getattr(obj, 'last_modified', None)
                        }
                    }
                }
                process_minio_record(client, record)
    except Exception as e:
        logging.error(f"Polling error: {e}")


def listen_loop():
    global _running, _current_format
    client = create_minio_client()
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            logging.info(f"Created bucket '{BUCKET_NAME}'")
    except Exception as e:
        logging.error(f"Bucket check/create error: {e}")

    while _running:
        try:
            logging.info(f"Listening for notifications on bucket '{BUCKET_NAME}'")
            with client.listen_bucket_notification(BUCKET_NAME, events=["s3:ObjectCreated:*"]) as events:
                for ev in events:
                    if not _running:
                        break
                    if isinstance(ev, dict) and "Records" in ev:
                        for r in ev["Records"]:
                            process_minio_record(client, r)
                    elif isinstance(ev, dict):
                        if "s3" in ev:
                            process_minio_record(client, ev)
        except Exception as e:
            logging.warning(f"Notifications unavailable: {e}. Falling back to polling.")
            try:
                while _running:
                    poll_and_process(client)
                    # Flush remaining buffer at end of poll cycle
                    if _buffer_tokens > 0 and _current_format:
                        logging.info(f"End of poll cycle, flushing remaining buffer ({_buffer_tokens} tokens)")
                        created = flush_buffer(_current_format)
                        for p in created:
                            logging.info(f"Created file: {p}")
                    time.sleep(POLL_INTERVAL)
            except Exception as e2:
                logging.error(f"Polling loop error: {e2}")
                time.sleep(5)
        if _running:
            time.sleep(1)