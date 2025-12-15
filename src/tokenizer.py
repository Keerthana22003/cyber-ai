#!/usr/bin/env python3
"""
MinIO Tokenizer Module - Format-Preserving Version (Complete)

Features:
- Streams JSON / JSONL / CSV / XML from MinIO
- Tokenizes items and writes format-preserving output files
- Token limit per output file = 10000 (default, overridable by TOKEN_THRESHOLD env var)
- Splits only on syntactic boundaries (JSON items, JSONL lines, CSV rows, XML top-level elements)
- If a single syntactic item > threshold, it is written as its own file (no mid-item split)
- CSV: captures raw header line verbatim and writes it once per output file
- XML: buffers full top-level elements and writes single-element file as-is (preserve namespaces)
- Output filenames include the input filename base for traceability
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
# Enforce default token threshold 10000
TOKEN_THRESHOLD = int(os.getenv("TOKEN_THRESHOLD", "10000"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
SQLITE_DB = os.getenv("SQLITE_DB", "./processed_objects.db")

# Internal state - format-specific buffers
_json_buffer: List[Dict] = []
_csv_buffer: List[List[str]] = []
_xml_buffer: List[Element] = []
_jsonl_buffer: List[Dict] = []

_buffer_tokens = 0
_file_counter = 1
_running = True
_db_conn = None
_current_format = None  # 'json', 'csv', 'xml', 'jsonl'
_current_object_base: Optional[str] = None  # base name of current input object (no ext)

# CSV preservation state (per-file)
_csv_header: Optional[List[str]] = None         # parsed header fields (list)
_csv_header_line: Optional[str] = None         # raw header line (verbatim)
_csv_dialect = None                             # csv.Dialect or None
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


# Output directory management
def ensure_output_dir():
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)


def get_output_filename(format_type: str, counter: int) -> str:
    """Generate filename using current input basename if available."""
    timestamp = int(time.time())
    extensions = {
        'json': '.json',
        'jsonl': '.jsonl',
        'csv': '.csv',
        'xml': '.xml'
    }
    ext = extensions.get(format_type, '.txt')
    base = _current_object_base if _current_object_base else None
    if base:
        safe_base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
        return f"tokens_output_{safe_base}_{format_type}_{counter:03d}_{timestamp}{ext}"
    else:
        return f"tokens_output_{format_type}_{counter:03d}_{timestamp}{ext}"


# Format-specific output writers
def write_json_output(items: List[Dict], filename: str) -> str:
    ensure_output_dir()
    filepath = os.path.join(LOCAL_OUTPUT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(items, f, separators=(',', ':'), ensure_ascii=False)
    logging.info(f"JSON file written: {filepath} ({len(items)} items)")
    return filepath


def write_jsonl_output(items: List[Dict], filename: str) -> str:
    ensure_output_dir()
    filepath = os.path.join(LOCAL_OUTPUT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        for item in items:
            f.write(json.dumps(item, separators=(',', ':'), ensure_ascii=False) + '\n')
    logging.info(f"JSONL file written: {filepath} ({len(items)} lines)")
    return filepath


def write_csv_output(rows: List[List[str]], filename: str) -> str:
    """Write CSV: write raw header line verbatim only if not yet written for this input file."""
    ensure_output_dir()
    filepath = os.path.join(LOCAL_OUTPUT_DIR, filename)
    global _csv_header_line, _csv_dialect, _csv_header_written

    writer_kwargs = {}
    try:
        if _csv_dialect:
            writer_kwargs['dialect'] = _csv_dialect
    except Exception:
        writer_kwargs = {}

    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        # Only write header if it hasn't been written yet for this input file
        if _csv_header_line is not None and not _csv_header_written:
            f.write(_csv_header_line + '\n')
            _csv_header_written = True  # Mark as written
            
        writer = csv.writer(f, **writer_kwargs)
        for row in rows:
            writer.writerow(row)

    logging.info(f"CSV file written: {filepath} ({len(rows)} rows)")
    return filepath

def stream_xml_fallback(client: Minio, object_name: str) -> bool:
    """Fallback XML streaming using xml.etree.ElementTree.XMLPullParser.

    - Buffers full top-level elements (preserve nesting & namespaces as best as ElementTree can).
    - Adds deep-copied top-level elements to _xml_buffer via add_to_buffer(..., 'xml').
    """
    got_any = False
    try:
        obj = client.get_object(BUCKET_NAME, object_name)
    except Exception as e:
        logging.error(f"Failed to get object {object_name}: {e}")
        return False

    # pull parser needs to see start and end events to detect full top-level elements
    parser = XMLPullParser(events=('start', 'end'))
    decoder = codecs.getincrementaldecoder('utf-8')()
    root_tag = None

    try:
        while True:
            chunk = obj.read(64 * 1024)
            if not chunk:
                break
            try:
                text_chunk = decoder.decode(chunk)
            except Exception:
                text_chunk = chunk.decode('utf-8', errors='replace')
            parser.feed(text_chunk)

            for event, elem in parser.read_events():
                if event == 'start':
                    if root_tag is None:
                        # first start event -> this is the top-level element tag
                        root_tag = elem.tag
                elif event == 'end':
                    if elem.tag == root_tag:
                        # finished a full top-level element/document
                        import copy
                        elem_copy = copy.deepcopy(elem)
                        add_to_buffer(elem_copy, 'xml')
                        got_any = True
                        # clear to free parser memory
                        elem.clear()
                        # reset to capture next top-level element if any
                        root_tag = None

    except Exception as e:
        logging.error(f"Error streaming XML (fallback) for {object_name}: {e}")
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

def write_xml_output_fallback(elements: List[Element], filename: str) -> str:
    """ElementTree fallback writer for XML output.

    - Single element -> write as full XML document (with declaration).
    - Multiple elements -> wrap under <root> to ensure well-formed XML.
    """
    ensure_output_dir()
    filepath = os.path.join(LOCAL_OUTPUT_DIR, filename)

    try:
        with open(filepath, 'wb') as f:
            if len(elements) == 1:
                # Single element: write declaration + element bytes
                f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
                xml_bytes = tostring(elements[0], encoding='utf-8')
                f.write(xml_bytes)
                f.write(b'\n')
            else:
                # Multiple elements: wrap under <root>
                f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<root>\n')
                for elem in elements:
                    xml_bytes = tostring(elem, encoding='utf-8')
                    f.write(b'  ')
                    f.write(xml_bytes)
                    f.write(b'\n')
                f.write(b'</root>\n')
        logging.info(f"XML file written (fallback): {filepath} ({len(elements)} elements)")
    except Exception as e:
        logging.error(f"Failed to write XML fallback file {filepath}: {e}")
        raise

    return filepath


def write_xml_output(elements: List[Element], filename: str) -> str:
    """Write XML output using lxml to preserve original namespace prefixes.

    If exactly one element, writes that element as the full XML document with declaration.
    If multiple elements, wraps under <root> while preserving children's namespace maps.
    """
    ensure_output_dir()
    filepath = os.path.join(LOCAL_OUTPUT_DIR, filename)

    if LET is None:
        # Fallback: use the previous ElementTree writer
        return write_xml_output_fallback(elements, filename)

    # Use lxml serialization
    with open(filepath, 'wb') as f:
        if len(elements) == 1:
            # Single element -> write as full document preserving prefixes & nsmap
            try:
                # elements[0] may be either lxml element or ElementTree element.
                # If it's an ElementTree element, convert to bytes then to lxml object.
                if not isinstance(elements[0], LET._Element):
                    # convert via tostring then parse back to lxml to ensure nsmap preserved
                    raw = tostring(elements[0], encoding='utf-8')
                    lxml_elem = LET.fromstring(raw)
                else:
                    lxml_elem = elements[0]

                xml_bytes = LET.tostring(lxml_elem, encoding='utf-8', xml_declaration=False, pretty_print=False)
                f.write(xml_bytes)
                f.write(b'\n')
            except Exception as e:
                logging.error(f"Failed to serialize single XML element with lxml: {e}; falling back to ElementTree writer.")
                # fallback to previous behavior
                f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
                xml_bytes = tostring(elements[0], encoding='utf-8')
                f.write(xml_bytes)
                f.write(b'\n')
        else:
            # Multiple elements: create a new lxml root and append copies (preserving nsmap on children)
            root = LET.Element('root')
            for elem in elements:
                try:
                    if isinstance(elem, LET._Element):
                        child = copy.deepcopy(elem)
                    else:
                        # convert ElementTree to lxml element
                        raw = tostring(elem, encoding='utf-8')
                        child = LET.fromstring(raw)
                except Exception:
                    # last resort: parse via ElementTree serialization
                    raw = tostring(elem, encoding='utf-8')
                    child = LET.fromstring(raw)
                root.append(child)
            try:
                xml_bytes = LET.tostring(root, encoding='utf-8', xml_declaration=False, pretty_print=False)
                f.write(xml_bytes)
                f.write(b'\n')
            except Exception as e:
                logging.error(f"Failed to serialize multi-element XML with lxml: {e}; falling back to ElementTree writer.")
                # fallback to ElementTree style writer
                f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<root>\n')
                for elem in elements:
                    xml_bytes = tostring(elem, encoding='utf-8')
                    f.write(b'  ')
                    f.write(xml_bytes)
                    f.write(b'\n')
                f.write(b'</root>\n')

    logging.info(f"XML file written: {filepath} ({len(elements)} elements)")
    return filepath



# Buffer management (flush only at item boundaries)
def flush_buffer(format_type: str) -> List[str]:
    global _json_buffer, _csv_buffer, _xml_buffer, _jsonl_buffer
    global _buffer_tokens, _file_counter

    created = []

    if format_type == 'csv' and _csv_buffer:
        # Simple case: flush everything in buffer to one file
        # Since add_to_buffer now prevents exceeding threshold,
        # buffer should always be <= threshold when we get here
        filename = get_output_filename('csv', _file_counter)
        path = write_csv_output(_csv_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _csv_buffer = []

    # Similar simplification for other formats...
    elif format_type == 'json' and _json_buffer:
        filename = get_output_filename('json', _file_counter)
        path = write_json_output(_json_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _json_buffer = []

    elif format_type == 'jsonl' and _jsonl_buffer:
        filename = get_output_filename('jsonl', _file_counter)
        path = write_jsonl_output(_jsonl_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _jsonl_buffer = []

    elif format_type == 'xml' and _xml_buffer:
        filename = get_output_filename('xml', _file_counter)
        path = write_xml_output(_xml_buffer, filename)
        created.append(path)
        logging.info(f"Flushed {_buffer_tokens} tokens to: {path}")
        _file_counter += 1
        _xml_buffer = []

    _buffer_tokens = 0
    return created


def add_to_buffer(item: Any, format_type: str):
    global _json_buffer, _csv_buffer, _xml_buffer, _jsonl_buffer
    global _buffer_tokens, _current_format

    # If switching format, flush previous
    if _current_format and _current_format != format_type and _buffer_tokens > 0:
        logging.info(f"Format changed from {_current_format} to {format_type}, flushing old buffer")
        flush_buffer(_current_format)

    _current_format = format_type

    if format_type == 'json':
        item_tokens = count_tokens(item)
        _json_buffer.append(item)
    elif format_type == 'jsonl':
        item_tokens = count_tokens(item)
        _jsonl_buffer.append(item)
    elif format_type == 'csv':
        row_text = ", ".join(item)
        item_tokens = len(tokenize_text(row_text))
        _csv_buffer.append(item)
    elif format_type == 'xml':
        elem_str = tostring(item, encoding='unicode')
        item_tokens = len(tokenize_text(elem_str))
        _xml_buffer.append(item)
    else:
        return

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


# Streaming JSON / JSONL / CSV / XML

def stream_json_with_ijson(client: Minio, object_name: str) -> bool:
    """Stream JSON with ijson, preserving structure and producing JSON-array chunk outputs.

    ENHANCED: Now detects JSONL format even in .json files and processes accordingly.
    """
    got_any = False
    try:
        obj = client.get_object(BUCKET_NAME, object_name)
    except Exception as e:
        logging.error(f"Failed to get object {object_name}: {e}")
        return False

    prefix_bytes = b""
    try:
        prefix_bytes = obj.read(8192)  # Read more to detect format better
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
        
        # Detect if this is JSONL format (multiple objects, one per line)
        # Check if we have multiple lines with JSON objects
        lines = decoded_prefix.strip().split('\n')
        is_jsonl = False
        
        if len(lines) > 1:
            # Check if first two non-empty lines both start with '{'
            non_empty_lines = [ln.strip() for ln in lines if ln.strip()]
            if len(non_empty_lines) >= 2:
                if non_empty_lines[0].startswith('{') and non_empty_lines[1].startswith('{'):
                    is_jsonl = True
                    logging.info(f"Detected JSONL format in {object_name} (even though extension is .json)")
        
        if not is_jsonl:
            # Original detection logic
            for ch in decoded_prefix:
                if not ch.isspace():
                    start_char = ch
                    break
    except Exception:
        start_char = None
        is_jsonl = False

    try:
        if is_jsonl:
            # Process as JSONL
            logging.info(f"Processing {object_name} as JSONL format")
            stream.close()
            # Re-open and use JSONL streaming
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
            # For a top-level object, collect key/value pairs into one dict
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
    """Stream CSV preserving header verbatim and row boundaries."""
    got_any = False
    obj = None

    # Declare per-file CSV globals up front to avoid Python scoping errors
    global _csv_header_line, _csv_header, _csv_dialect

    try:
        obj = client.get_object(BUCKET_NAME, object_name)
        buffered = io.BufferedReader(obj)
        text_stream = io.TextIOWrapper(buffered, encoding='utf-8', errors='replace', newline='')

        try:
            # Reset per-file CSV header state
            _csv_header_line = None
            _csv_header = None
            _csv_dialect = None

            # Read first non-empty line as header (verbatim)
            while True:
                header_line = text_stream.readline()
                if not header_line:
                    break
                if header_line.strip() == '':
                    continue
                _csv_header_line = header_line.rstrip('\r\n')
                # Try to sniff dialect; best-effort using header only
                try:
                    _csv_dialect = csv.Sniffer().sniff(_csv_header_line)
                except Exception:
                    _csv_dialect = None
                break

            # Build reader for remaining lines
            if _csv_dialect:
                reader = csv.reader(text_stream, dialect=_csv_dialect)
            else:
                reader = csv.reader(text_stream)

            for row in reader:
                if not row:
                    continue
                if _csv_header is None:
                    # first parsed row after header -> treat as header fallback if needed
                    _csv_header = row
                    # don't add header as data
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

        # Fallback incremental line approach (keeps raw header handling)
        try:
            obj = client.get_object(BUCKET_NAME, object_name)
            decoder = codecs.getincrementaldecoder('utf-8')()
            line_buf = ""
            # reset header state
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
    """Stream XML preserving full top-level elements and original namespace prefixes using lxml.

    Requires lxml (LET != None). Falls back to previous implementation if lxml unavailable.
    """
    if LET is None:
        # fallback to previous xml.etree implementation (you should keep the original as fallback)
        logging.warning("lxml not available; falling back to ElementTree-based XML streaming.")
        return stream_xml_fallback(client, object_name)  # ensure you have an ElementTree fallback defined

    got_any = False
    try:
        obj = client.get_object(BUCKET_NAME, object_name)
    except Exception as e:
        logging.error(f"Failed to get object {object_name}: {e}")
        return False

    parser = LET.XMLPullParser(events=('start', 'end'))
    decoder = codecs.getincrementaldecoder('utf-8')()
    root_tag = None

    try:
        while True:
            chunk = obj.read(64 * 1024)
            if not chunk:
                break
            try:
                text_chunk = decoder.decode(chunk)
            except Exception:
                text_chunk = chunk.decode('utf-8', errors='replace')
            # Feed bytes (lxml accepts str for feed)
            parser.feed(text_chunk)

            for event, elem in parser.read_events():
                if event == 'start':
                    if root_tag is None:
                        # store the first start element's tag (namespace-aware)
                        root_tag = elem.tag
                elif event == 'end':
                    if elem.tag == root_tag:
                        # We finished a top-level element. Copy it and buffer.
                        import copy
                        elem_copy = copy.deepcopy(elem)  # preserves nsmap and prefixes
                        add_to_buffer(elem_copy, 'xml')
                        got_any = True
                        # clear element to free memory
                        elem.clear()
                        # reset root_tag so if multiple sibling top-level elements exist we can capture them too
                        root_tag = None

    except Exception as e:
        logging.error(f"Error streaming XML (lxml) for {object_name}: {e}")
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



# Top-level processing
def stream_and_process_object(client: Minio, object_name: str) -> bool:
    """Detect extension and process accordingly (JSON files produce JSON outputs)."""
    ext = get_file_extension(object_name).lower()

    if ext == '.jsonl':
        return stream_jsonl(client, object_name)
    elif ext == '.json':
        # For .json inputs we must keep producing .json outputs (no jsonl fallback)
        try:
            result = stream_json_with_ijson(client, object_name)
            if not result:
                logging.error(f"Failed to parse {object_name} as JSON with ijson.")
            return result
        except Exception as e:
            logging.error(f"ijson failed for {object_name}: {e}. Not falling back to JSONL for .json inputs.")
            return False
    elif ext == '.xml':
        return stream_xml(client, object_name)
    elif ext == '.csv':
        return stream_csv(client, object_name)
    else:
        # Auto-detect as before, but if auto-detect finds JSON we still call stream_json_with_ijson
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
    # Declare globals up front to avoid Python scoping errors
    global _current_object_base, _buffer_tokens, _current_format
    global _csv_header, _csv_header_line, _csv_dialect, _csv_header_written

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
        # set input base for naming
        _current_object_base = os.path.splitext(os.path.basename(key))[0]
        
        # Reset header written flag for new input file
        _csv_header_written = False

        produced = stream_and_process_object(client, key)
        if not produced:
            logging.info(f"No tokens produced for {key}")

        # flush any remaining for this object immediately
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
        # clear per-file CSV header and object base state
        _csv_header = None
        _csv_header_line = None
        _csv_dialect = None
        _csv_header_written = False  # Reset for next file
        _current_object_base = None


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

