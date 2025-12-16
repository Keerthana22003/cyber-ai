import os
import csv
import json
import time
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from langchain_qdrant import QdrantVectorStore
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage
from langchain_huggingface import HuggingFaceEmbeddings
from qdrant_client import QdrantClient, models
from grpc import RpcError, StatusCode
from dotenv import load_dotenv

load_dotenv()

# ======================================================
# CONFIG
# ======================================================

TEMP_DIR = ".\processed_tokens"     
OUTPUT_DIR = "./reports"       

QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_URL = os.getenv("QDRANT_ENDPOINT")
QDRANT_COLLECTION = os.getenv("COLLECTION_NAME")

llm = AzureChatOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
    temperature=0
)
 
 
embeddings = HuggingFaceEmbeddings(
    model_name=os.getenv("SENTENCE_TRANSFORMER_MODEL", "all-MiniLM-L6-v2"),
    cache_folder="hf_cache"
)

def get_qdrant_client():
    return QdrantClient(api_key=QDRANT_API_KEY, host=QDRANT_URL, port=6334, prefer_grpc=True, timeout=60.0)

# ======================================================
# QDRANT RETRIEVER (READ ONLY)
# ======================================================

def get_retriever():
    qdrant_client = get_qdrant_client()
    vectorstore = QdrantVectorStore(
        client=qdrant_client,
        collection_name=QDRANT_COLLECTION,
        embedding=embeddings,
        content_payload_key="page_content",
    )
    return vectorstore.as_retriever(search_kwargs={"k": 5})

# ======================================================
# -------- UNIT EXTRACTION (FORMAT AWARE) --------------
# ======================================================

def csv_units(path: str) -> Iterable[str]:
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            yield " | ".join(row)

def json_units(path: str) -> Iterable[str]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        for obj in data:
            yield json.dumps(obj, ensure_ascii=False)
    elif isinstance(data, dict):
        for k, v in data.items():
            yield f"{k}: {json.dumps(v, ensure_ascii=False)}"

def wait_for_file_ready(path, timeout=10):
    start = time.time()
    last_size = -1

    while time.time() - start < timeout:
        if not os.path.exists(path):
            time.sleep(0.1)
            continue

        size = os.path.getsize(path)
        if size > 0 and size == last_size:
            return True  # stable

        last_size = size
        time.sleep(0.2)

    return False

def xml_units(path: str) -> Iterable[str]:
    if not wait_for_file_ready(path):
        print(f"[WARN] XML not ready, skipping: {path}")
        return []

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as e:
        print(f"[WARN] Invalid XML skipped: {path} → {e}")
        return []

    record = {}
    for elem in root.iter():
        if elem.text and elem.text.strip():
            record[elem.tag] = elem.text.strip()

    if record:
        yield json.dumps(record, ensure_ascii=False)

def extract_units(path: str, file_type: str) -> Iterable[str]:
    if file_type == "csv":
        return csv_units(path)
    if file_type == "json":
        return json_units(path)
    if file_type == "xml":
        units = xml_units(path)
        print(units)
        return units
    return []

# ======================================================
# -------- GENAI + QDRANT RETRIEVAL --------------------
# ======================================================

def llm_validate_ttp(log_unit: str, kb_docs: List) -> List[Dict]:
    context = "\n".join([
        f"Tactic: {d.metadata.get('tactic')}, "
        f"Technique: {d.metadata.get('technique')}\n"
        f"KB Content: {d.page_content}"
        for d in kb_docs
    ])

    prompt = f"""
You are a MITRE ATT&CK expert.

LOG EVENT:
{log_unit}

CANDIDATE KB MATCHES:
{context}

TASK:
Select ONLY the tactics and techniques that clearly apply.
Do not invent new tactics.

Return strict JSON:
{{
  "matches": [
    {{
      "tactic": "...",
      "techniques": ["..."]
    }}
  ]
}}
"""

    response = llm.invoke([HumanMessage(content=prompt)])
    return json.loads(response.content).get("matches", [])

def retrieve_ttp(log_unit: str, retriever):
    try:
        docs = retriever.invoke(log_unit)
    except RpcError as e:
        code = e.code()
        logging.error(f"RPC Error: {code}")

        # DEADLINE_EXCEEDED or UNAVAILABLE → reconnect
        if code in (StatusCode.DEADLINE_EXCEEDED, StatusCode.UNAVAILABLE):
            logging.info("Reconnecting to Qdrant...")
            time.sleep(2) 
            retriever = get_retriever()
            docs = retriever.invoke(log_unit)
    except Exception as e:
        logging.error(f"Unexpected Error: {e}")        

    return docs

# ======================================================
# -------- FILE LEVEL AGGREGATION ----------------------
# ======================================================

def process_file_folder(folder_path: str) -> Dict:
    retriever = get_retriever()
    source_file = os.path.basename(folder_path)
    print(f"Source File: {source_file}, path: {folder_path}")
    file_type = source_file.split(".")[-1].lower()

    aggregation = {
        "source_file": source_file,
        "file_type": file_type,
        "chunks_processed": 0,
        "units_analyzed": 0,
        "tactics": {}
    }

    print("Aggregation initials ", aggregation)

    for file in os.listdir(folder_path):
        

        aggregation["chunks_processed"] += 1
        chunk_path = os.path.join(folder_path, file)
        file_type = chunk_path.split(".")[-1].lower()

        for unit in extract_units(chunk_path, file_type):
            aggregation["units_analyzed"] += 1
            matches = retrieve_ttp(unit, retriever)

            for m in matches:
                tactic = m.metadata.get("tactic")
                techniques = m.metadata.get("technique", [])

                aggregation["tactics"].setdefault(
                    tactic,
                    {"techniques": set(), "unit_count": 0}
                )
                if isinstance(techniques, str):
                    aggregation["tactics"][tactic]["techniques"].add(techniques)
                else:
                    aggregation["tactics"][tactic]["techniques"].update(techniques)
                aggregation["tactics"][tactic]["unit_count"] += 1
        
        print(f"Aggregation after process: {aggregation}")

    return aggregation

# ======================================================
# -------- GENAI REPORT GENERATION ---------------------
# ======================================================

def generate_llm_report(aggregation: Dict) -> Dict:
    tactics_summary = [
        {
            "tactic": t,
            "techniques": list(v["techniques"]),
            "evidence_count": v["unit_count"]
        }
        for t, v in aggregation["tactics"].items()
    ]

    prompt = f"""
You are a senior SOC analyst.

Generate a threat intelligence report.

INPUT DATA:
{json.dumps(tactics_summary, indent=2)}
Respond with STRICT JSON ONLY.
No markdown.
No commentary.
No explanations.
OUTPUT FORMAT :
{{
  "executive_summary": "...",
  "threats": [
    {{
      "tactic": "...",
      "observed_behavior": "...",
      "severity": "Low | Medium | High",
      "recommended_actions": []
    }}
  ]
}}
"""

    response = llm.invoke([HumanMessage(content=prompt)])
 
    raw = response.content.strip()
 
    if not raw:
        raise RuntimeError("LLM returned empty response")
 
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(" LLM returned invalid JSON:")
        raise
# ======================================================
# -------- WATCHDOG HANDLER ----------------------------
# ======================================================

class CompletionHandler(FileSystemEventHandler):

    def on_created(self, event):
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == "_DONE.txt":
            self.process(os.path.dirname(event.src_path))

    def process(self, folder_path):
        flag = os.path.join(folder_path, ".processed")
        if os.path.exists(flag):
            return
        

        print(f"Processing: {os.path.basename(folder_path)}")

        aggregation = process_file_folder(folder_path)
        llm_report = generate_llm_report(aggregation)
 
        final_report = {
            "metadata": {
                "source_file": aggregation["source_file"],
                "file_type": aggregation["file_type"],
                "chunks_processed": aggregation["chunks_processed"],
                "units_analyzed": aggregation["units_analyzed"],
                "tactics": [
                    {
                        "tactic": t,
                        "techniques": list(v["techniques"]),
                        "unit_count": v["unit_count"]
                    }
                    for t, v in aggregation["tactics"].items()
                ]
            },
            "analysis": llm_report
        }
 
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(
            OUTPUT_DIR,
            f"{aggregation['source_file']}.report.json"
        )
 
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_report, f, indent=2)
 
        open(flag, "w").close()
        print(f"Report generated → {output_path}")

# ======================================================
# -------- WATCHER RUNNER ------------------------------
# ======================================================

# def start_watcher():
#     os.makedirs(TEMP_DIR, exist_ok=True)
#     os.makedirs(OUTPUT_DIR, exist_ok=True)

#     observer = Observer()
#     observer.schedule(CompletionHandler(), TEMP_DIR, recursive=True)
#     observer.start()

#     print(f"Watching {TEMP_DIR} for completed tokenized files")

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()

#     observer.join()

# ======================================================
# ENTRY POINT
# ======================================================

# if __name__ == "__main__":
#     start_watcher()

