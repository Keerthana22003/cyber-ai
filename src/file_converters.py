import os
import Evtx.Evtx as evtx
import Evtx.Views as e_views
import pandas as pd
import csv
from datetime import datetime
 
OUTPUT_FILE_PATH = "..\data\processed\input"
 
def evtx_to_xml(evtx_file_path: str, output_dir=OUTPUT_FILE_PATH) -> tuple:
    """
    Convert an EVTX file into an XML string (not saved to disk)
    and return a filename ending with .xml.
    """
 
    print("Conversion Started:", datetime.now())
 
    # Output file name
    base_name = os.path.splitext(os.path.basename(evtx_file_path))[0]
    xml_file_name = f"{base_name}.xml"
 
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    xml_file_path = os.path.join(output_dir, xml_file_name)
 
    # Write XML directly to file (streamed)
    with open(xml_file_path, "w", encoding="utf-8") as xml_file:
        xml_file.write('<?xml version="1.0" encoding="utf-8"?>\n')
        xml_file.write("<Events>\n")
 
        with evtx.Evtx(evtx_file_path) as log:
            for record in log.records():
                # Write each record immediately → no huge in-memory string
                xml_file.write(record.xml())
                xml_file.write("\n")
 
        xml_file.write("</Events>")
 
    print("Conversion Ended:", datetime.now())
    print("XML file created:", xml_file_path)
 
    return xml_file_path
 
def log_to_csv(input_log_path: str, output_dir=OUTPUT_FILE_PATH) -> tuple:
    """
    Convert a .log file into CSV format IN-MEMORY without modifying or removing any content.
    and return a filename ending with .csv.
    """
    print("Conversion Started:", datetime.now())
 
    # Output CSV file path
    base_name = os.path.basename(input_log_path)
    output_csv_filename = os.path.splitext(base_name)[0] + ".csv"
 
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    output_csv_path = os.path.join(output_dir, output_csv_filename)
 
    # Open CSV writer (streaming, no memory usage)
    with open(output_csv_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["line"])  # header
 
        # Read LOG file line-by-line
        with open(input_log_path, "r", encoding="utf-8", errors="ignore") as log_file:
            for line in log_file:
                writer.writerow([line.rstrip("\n")])
 
    print("Conversion Ended:", datetime.now())
    print("CSV file created:", output_csv_path)
 
    return output_csv_path
 