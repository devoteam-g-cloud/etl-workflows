import sys
import logging
import json
import re
import os
import csv
import gzip
from typing import Dict, Iterable
from datetime import datetime

import yaml
from google.cloud import storage, bigquery
from google.cloud.bigquery.job.load import LoadJobConfig
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.enums import CreateDisposition, SourceFormat, WriteDisposition
from flask import jsonify
from werkzeug.exceptions import HTTPException

from exceptions import *


logging.root.setLevel(logging.INFO)
ENCODING = "ISO-8859-1"
SCHEMAS_BUCKET = "my-assets-bucket" # SET YOUR BUCKET NAME
ARCHIVED_SUBFOLDER = "ARCHIVED/"
DESTINATION_TABLE_REGEX = r"\{([0-9]+:[0-9]+)\}"
INTEGER_TYPE = "INTEGER"
FLOAT_TYPE = "FLOAT"
TIMESTAMP_TYPE = "TIMESTAMP"
DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y%m%d"
]
gs = storage.Client()
bq = bigquery.Client()


def execute_job(bucket_name: str, prefix: str, schema_name: str, destination_table: str, archive_files_after: bool = True, skip_headers: bool = True):
    csv_path = download_most_recent_csv(bucket_name, prefix)
    csv_name = os.path.basename(csv_path)
    schema_path = download_schema(schema_name)
    
    for g in re.findall(DESTINATION_TABLE_REGEX, destination_table):
        start, end = g.split(":")
        destination_table = destination_table.replace("{" + g + "}", csv_name[int(start):int(end)])
    
    with open(schema_path, "r") as f:
        if schema_path.endswith(".yaml"):
            schema = yaml.load(f, yaml.SafeLoader)
        elif schema_path.endswith(".json"):
            schema = json.load(f)
    
    fixed_csv_path = fix_csv_from_schema(csv_path, csv_name, schema, skip_headers=skip_headers)
    fixed_csv_name = os.path.basename(fixed_csv_path)
    upload_fixed_csv(bucket_name, fixed_csv_path, fixed_csv_name)

    csv_uri = f"gs://{bucket_name}/{fixed_csv_name}"
    load_csv(csv_uri, schema, destination_table)

    if archive_files_after:
        clean_bucket(bucket_name, prefix)


def download_most_recent_csv(bucket_name: str, prefix: str):
    bucket = gs.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    most_recent_blob = None
    
    for blob in blobs:
        if not most_recent_blob or blob.name > most_recent_blob.name:
            most_recent_blob = blob
    
    if not most_recent_blob:
        logging.error(f"No csv file found in bucket {bucket_name} for prefix {prefix}")
        raise CsvNotFound()

    if not most_recent_blob.name.endswith(".csv") and not most_recent_blob.name.endswith(".csv.gz"):
        logging.error("CSV file has invalid extension (.csv or .csv.gz needed)")
        raise CsvInvalid()
    
    local_path = f"/tmp/{most_recent_blob.name}"
    most_recent_blob.download_to_filename(local_path)
    return local_path
    #return f"gs://{bucket_name}/{most_recent_blob.name}"


def fix_csv_from_schema(csv_path: str, csv_name: str, schema: Dict, skip_headers: bool = True):
    fixed_csv_path = f"/tmp/FIXED_{csv_name}".replace(".gz", "")
    
    with gzip.open(csv_path, "rt", encoding=ENCODING) if csv_path.endswith(".gz") else open(csv_path, "r", encoding=ENCODING) as f:
        r = csv.reader(f, delimiter=";", quoting=csv.QUOTE_NONE, escapechar="\\")
        
        if skip_headers:
            next(r)
        
        with open(fixed_csv_path, "w") as f2:
            w = csv.writer(f2, delimiter=";", quoting=csv.QUOTE_NONE, escapechar="\\")
            for row in r:
                if len(schema["fields"]) != len(row):
                    # invalid row, we skip it
                    continue
                row = fix_csv_row(row, schema["fields"])
                w.writerow(row)
    return fixed_csv_path


def fix_csv_row(row: Iterable, fields: Iterable):
    for i in range(len(fields)):
        if fields[i]["type"] == INTEGER_TYPE:
            try:
                int(row[i])
            except:
                row[i] = None
        elif fields[i]["type"] == FLOAT_TYPE:
            try:
                float(row[i])
            except:
                row[i] = None
        elif fields[i]["type"] == TIMESTAMP_TYPE:
            new_date = None
            for format in DATE_FORMATS:
                try:
                    dt = datetime.strptime(row[i], format)
                    new_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                    break
                except:
                    continue
            row[i] = new_date
    return row
    

def upload_fixed_csv(bucket_name: str, fixed_csv_path: str, fixed_csv_name: str):
    bucket = gs.get_bucket(bucket_name)
    blob = bucket.blob(fixed_csv_name)
    blob.upload_from_filename(fixed_csv_path, timeout=600)


def download_schema(schema_name: str):
    bucket = gs.get_bucket(SCHEMAS_BUCKET)
    blob = bucket.get_blob(schema_name)

    if not blob:
        logging.exception(f"No schema file named {schema_name} found in bucket {SCHEMAS_BUCKET}")
        raise SchemaNotFound()

    if not blob.name.endswith(".json") and not blob.name.endswith(".yaml"):
        logging.info("Schema file has invalid extension (.json or .yaml needed)")
        raise SchemaInvalid()

    local_path = f"/tmp/{blob.name}"
    blob.download_to_filename(local_path)
    return local_path


def load_csv(csv_uri: str, schema: Dict, destination_table: str):            
    job_config = LoadJobConfig(
        schema=[
            SchemaField(field["name"], field["type"], field["mode"])
            for field in schema["fields"]
        ],
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        create_disposition=CreateDisposition.CREATE_IF_NEEDED,
        source_format=SourceFormat.CSV,
        field_delimiter=";",
        ignore_unknown_values=True,
        quote_character="",
    )
    
    load_job = bq.load_table_from_uri(csv_uri, destination_table, job_config=job_config)
    
    try:
        load_job.result()
    except Exception as e:
        logging.exception(e)
        logging.error(load_job.error_result)
        logging.error(load_job.errors)
        raise LoadJobError()


def clean_bucket(bucket_name, prefix):
    bucket = gs.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        bucket.rename_blob(blob, ARCHIVED_SUBFOLDER + blob.name)
    
    blobs = bucket.list_blobs(prefix="FIXED_" + prefix)
    for blob in blobs:
        bucket.rename_blob(blob, ARCHIVED_SUBFOLDER + blob.name)


def http_trigger(request):
    logging.info("New CSV load")
    config = request.get_json(silent=True)
    logging.info(json.dumps(config))
    bucket_name = config.get("bucket")
    prefix = config.get("prefix")
    schema_name = config.get("schema")
    destination_table = config.get("destinationTable")
    archive_files_after = config.get("archiveFiles", True)
    skip_headers = config.get("skipHeaders", True)

    try:
        execute_job(bucket_name, prefix, schema_name, destination_table, archive_files_after, skip_headers)
    except HTTPException as e:
        return jsonify({ "description": e.description }), e.code
    except Exception as e:
        logging.exception(e)
        return jsonify({ "description": "Unknown error" }), 500
    
    return jsonify({ "description": "Success" }), 200


if __name__ == "__main__":
    if len(sys.argv) == 5:
        execute_job(*sys.argv[1:])
