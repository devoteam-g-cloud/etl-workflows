import sys
import logging
import json

from google.cloud import storage, bigquery
from google.cloud.bigquery.job.query import QueryJobConfig
from google.cloud.bigquery.enums import CreateDisposition, WriteDisposition
from flask import jsonify
from werkzeug.exceptions import HTTPException

from exceptions import *


logging.root.setLevel(logging.INFO)
QUERIES_BUCKET = "my-assets-bucket" # SET YOUR BUCKET NAME
gs = storage.Client()
bq = bigquery.Client()


def execute_job(query_name: str, destination_table: str, use_legacy_sql: bool = False, append: bool = False):
    query_path = download_query(query_name)
    load_query(query_path, destination_table, use_legacy_sql, append)


def download_query(query_name: str):
    bucket = gs.get_bucket(QUERIES_BUCKET)
    blob = bucket.get_blob(query_name)

    if not blob:
        logging.exception(f"No query file named {query_name} found in bucket {QUERIES_BUCKET}")
        raise QueryNotFound()

    if not blob.name.endswith(".sql"):
        logging.info("Query file has invalid extension (.sql needed)")
        raise QueryInvalid()

    local_path = f"/tmp/{blob.name}"
    blob.download_to_filename(local_path)
    return local_path


def load_query(query_path: str, destination_table: str, use_legacy_sql: bool = False, append: bool = False):
    if not query_path.endswith(".sql"):
        logging.error("Query file has invalid extension (.sql needed)")
        raise QueryInvalid()

    with open(query_path, "r") as f:
        query = f.read()
                
    job_config = QueryJobConfig(
        use_legacy_sql=use_legacy_sql,
        write_disposition=WriteDisposition.WRITE_APPEND if append else WriteDisposition.WRITE_TRUNCATE,
        create_disposition=CreateDisposition.CREATE_IF_NEEDED,
        destination=destination_table
    )
    
    load_job = bq.query(query, job_config=job_config)
    
    try:
        load_job.result()
    except Exception as e:
        logging.exception(e)
        logging.error(load_job.error_result)
        logging.error(load_job.errors)
        raise CreationFailed()


def http_trigger(request):
    logging.info("New query load")
    config = request.get_json(silent=True)
    logging.info(json.dumps(config))
    query_name = config.get("query")
    destination_table = config.get("destinationTable")
    use_legacy_sql = config.get("useLegacySql", False)
    append = config.get("append", False)

    try:
        execute_job(query_name, destination_table, use_legacy_sql, append)
    except HTTPException as e:
        return jsonify({ "description": e.description }), e.code
    except Exception as e:
        logging.exception(e)
        return jsonify({ "description": "Unknown error" }), 500
    
    return jsonify({ "description": "Success" }), 200


if __name__ == "__main__":
    if len(sys.argv) == 3:
        execute_job(*sys.argv[1:])
