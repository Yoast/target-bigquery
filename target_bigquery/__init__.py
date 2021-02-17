#!/usr/bin/env python3

import argparse
import io
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources
import decimal

from jsonschema import validate
import singer

from oauth2client import tools
from tempfile import TemporaryFile

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

from target_bigquery.schema import build_schema, filter
from target_bigquery.encoders import DecimalEncoder
from target_bigquery.job import persist_lines_job
from target_bigquery.stream import persist_lines_stream
from target_bigquery.utils import emit_state, collect

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.insertdata",
]
CLIENT_SECRET_FILE = "client_secret.json"
APPLICATION_NAME = "Singer BigQuery Target"

StreamMeta = collections.namedtuple(
    "StreamMeta", ["schema", "key_properties", "bookmark_properties"]
)


def main():
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    flags = parser.parse_args()

    with open(flags.config) as input:
        config = json.load(input)

    if not config.get("disable_collection", False):
        logger.info(
            "Sending version information to stitchdata.com. "
            "To disable sending anonymous usage data, set "
            "the config parameter 'disable_collection' to true"
        )
        threading.Thread(target=collect).start()

    if config.get("replication_method") == "FULL_TABLE":
        truncate = True
    else:
        truncate = False
    forced_fulltables = config.get("forced_fulltables", [])
    table_suffix = config.get("table_suffix")

    location = config.get("location", "EU")

    validate_records = config.get("validate_records", True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    project_id, dataset_id = config["project_id"], config["dataset_id"]

    client, dataset = ensure_dataset(project_id, dataset_id, location)

    if config.get("stream_data", True):
        state_iterator = persist_lines_stream(
            client,
            dataset,
            input,
            validate_records=validate_records,
            table_suffix=table_suffix,
        )

    else:
        state_iterator = persist_lines_job(
            client,
            dataset,
            input,
            truncate=truncate,
            forced_fulltables=forced_fulltables,
            validate_records=validate_records,
            table_suffix=table_suffix,
        )

    for state in state_iterator:
        emit_state(state)


def ensure_dataset(project_id, dataset_id, location):
    client = bigquery.Client(project=project_id, location=location)

    dataset_ref = client.dataset(dataset_id)
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
    except Exception:
        # attempt to run even if creation fails due to permissions etc.
        pass

    return client, Dataset(dataset_ref)


if __name__ == "__main__":
    main()
