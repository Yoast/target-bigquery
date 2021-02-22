"""Tools."""
# -*- coding: utf-8 -*-
import json
import logging
import sys
from typing import Optional

import singer
from google.cloud.bigquery.client import Client
from google.cloud.exceptions import NotFound

logger: logging.RootLogger = singer.get_logger()


def emit_state(state: Optional[dict]) -> None:
    """Write state to stdout.

    Arguments:
        state {Optional[dict]} -- State
    """
    if state is not None:
        line: str = json.dumps(state)
        logger.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()


def dataset_exists(client: Client, dataset_id: str) -> bool:
    """Check whether a BigQuery dataset exists.

    Arguments:
        client {Client} -- BigQuery client
        dataset_id {str} -- Dataset id

    Returns:
        bool -- Whether the dataset exists
    """
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        return False
    return True


def table_exists(
    client: Client,
    project_id: str,
    dataset: str,
    table_ref: str,
) -> bool:
    """Check whether a table exists.

    Arguments:
        client {Client} -- BigQuery client
        project_id {str} -- BigQuery project id
        dataset {str} -- BigQuery dataset
        table_ref {str} -- BigQuery table

    Returns:
        bool -- Whether the table exists.
    """
    table: str = f'{project_id}.{dataset}.{table_ref}'
    try:
        client.get_table(table)
    except NotFound:
        return False
    return True
