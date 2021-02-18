"""BigQuery target."""
# -*- coding: utf-8 -*-

import argparse
import io
import sys

import pkg_resources

import logging

from typing import Iterator, Optional, TextIO
from argparse import ArgumentParser, Namespace

from typing import Tuple
import singer


from oauth2client import tools

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud import bigquery

from target_bigquery.job import persist_lines_job
from target_bigquery.stream import persist_lines_stream
from target_bigquery.utils import emit_state

from singer import utils

VERSION: str = pkg_resources.get_distribution('target-bigquery').version
LOGGER: logging.RootLogger = singer.get_logger()
REQUIRED_CONFIG_KEYS: tuple = ('project_id', 'dataset_id')

# Disable google cache logger message
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


@utils.handle_top_exception(LOGGER)
def main() -> None:
    """Run target."""
    # Required command line arguments
    parser: ArgumentParser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)

    # Parse command line arguments
    # args: Namespace = parser.parse_args()
    args: Namespace = utils.parse_args(REQUIRED_CONFIG_KEYS)

    LOGGER.info(f'>>> Running target-bigquery v{VERSION}')

    # Configuration variables
    config = args.config

    truncate: bool = config.get('replication_method') == 'FULL_TABLE'
    forced_fulltables: list = config.get('forced_fulltables', [])
    table_suffix: Optional[str] = config.get('table_suffix')
    location: str = config.get('location', 'EU')
    validate_records: bool = config.get('validate_records', True)
    project_id, dataset_id = config['project_id'], config['dataset_id']
    stream_data: bool = config.get('stream_data', True)

    LOGGER.info(
        f'BigQuery target configured to move data to {project_id}.'
        f'{dataset_id}. table_suffix={table_suffix}, stream_data='
        f'{stream_data}, location={location}, validate_records='
        f'{validate_records}, forced_fulltables={forced_fulltables}'
    )

    # Create dataset if not exists
    client, dataset = ensure_dataset(project_id, dataset_id, location)

    # Input data from the tap
    input_target: TextIO = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if stream_data and truncate:
        raise NotImplementedError(
            'Streaming data and truncating table is currently not implemented.'
        )

    if stream_data:
        state_iterator: Iterator = persist_lines_stream(
            client,
            dataset,
            input_target,
            validate_records=validate_records,
            table_suffix=table_suffix,
        )

    else:
        state_iterator = persist_lines_job(
            client,
            dataset,
            input_target,
            truncate=truncate,
            forced_fulltables=forced_fulltables,
            validate_records=validate_records,
            table_suffix=table_suffix,
        )

    for state in state_iterator:
        emit_state(state)


def ensure_dataset(
    project_id: str,
    dataset_id: str,
    location: str,
) -> Tuple[Client, Dataset]:
    """Create BigQuery dataset if not exists.

    Arguments:
        project_id {str} -- Project id
        dataset_id {str} -- Dataset id
        location {str} -- Dataset location

    Returns:
        Tuple[Client, Dataset] -- BigQuery Client and Dataset
    """
    client: Client = bigquery.Client(project=project_id, location=location)

    dataset_ref: Dataset = client.dataset(dataset_id)
    try:
        LOGGER.info(
            f'Attempting to create dataset: {project_id}.{dataset_id} in '
            f'location: {location}'
        )
        client.create_dataset(dataset_ref, exists_ok=True)
        LOGGER.info(
            f'Succesfully created dataset: {project_id}.{dataset_id} in '
            f'location: {location}'
        )
    except Exception:
        # attempt to run even if creation fails due to permissions etc.
        pass

    return client, Dataset(dataset_ref)


if __name__ == '__main__':
    main()
