"""BigQuery target."""
# -*- coding: utf-8 -*-

import io
import logging
import sys
from argparse import ArgumentParser, Namespace
from typing import Iterator, Optional, TextIO, Tuple

import pkg_resources
from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from oauth2client import tools
from singer import get_logger, utils

from target_bigquery.job import persist_lines_job
from target_bigquery.stream import persist_lines_stream
from target_bigquery.tools import dataset_exists, emit_state

VERSION: str = pkg_resources.get_distribution('target-bigquery').version
LOGGER: logging.RootLogger = get_logger()
REQUIRED_CONFIG_KEYS: tuple = ('project_id', 'dataset_id')

# Disable google cache logger message
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


@utils.handle_top_exception(LOGGER)
def main() -> None:  # noqa: WPS210
    """Run target."""
    # Required command line arguments
    parser: ArgumentParser = ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)

    # Parse command line arguments
    args: Namespace = utils.parse_args(REQUIRED_CONFIG_KEYS)

    LOGGER.info(f'>>> Running target-bigquery v{VERSION}')

    # Configuration variables
    config = args.config

    truncate: bool = config.get('replication_method') == 'FULL_TABLE'
    forced_fulltables: list = config.get('forced_fulltables', [])
    table_suffix: Optional[str] = config.get('table_suffix')
    table_prefix: Optional[str] = config.get('table_prefix')
    location: str = config.get('location', 'EU')
    validate_records: bool = config.get('validate_records', True)
    project_id, dataset_id = config['project_id'], config['dataset_id']
    stream_data: bool = config.get('stream_data', True)

    LOGGER.info(
        f'BigQuery target configured to move data to '  # noqa: WPS221
        f'{project_id}.{dataset_id}. table_suffix={table_suffix}, '
        f'table_prefix={table_prefix}, stream_data={stream_data}, '
        f'location={location}, validate_records={validate_records}, '
        f'forced_fulltables={forced_fulltables}',
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
            project_id,
            dataset,
            input_target,
            truncate=truncate,
            forced_fulltables=forced_fulltables,
            validate_records=validate_records,
            table_suffix=table_suffix,
            table_prefix=table_prefix,
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
            table_prefix=table_prefix,
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

    if not dataset_exists(client, dataset_id):
        # Create dataset
        LOGGER.info(
            f'Creating dataset: {project_id}.{dataset_id} in '
            f'location: {location}',
        )

        client.create_dataset(dataset_ref, exists_ok=True)

        LOGGER.info(
            f'Succesfully created dataset: {project_id}.{dataset_id} in '
            f'location: {location}',
        )

    return client, Dataset(dataset_ref)


if __name__ == '__main__':
    main()
