"""Streaming data into BigQuery."""
# -*- coding: utf-8 -*-
import json
import logging
import time
from typing import Iterator, Optional, TextIO, Union

from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.table import TableReference
from jsonschema import validate
from singer import (  # noqa: I001
    get_logger,  # noqa: I001
    parse_message,  # noqa: I001
    RecordMessage,  # noqa: I001
    SchemaMessage,  # noqa: I001
    StateMessage,  # noqa: I001
)  # noqa: I001

from target_bigquery.encoders import DecimalEncoder
from target_bigquery.exceptions import (  # noqa: I001
    InvalidSingerMessage,  # noqa: I001
    SchemaNotFoundException,  # noqa: I001
)  # noqa: I001
from target_bigquery.schema import build_schema, filter_schema
from target_bigquery.tools import table_exists

LOGGER: logging.RootLogger = get_logger()
FIVE_MINUTES: int = 300


def persist_lines_stream(  # noqa: 211
    client: Client,
    project_id,
    dataset: Dataset,
    lines: TextIO,
    truncate: bool,
    forced_fulltables: list,
    validate_records: bool = True,
    table_suffix: Optional[str] = None,
    table_prefix: Optional[str] = None,
) -> Iterator[Optional[str]]:
    """Stream data into BigQuery.

    Arguments:
        client {Client} -- BigQuery client
        dataset {Dataset} -- BigQuery dataset
        lines {TextIO} -- Tap stream

    Keyword Arguments:
        truncate {bool} -- Whether to truncunate the table
        forced_fulltables {list} -- List of tables to truncunate
        validate_records {bool} -- Whether to alidate records (default: {True})
        table_suffix {Optional[str]} -- Suffix for tables (default: {None})
        table_prefix {Optional[str]} -- Prefix for tables (default: {None})

    Raises:
        SchemaNotFoundException: If the schema message was not received yet
        InvalidSingerMessage: Invalid Sinnger message

    Yields:
        Iterator[Optional[str]] -- State
    """
    # Create variable in which we save data in the upcomming loop
    state: Optional[str] = None
    schemas: dict = {}
    key_properties: dict = {}
    tables: dict = {}
    rows: dict = {}
    errors: dict = {}
    table_suffix = table_suffix or ''
    table_prefix = table_prefix or ''

    # For every Singer input message
    for line in lines:
        # Parse the message
        try:
            msg: Union[SchemaMessage, StateMessage, RecordMessage] = (
                parse_message(line)
            )
        except json.decoder.JSONDecodeError:
            LOGGER.error(f'Unable to parse Singer Message:\n{line}')
            raise

        # There can be several kind of messages. When inserting data, the
        # schema message comes first
        if isinstance(msg, SchemaMessage):
            # Schema message, create the table
            table_name: str = table_prefix + msg.stream + table_suffix

            # Save the schema, key_properties and message to use in the
            # record messages that are following
            schemas[table_name] = msg.schema
            key_properties[table_name] = msg.key_properties

            tables[table_name] = bigquery.Table(
                dataset.table(table_name),
                schema=build_schema(schemas[table_name]),
            )

            rows[table_name] = 0
            errors[table_name] = None

            dataset_id: str = dataset.dataset_id
            if not table_exists(client, project_id, dataset_id, table_name):
                # Create the table
                client.create_table(tables[table_name])
            elif truncate or table_name in forced_fulltables:
                LOGGER.info(f'Load {table_name} by FULL_TABLE')

                # When truncating is enabled and the table exists, the table
                # has to be recreated. Because of this, we have to wait
                # otherwise data can be lost, see:
                # https://stackoverflow.com/questions/36846571/
                # bigquery-table-truncation-before-streaming-not-working
                LOGGER.info(f'Deleting table {table_name} because it exists')
                client.delete_table(tables[table_name])
                LOGGER.info(f'Recreating table {table_name}')
                client.create_table(tables[table_name])
                LOGGER.info(
                    'Sleeping for 5 minutes before streaming data, '
                    f'to avoid streaming data loss in {table_name}',
                )
                time.sleep(FIVE_MINUTES)

                # Delete table

        elif isinstance(msg, RecordMessage):
            # Record message
            table_name = table_prefix + msg.stream + table_suffix

            if table_name not in schemas:
                raise SchemaNotFoundException(
                    f'A record for stream {table_name} was encountered before '
                    'a corresponding schema',
                )

            # Retrieve schema
            schema: dict = schemas[table_name]

            # Retrieve table
            table_ref: TableReference = tables[table_name]

            # Validate the record
            if validate_records:
                # Raises ValidationError if the record has invalid schema
                validate(msg.record, schema)

            # Filter the record
            record_input: Optional[Union[dict, str, list]] = filter_schema(
                schema,
                msg.record,
            )

            # Somewhere in the process, the input record can have decimal
            # values e.g. "value": Decimal('10.25'). These are not JSON
            # erializable. Therefore, we dump the JSON here, which converts
            # them to string. Thereafter, we load the dumped JSON so we get a
            # dictionary again, which we can insert to BigQuery
            record_json: str = json.dumps(record_input, cls=DecimalEncoder)
            record: dict = json.loads(record_json)

            # Save the error
            err: Optional[list] = None

            try:
                # Insert record
                err = client.insert_rows(table_ref, [record])
            except Exception as exc:
                LOGGER.error(
                    f'Failed to insert rows for {table_name}: {exc}\n'
                    f'{record}\n{err}',
                )
                raise

            # Save errors of the stream and increate the insert rows
            errors[msg.stream] = err
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, StateMessage):
            # State messages
            LOGGER.debug(f'Setting state to {msg.value}')
            state = msg.value

        else:
            raise InvalidSingerMessage(f'Unrecognized Singer Message:\n {msg}')

    for table in errors.keys():
        if errors[table]:
            logging.error(f'Errors: {errors[table]}')
        else:
            logging.info(
                'Loaded {rows} row(s) from {source} into {tab}:{path}'.format(
                    rows=rows[table],
                    source=dataset.dataset_id,
                    tab=table,
                    path=tables[table].path,
                ),
            )
            yield state
