"""Load jobs into BigQuery."""
# -*- coding: utf-8 -*-
import json
import logging
from tempfile import TemporaryFile
from typing import Iterator, Optional, TextIO, Union

from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import LoadJob, LoadJobConfig, WriteDisposition
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import SourceFormat
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

LOGGER: logging.RootLogger = get_logger()


def persist_lines_job(  # noqa: WPS210, WPS211, WPS213, WPS231, WPS238
    client: Client,
    dataset: Dataset,
    lines: TextIO,
    truncate: bool,
    forced_fulltables: list,
    validate_records: bool = True,
    table_suffix: Optional[str] = None,
    table_prefix: Optional[str] = None,
) -> Iterator[Optional[str]]:
    """Perform a load job into BigQuery.

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
            # Schema message, save schema
            table_name: str = table_prefix + msg.stream + table_suffix

            # Skip schema if already created
            if table_name in rows:
                continue

            # Save schema and setup a temp file for data storage
            schemas[table_name] = msg.schema
            key_properties[table_name] = msg.key_properties
            rows[table_name] = TemporaryFile(mode='w+b')
            errors[table_name] = None

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

            # Validate the record
            if validate_records:
                # Raises ValidationError if the record has invalid schema
                validate(msg.record, schema)

            record_input: Optional[Union[dict, str, list]] = filter_schema(
                schema,
                msg.record,
            )

            # Somewhere in the process, the input record can have decimal
            # values e.g. "value": Decimal('10.25'). These are not JSON
            # erializable. Therefore, we dump the JSON here, which converts
            # them to string. Thereafter, we load the dumped JSON so we get a
            # dictionary again, which we can insert to BigQuery
            record_str: str = '{rec}\n'.format(
                rec=json.dumps(record_input, cls=DecimalEncoder),
            )

            record: bytes = bytes(record_str, 'UTF-8')

            # Save data to load later
            rows[table_name].write(record)

            state = None

        elif isinstance(msg, StateMessage):
            # State messages
            LOGGER.debug(f'Setting state to {msg.value}')
            state = msg.value

        else:
            raise InvalidSingerMessage(
                f'Unrecognized Singer Message:\n {msg}',
            )

    # After all recordsa are received, setup a load job per stream
    for table in rows.keys():
        # Prepare load job
        key_props: str = key_properties[table]
        load_config: LoadJobConfig = LoadJobConfig()
        load_config.schema = build_schema(
            schemas[table],
            key_properties=key_props,
        )
        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        # Overwrite the table if truncate is enabled
        if truncate or table in forced_fulltables:
            LOGGER.info(f'Load {table} by FULL_TABLE')
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        LOGGER.info(f'loading {table} to Bigquery.')

        # Setup load job
        load_job: LoadJob = client.load_table_from_file(
            rows[table],
            dataset.table(table),
            job_config=load_config,
            rewind=True,
        )

        LOGGER.info(f'loading job {load_job.job_id}')

        # Run load job
        try:
            load_job.result()
        except google_exceptions.GoogleAPICallError as err:
            # Parse errors
            LOGGER.error(f'failed to load table {table} from file: {err}')

            if load_job.errors:
                messages: list = [
                    f"reason: {err['reason']}, message: {err['message']}"
                    for err in load_job.errors
                ]
                messages_str: str = '\n'.join(messages)
                LOGGER.error(f'errors:\n{messages_str}')
            raise
        LOGGER.info(
            f'Loaded {load_job.output_rows} row(s) in '
            f'{load_job.destination}',
        )

    yield state
