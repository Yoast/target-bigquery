"""Load jobs into BigQuery"""
# -*- coding: utf-8 -*-
import logging
import json
from typing import Iterator, Optional, TextIO, Union
from tempfile import TemporaryFile
from singer import SchemaMessage, StateMessage, RecordMessage

from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import WriteDisposition
from google.cloud.bigquery import LoadJob, LoadJobConfig
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.client import Client
from google.api_core import exceptions as google_exceptions

import singer
from jsonschema import validate

from target_bigquery.encoders import DecimalEncoder
from target_bigquery.schema import build_schema, filter

logger: logging.RootLogger = singer.get_logger()


def persist_lines_job(
    client: Client,
    dataset: Dataset,
    lines: TextIO,
    truncate: bool = False,
    forced_fulltables: list = [],
    validate_records: bool = True,
    table_suffix: Optional[str] = None,
    table_prefix: Optional[str] = None,
) -> Iterator[Optional[str]]:
    # Create variable in which we save data in the upcomming loop
    state: Optional[str] = None
    schemas: dict = {}
    key_properties: dict = {}
    rows: dict = {}
    errors: dict = {}
    table_suffix = table_suffix or ""
    table_prefix = table_prefix or ''

    # For every Singer input message
    for line in lines:
        # Parse the message
        try:
            msg: Union[SchemaMessage, StateMessage, RecordMessage] = (
                singer.parse_message(line)
            )
        except json.decoder.JSONDecodeError:
            logger.error(f'Unable to parse Singer Message:\n{line}')
            raise

        # There can be several kind of messages. When inserting data, the
        # schema message comes first
        if isinstance(msg, singer.SchemaMessage):
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

        elif isinstance(msg, singer.RecordMessage):
            # Record message
            table_name = table_prefix + msg.stream + table_suffix

            if table_name not in schemas:
                raise Exception(
                    f'A record for stream {table_name} was encountered before '
                    'a corresponding schema'
                    )

            # Retrieve schema
            schema: dict = schemas[table_name]

            # Validate the record
            if validate_records:
                # Raises ValidationError if the record has invalid schema
                validate(msg.record, schema)

            record_input: dict = filter(schema, msg.record)

            # Somewhere in the process, the input record can have decimal
            # values e.g. "value": Decimal('10.25'). These are not JSON
            # erializable. Therefore, we dump the JSON here, which converts
            # them to string. Thereafter, we load the dumped JSON so we get a
            # dictionary again, which we can insert to BigQuery
            record: bytes = bytes(
                json.dumps(record_input, cls=DecimalEncoder) + "\n", 'UTF-8'
            )

            # Save data to load later
            rows[table_name].write(record)

            state = None

        elif isinstance(msg, singer.StateMessage):
            # State messages
            logger.debug(f'Setting state to {msg.value}')
            state = msg.value

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception(f'Unrecognized Singer Message:\n {msg}')

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
            logger.info(f'Load {table} by FULL_TABLE')
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        logger.info(f'loading {table} to Bigquery.')

        try:
            # Setup load job
            load_job: LoadJob = client.load_table_from_file(
                rows[table],
                dataset.table(table),
                job_config=load_config,
                rewind=True,
            )
            logger.info(f'loading job {load_job.job_id}')
            load_job.result()
            logger.info(
                f'Loaded {load_job.output_rows} rows in {load_job.destination}'
            )

        except google_exceptions.BadRequest as err:
            # Parse errors
            logger.error(f'failed to load table {table} from file: {err}')

            if load_job.errors:
                messages: list = [
                    f"reason: {err['reason']}, message: {err['message']}"
                    for err in load_job.errors
                ]
                messages_str: str = "\n".join(messages)
                logger.error(f'errors:\n{messages_str}')
            raise

    yield state
