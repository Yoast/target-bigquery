"""Streaming data into BigQuery"""
# -*- coding: utf-8 -*-
import logging
import json
from typing import Optional, Iterator, TextIO, Union
from singer import SchemaMessage, StateMessage, RecordMessage
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.table import TableReference
from google.cloud import bigquery
from google.api_core import exceptions

from jsonschema import validate
import singer

from target_bigquery.schema import build_schema, filter
from target_bigquery.encoders import DecimalEncoder


logger: logging.RootLogger = singer.get_logger()


def persist_lines_stream(
    client: Client,
    dataset: Dataset,
    lines: TextIO,
    validate_records: bool = True,
    table_suffix: Optional[str] = None,
    table_prefix: Optional[str] = None,
) -> Iterator[Optional[str]]:
    # Create variable in which we save data in the upcomming loop
    state: Optional[str] = None
    schemas: dict = {}
    key_properties: dict = {}
    tables: dict = {}
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
            # Schema message, create the table
            table_name: str = table_prefix + msg.stream + table_suffix

            # Save the schema, key_properties and message to use in the
            # record messages that are following
            schemas[table_name] = msg.schema
            key_properties[table_name] = msg.key_properties

            tables[table_name] = bigquery.Table(
                dataset.table(table_name),
                schema=build_schema(schemas[table_name])
            )

            rows[table_name] = 0
            errors[table_name] = None

            # Create the table
            try:
                tables[table_name] = client.create_table(tables[table_name])
            except exceptions.Conflict:
                # Ignore errors about the table already exists
                pass

        elif isinstance(msg, singer.RecordMessage):
            # Record message
            table_name: str = table_prefix + msg.stream + table_suffix

            if table_name not in schemas:
                raise Exception(
                    f'A record for stream {table_name} was encountered before '
                    'a corresponding schema'
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
            record_input: dict = filter(schema, msg.record)

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
                logger.error(
                    f'Failed to insert rows for {table_name}: {str(exc)}\n'
                    f'{record}\n{err}'
                )
                raise

            # Save errors of the stream and increate the insert rows
            errors[msg.stream] = err
            rows[msg.stream] += 1

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

    for table in errors.keys():
        if not errors[table]:
            logging.info(
                "Loaded {} row(s) from {} into {}:{}".format(
                    rows[table], dataset.dataset_id, table, tables[table].path
                )
            )
            yield state
        else:
            logging.error("Errors: %s", errors[table])
