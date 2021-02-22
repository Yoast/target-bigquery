"""Schema."""
# -*- coding: utf-8 -*-

import re
from typing import Optional, Union

from google.cloud.bigquery import SchemaField

JSON_SCHEMA_LITERALS: tuple = ('boolean', 'number', 'integer', 'string')


def get_type(prop: dict) -> tuple:
    """Retrieve the type of the property.

    Arguments:
        prop {dict} -- Input property

    Raises:
        ValueError: If the schema is missingn required fields

    Returns:
        tuple -- The field or property type and whether it is nullable
    """
    nullable: bool = False
    prop_type: Optional[str] = None

    if 'anyOf' in prop:
        return 'anyOf', nullable
    elif prop.get('type'):
        prop_type = prop['type']
    else:
        raise ValueError(
            f"'type' or 'anyOf' are required fields in property: {prop}",
        )

    # The property has one type
    if isinstance(prop_type, str):
        return prop_type, nullable

    field_type: Optional[bool] = None

    # The property ha multiple types
    if isinstance(prop_type, list):
        for p_type in prop_type:
            if p_type.lower() == 'null':
                nullable = True
            else:
                field_type = p_type

    return field_type, nullable


def filter_schema(  # noqa: WPS210, WPS212, WPS231
    schema: dict,
    record: Optional[dict],
) -> Optional[Union[dict, str, list]]:
    """Filter the schema with the record.

    Arguments:
        schema {dict} -- Input schema
        record {Optional[dict]} -- Input record

    Raises:
        ValueError: If the field type is unknnown

    Returns:
        OptionalUnion[dict, str, list] -- The filterd record
    """
    if not record:
        return record

    field_type, _ = get_type(schema)

    # return literals without checking
    if field_type in JSON_SCHEMA_LITERALS:
        return record

    elif field_type == 'anyOf':
        # Parse anyOf

        for prop in schema['anyOf']:
            prop_type, _ = get_type(prop)

            if prop_type == 'null':
                continue

            # anyOf can be an array of properties, the choice here is to ignore
            # the case where anyOf is two types (not including 'null')
            # and simply choose the first one. This might bite us.
            return filter_schema(prop, record)

    elif field_type == 'object':
        # Parse an object
        props: dict = schema.get('properties', {})
        obj_results: dict = {}

        # Build the dictionary
        for key, prop_schema in props.items():
            if key not in record:
                continue

            obj_results[key] = filter_schema(prop_schema, record[key])

        return obj_results

    elif field_type == 'array':
        # Parse an array:
        props = schema.get('items', {})

        prop_type, _ = get_type(props)

        # array can contain either an object or literals
        # - if it contains literals, simply return those
        if prop_type != 'object':
            return record

        # Build the array list
        arr_result: list = []

        for schema_object in record:
            arr_result.append(filter_schema(props, schema_object))

        return arr_result
    else:
        raise ValueError(f'type {field_type} is unknown')


def define_schema(  # noqa: 231
    field: dict,
    name: str,
    required_fields: Optional[set] = None,
) -> SchemaField:
    """Createa schema field.

    Arguments:
        field {dict} -- Input field
        name {str} -- Field name

    Keyword Arguments:
        required_fields {Optional[set]} -- Required field (default: {None})

    Raises:
        ValueError: If the field type is unknown

    Returns:
        SchemaField -- Created schema field
    """
    field_type, _ = get_type(field)
    nullable: bool = True

    # Make required fields non-nullable
    if required_fields and name in required_fields:
        nullable = False

    if field_type == 'anyOf':
        # Multiple possible types
        props: dict = field['anyOf']

        # Select first non-null property
        for prop in props:
            prop_type, _ = get_type(prop)
            if not prop_type:
                continue

            # Take the first property that is not None
            # of the possible types
            if field_type == 'anyOf' and prop_type:
                field_type = prop_type
                field = prop

    schema_description: None = None
    schema_name: str = name
    schema_mode: str = 'NULLABLE' if nullable else 'required'

    if field_type == 'object':
        schema_type = 'RECORD'
        schema_fields = tuple(build_schema(field))
        return SchemaField(
            schema_name,
            schema_type,
            schema_mode,
            schema_description,
            schema_fields,
        )
    elif field_type == 'array':
        # objects in arrays cannot be nullable
        # - but nested fields in RECORDS can be nullable
        props = field.get('items', {})
        props_type, _ = get_type(props)

        if props_type == 'object':
            schema_type = 'RECORD'
            schema_fields = tuple(build_schema(props))
        else:
            schema_type = props_type
            schema_fields = ()

        schema_mode = 'REPEATED'
        return SchemaField(
            schema_name,
            schema_type,
            schema_mode,
            schema_description,
            schema_fields,
        )

    # Check if the field type is known
    if field_type not in JSON_SCHEMA_LITERALS:
        raise ValueError(f'unknown type: {field_type}')

    # Handle all custom field types
    if field_type == 'string' and 'format' in field:
        # String field types can be converted to custom types
        field_format: str = field['format']
        if field_format == 'date-time':
            schema_type = 'timestamp'
        elif field_format == 'date':
            schema_type = 'date'
        elif field_format == 'time':
            schema_type = 'time'
        else:
            schema_type = field_type

    elif field_type == 'number':
        # Number field types can be converted to custom number types
        field_format = str(field.get('format', ''))
        if field_format == 'float':
            schema_type = 'FLOAT64'
        elif field_format == 'numeric':
            schema_type = 'NUMERIC'
        elif field_format == 'bignumeric':
            schema_type = 'BIGNUMERIC'
        else:
            schema_type = 'NUMERIC'
    else:
        schema_type = field_type

        # always make a field nullable
    return SchemaField(
        schema_name,
        schema_type,
        schema_mode,
        schema_description,
        (),
    )


def bigquery_transformed_key(key: str) -> str:
    """Transform BigQuery key to legal format.

    Arguments:
        key {str} -- Input key

    Returns:
        str -- Transformed key
    """
    for pattern, repl in (('-', '_'), (r'^\d', '_'), (r'\.', '_')):
        key = re.sub(pattern, repl, key)

    return key


def build_schema(
    schema: dict,
    key_properties: Optional[Union[str, list]] = None,
) -> list:
    """Build the schema.

    Arguments:
        schema {dict} -- Input schema

    Keyword Arguments:
        key_properties {Optional[Union[str, list]]} -- Key properties
        (default: {None})

    Returns:
        list -- Built schema
    """
    built_schema: list = []

    required_fields = set(key_properties) if key_properties else set()
    if schema.get('required'):
        required_fields.update(schema['required'])

    for key, props in schema['properties'].items():

        if not props:
            # if we endup with an empty record.
            continue

        built_schema.append(
            define_schema(
                props,
                bigquery_transformed_key(key),
                required_fields=required_fields,
            ),
        )

    return built_schema
