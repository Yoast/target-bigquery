"""Decimal encoder."""
# -*- coding: utf-8 -*-

import decimal
import json
from typing import Any


class DecimalEncoder(json.JSONEncoder):
    """Convert Decimal in JSON to string."""

    def default(self, input_object: Any) -> Any:
        """Convert Decimal objects to string.

        Arguments:
            input_object {Any} -- Input object

        Returns:
            Any -- Output object
        """
        if isinstance(input_object, decimal.Decimal):
            return str(input_object)
        return super().default(input_object)
