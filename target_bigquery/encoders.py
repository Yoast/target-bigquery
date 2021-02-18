"""Decimal encoder."""
# -*- coding: utf-8 -*-

import json
import decimal
from typing import Any


class DecimalEncoder(json.JSONEncoder):
    """Convert Decimal in JSON to string."""

    def default(self, object: Any) -> Any:
        if isinstance(object, decimal.Decimal):
            return str(object)
        return super(DecimalEncoder, self).default(object)
