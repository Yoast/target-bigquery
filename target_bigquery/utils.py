"""Utilities"""
# -*- coding: utf-8 -*-
import logging
import json
import sys
from typing import Optional

import singer

logger: logging.RootLogger = singer.get_logger()


def emit_state(state: Optional[dict]) -> None:
    """Write state to stdout

    Arguments:
        state {Optional[dict]} -- State
    """    
    if state is not None:
        line: str = json.dumps(state)
        logger.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()
