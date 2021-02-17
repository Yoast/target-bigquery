"""Utilities"""
# -*- coding: utf-8 -*-
import logging
import json
import sys

import singer

logger: logging.RootLogger = singer.get_logger()


def emit_state(state) -> None:
    if state is not None:
        line: str = json.dumps(state)
        logger.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()
