import logging
import json
import sys

import singer

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

