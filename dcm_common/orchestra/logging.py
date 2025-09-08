"""Global `orchestra`-settings."""

import os
import sys
from time import time


def map_loglevel(level: str) -> int:
    """Returns integer-representation of the given loglevel."""
    match level:
        case "none":
            return -1
        case "error":
            return 0
        case "info":
            return 1
        case "debug":
            return 2
        case _:
            raise ValueError(f"Unknown loglevel '{level}'.")


class Logging:
    """Global `orchestra`-settings."""

    LEVEL_NONE = -1
    LEVEL_ERROR = 0
    LEVEL_INFO = 1
    LEVEL_DEBUG = 2
    LOGLEVEL = map_loglevel(os.environ.get("ORCHESTRA_LOGLEVEL", "info"))
    LOGFILE = sys.stderr
    LOGPREFIX = os.environ.get("ORCHESTRA_LOGPREFIX", "[orchestra]")

    @classmethod
    def print_to_log(cls, msg: str, level: int):
        """Print to orchestra-log."""
        if level <= cls.LOGLEVEL:
            print(
                cls.LOGPREFIX
                + f" [{(str(time()) + '000')[:13]}] "
                + msg,
                file=cls.LOGFILE,
            )
