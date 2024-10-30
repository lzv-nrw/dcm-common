"""CORS-extension."""

import sys

from flask import Flask


def cors(app: Flask, kwargs=None) -> None:
    """
    Register the `Flask-CORS` extension with the given `kwargs` if
    possible (i.e. if the package is installed).
    """
    try:
        from flask_cors import CORS
    except ImportError:
        print(
            "ERROR: Missing package 'Flask-CORS' for 'ALLOW_CORS=1'. "
            + "Exiting..",
            file=sys.stderr
        )
        sys.exit(1)
    CORS(app, **(kwargs or {}))
