"""CORS-extension."""

import sys

from flask import Flask

from .common import ExtensionLoaderResult


def cors_loader(app: Flask, kwargs=None) -> ExtensionLoaderResult:
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
            file=sys.stderr,
        )
        sys.exit(1)
    _cors = CORS(app, **(kwargs or {}))
    return ExtensionLoaderResult(_cors).toggle()
