"""
This module contains a db-middleware, i.e. a flask app-factory that can
be used to provide a shared database.
"""

from typing import Optional
from threading import Lock
from pathlib import Path
from uuid import uuid4
import os
import sys

from flask import (
    Flask, Blueprint, Response, jsonify, request, send_from_directory
)

from dcm_common.db import KeyValueStore


def bp_factory(
    db: KeyValueStore, name: Optional[str] = None, cors=None
) -> Blueprint:
    """
    Returns a flask-Blueprint with endpoints for database interaction
    via http. Refer to the API-document provided by the `/api-GET`-
    endpoint.
    It is designed to handle concurrent requests to a database that
    stores JSON-data efficiently and without race-conditions.
    """

    bp = Blueprint(name or "db", __name__)

    # load config-info
    db_info = {
        "backend": db.__class__.__name__
    }

    if hasattr(db, "dir"):
        db_info["dir"] = str(db.dir.resolve())

    # resource lock
    db_lock = Lock()

    @bp.route("/db", methods=["OPTIONS"], provide_automatic_options=False)
    def options_key():
        with db_lock:
            keys = db.keys()
        return jsonify(list(keys)), 200

    @bp.route("/db", methods=["GET"], provide_automatic_options=False)
    def next_():
        with db_lock:
            keys = db.keys()
            if not keys:
                return Response(
                    "Empty database.", 404, mimetype="text/plain"
                )
            value = db.read(keys[0])
            if "pop" in request.args:
                db.delete(keys[0])
        return jsonify({"key": keys[0], "value": value}), 200

    @bp.route("/db/<key>", methods=["GET"], provide_automatic_options=False)
    def get_key(key: str):
        with db_lock:
            if key not in db.keys():
                return Response(
                    f"Unknown key '{key}'.", 404, mimetype="text/plain"
                )
            value = db.read(key)
            if "pop" in request.args:
                db.delete(key)
        return jsonify(value), 200

    @bp.route("/db", methods=["POST"], provide_automatic_options=False)
    def push():
        with db_lock:
            while (key := str(uuid4())) in db.keys():
                pass
            db.write(key, request.json)
        return Response(key, 200, mimetype="text/plain")

    @bp.route("/db/<key>", methods=["POST"], provide_automatic_options=False)
    def post_key(key: str):
        with db_lock:
            db.write(key, request.json)
        return Response("OK", 200, mimetype="text/plain")

    @bp.route("/db/<key>", methods=["DELETE"], provide_automatic_options=False)
    def delete_key(key: str):
        with db_lock:
            db.delete(key)
        return Response("OK", 200, mimetype="text/plain")

    @bp.route("/config", methods=["GET"])
    def config():
        return jsonify(
            {"database": db_info, "cors": cors is not None}
        ), 200

    @bp.route("/api", methods=["GET"])
    def api():
        return send_from_directory(
            Path(__file__).parent,
            "openapi.yaml",
            mimetype="application/yaml"
        )
    return bp


def app_factory(db: KeyValueStore, name: Optional[str] = None) -> Flask:
    """
    Returns a flask-app object that allows database interaction via
    http. Refer to the API-document provided by the `/api-GET`-endpoint.
    It is designed to handle concurrent requests to a database that
    stores JSON-data efficiently and without race-conditions.
    """

    app = Flask(name or __name__)

    # handle CORS
    cors = None
    if os.environ.get("ALLOW_CORS") == "1":
        try:
            from flask_cors import CORS
        except ImportError:
            print(
                "WARNING: Missing package 'Flask-CORS' for 'ALLOW_CORS=1'. "
                + "CORS-requests will not work.",
                file=sys.stderr
            )
        else:
            cors = CORS(app)

    app.register_blueprint(bp_factory(db, "db", cors), url_prefix="/")

    return app
