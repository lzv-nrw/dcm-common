import os
from pathlib import Path

from flask import Flask

from dcm_common.db import (
    MemoryStore, JSONFileStore, key_value_store_bp_factory
)

app = Flask(__name__)

if "QUEUE_MOUNT_POINT" in os.environ:
    queue_backend = JSONFileStore(Path(os.environ["QUEUE_MOUNT_POINT"]))
else:
    queue_backend = MemoryStore()
if "REGISTRY_MOUNT_POINT" in os.environ:
    registry_backend = JSONFileStore(Path(os.environ["REGISTRY_MOUNT_POINT"]))
else:
    registry_backend = MemoryStore()

app.register_blueprint(
    key_value_store_bp_factory(queue_backend, "queue"),
    url_prefix="/queue"
)
app.register_blueprint(
    key_value_store_bp_factory(registry_backend, "registry"),
    url_prefix="/registry"
)
