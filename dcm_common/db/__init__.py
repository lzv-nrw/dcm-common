from .key_value_store.backend.interface import KeyValueStore
from .key_value_store.backend.memory import MemoryStore
from .key_value_store.backend.disk import JSONFileStore
from .key_value_store.middleware.flask.factory import (
    app_factory as key_value_store_app_factory,
    bp_factory as key_value_store_bp_factory,
)
from .key_value_store.adapter.interface import KeyValueStoreAdapter
from .key_value_store.adapter.native import NativeKeyValueStoreAdapter
from .key_value_store.adapter.http import HTTPKeyValueStoreAdapter


def check_psycopg_dependencies():
    """
    Checks whether the package `psycopg` can be imported without segfault.
    """
    # pylint: disable=import-outside-toplevel
    import sys
    import subprocess

    try:
        subprocess.run(
            [sys.executable, "-c", "import psycopg"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as exc_info:
        return False, str(exc_info)
    return True, ""


# safeguard imports
if (psycopg_ok := check_psycopg_dependencies())[0]:
    import psycopg

    from .sql.adapter.postgres import PostgreSQLAdapter14
else:
    psycopg = None

    class PostgreSQLAdapter14:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("Unable to import 'psycopg'" + psycopg_ok[1])


from .sql.adapter.sqlite import SQLiteAdapter3
from .sql.adapter.interface import (
    RawTransactionResult,
    TransactionResult,
    PooledConnectionAdapter,
    SQLAdapter,
    Transaction,
)
from .sql.adapter.pooling import (
    Connection,
    Claim,
    ConnectionPool,
)


__all__ = [
    "KeyValueStore",
    "MemoryStore",
    "JSONFileStore",
    "key_value_store_app_factory",
    "key_value_store_bp_factory",
    "KeyValueStoreAdapter",
    "NativeKeyValueStoreAdapter",
    "HTTPKeyValueStoreAdapter",
    "psycopg",
    "PostgreSQLAdapter14",
    "SQLiteAdapter3",
    "RawTransactionResult",
    "TransactionResult",
    "PooledConnectionAdapter",
    "SQLAdapter",
    "Transaction",
    "Connection",
    "Claim",
    "ConnectionPool",
]
