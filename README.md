# Digital Curation Manager - Common Code Library
This package provides common functions and components for the [`Digital Curation Manager`](https://github.com/lzv-nrw/digital-curation-manager)-project.
This includes:
* `db`: database implementations and adapter definitions,
* `models`: data-model interface and common models,
* `orchestration`: job orchestration-system,
* `plugins`: an interface and optional extensions for a general plugin-system,
* `services`: various general and dcm-specific components for the definition of Flask-based web-applications
* `daemon`: background-process utility,
* `logger`: logging and related definitions, and
* `util`: miscellaneous functions

## Install
Using a virtual environment is recommended.
Install this package and its dependencies form this repository by issuing `pip install .` .
Alternatively, consider installing via the [extra-index-url](https://pip.pypa.io/en/stable/cli/pip_install/#finding-packages) `https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple` with
```
pip install --extra-index-url https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple dcm-common
```

### Extra dependencies
#### Services
This package defines optional dependencies related to flask-webservices.
These can be installed by entering `pip install ".[services]"`.

#### Database
The `db`-subpackage imposes additional requirements.
These can be installed using `pip install ".[db]"`.

#### Orchestration
The `orchestration`-extra shares its additional requirements with the `db`-extra due to its dependence on the `db`-subpackage.

#### xml
The `xml`-subpackage imposes additional requirements.
These can be installed using `pip install ".[xml]"`.

## Tests
Install additional dev-dependencies with
```
pip install -r dev-requirements.txt
```
Run unit-tests with
```
pytest -v -s
```

## Services
Requires extra `services`.
### App-Configuration
#### BaseConfig - Environment/Configuration
* `ALLOW_CORS` [DEFAULT 0]: have flask-app allow cross-origin-resource-sharing; needed for hosting swagger-ui with try-it functionality

#### OrchestratedAppConfig - Environment/Configuration
* `ORCHESTRATION_PROCESSES` [DEFAULT 1]: maximum number of simultaneous job processes
* `ORCHESTRATION_AT_STARTUP` [DEFAULT 1]: whether orchestration-loop is automatically started with app
* `ORCHESTRATION_TOKEN_EXPIRATION` [DEFAULT 1]: whether job tokens (and their associated info like report) expire
* `ORCHESTRATION_TOKEN_DURATION` [DEFAULT 3600]: time until job token expires in seconds
* `ORCHESTRATION_DEBUG` [DEFAULT 0]: whether to have orchestrator print debug-information
* `ORCHESTRATION_CONTROLS_API` [DEFAULT 0]: whether the orchestration-controls API is available
* `ORCHESTRATION_QUEUE_ADAPTER` [DEFAULT "native"]: which adapter-type to use for the queue
* `ORCHESTRATION_REGISTRY_ADAPTER`: same as `ORCHESTRATION_QUEUE_ADAPTER` for registry-adapter
* `ORCHESTRATION_QUEUE_SETTINGS` [DEFAULT {"backend": "memory"}]: JSON object containing the relevant information for initializing the adapter
  * "backend": "disk" | "memory",
  * kwargs expected/accepted by the selected adapter/backend (like "dir", "url", "timeout", ...; see `db`-package docs for more information)
* `ORCHESTRATION_REGISTRY_SETTINGS`: same as `ORCHESTRATION_QUEUE_SETTINGS` for registry-adapter
* `ORCHESTRATION_DAEMON_INTERVAL` [DEFAULT None]: time in seconds between each iteration of the orchestrator daemon
* `ORCHESTRATION_ORCHESTRATOR_INTERVAL` [DEFAULT None]: time in seconds between each iteration of the orchestrator
* `ORCHESTRATION_ABORT_NOTIFICATIONS` [DEFAULT 0]: whether the Notification API is used for job abortion (only relevant in parallel deployment)
* `ORCHESTRATION_ABORT_NOTIFICATIONS_URL` [DEFAULT None]: Notification API url (only relevant in parallel deployment)
* `ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK` [DEFAULT None]: base-url at which abortion requests are made to from a broadcast of the Notification API (only relevant in parallel deployment)
* `ORCHESTRATION_ABORT_TIMEOUT` [DEFAULT 1.0]: timeout duration for notify-requests to the Notification API (only relevant in parallel deployment)

#### FSConfig - Environment/Configuration
In addition to the `BaseConfig`-environment settings, the `FSConfig` introduces the following
* `FS_MOUNT_POINT` [DEFAULT "/file_storage"]: Path to the working directory (typically mount point of the shared file system)

#### DBConfig - Environment/Configuration
* `DB_ADAPTER` [DEFAULT "sqlite"]: database adapter-type; one of
  * `sqlite`: for a SQLite3 database
  * `postgres`: for a PostgreSQL14 database
* `DB_ADAPTER_POOL_SIZE` [DEFAULT 1]: number of persistent connections in connection pool
* `DB_ADAPTER_POOL_OVERFLOW` [DEFAULT 0]: whether to allow the pool to overflow during high load (dynamically allocates more connections when needed)
* `DB_ADAPTER_CONNECTION_TIMEOUT` [DEFAULT null]: duration after which a database connection-attempt times out
* `SQLITE_DB_FILE` [DEFAULT null]: SQLite db-file location (null corresponds to an in-memory database, only supports single connection and no overflow)
* `POSTGRES_DB_NAME` [DEFAULT null]: PostgreSQL database name
* `POSTGRES_DB_HOST` [DEFAULT null]: PostgreSQL database host
* `POSTGRES_DB_PORT` [DEFAULT null]: PostgreSQL database port
* `POSTGRES_DB_USER` [DEFAULT null]: PostgreSQL database user
* `POSTGRES_DB_PASSWORD` [DEFAULT null]: PostgreSQL database password
* `POSTGRES_DB_PASSFILE` [DEFAULT null]: PostgreSQL database passfile location

## Database
The `db`-subpackage requires the extra `db` (see above).

### Key-Value-Store implementation
The implementation is organized in multiple subpackages:
* `backend`: actual database implementations
  * `memory`: in-memory implementation without persistent data
  * `disk`: implementation that persists its data onto disk (in a working
    directory)
* `middleware`: provides creation of flask-apps (factory pattern) that implements the 'LZV.nrw - KeyValueStore-API' using a `backend`-component

  Running this app provides a shared database for multiple clients (ensures correct handling of concurrency).
  Minimal example:
  ```python
  from dcm_common.db import MemoryStore, key_value_store_app_factory

  app = key_value_store_app_factory(
    MemoryStore(), "db"
  )
  ```
* `adapter`: provides client-side access to key-value store databases regardless of native- or network-databases with a common interface
  * `native`: native python database (be aware that concurrent requests can lead to unexpected results)
  * `http`: network-database (like the flask-middleware provided here) that implements the 'LZV.nrw - KeyValueStore-API'

### SQL-adapter implementation
The implementation contains the definition of a common interface and two adapters for PostgreSQL (`PostgreSQLAdapterSQL14`, based on the `psycopg3`-package) and SQLite (`SQLiteAdapter3`, based on the standard library package `sqlite3`) databases.

Both adapters implement connection pooling and caching of basic database-schema information for methods like `get_table_names`.
The cache-size can be controlled with `DB_ADAPTER_SCHEMA_CACHE_SIZE` (default 64).

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
* Roman Kudinov