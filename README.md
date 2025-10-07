# Digital Curation Manager - Common Code Library
This package provides common functions and components for the [`Digital Curation Manager`](https://github.com/lzv-nrw/digital-curation-manager)-project.
This includes:
* `db`: database implementations and adapter definitions,
* `models`: data-model interface and common models,
* `orchestra`: job orchestration-system,
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
This extra depends on the [orchestra-subpackage](#orchestra) (the corresponding dependencies will be installed alongside the services-extra dependencies).

#### Database
The `db`-subpackage imposes additional requirements.
These can be installed using `pip install ".[db]"`.

#### orchestra
The `orchestra`-extra has additional requirements which can be installed with `pip install ".[orchestra]"`.

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
* `ORCHESTRA_WORKER_POOL_SIZE` [DEFAULT 1]: number of simultaneous worker threads
* `ORCHESTRA_AT_STARTUP` [DEFAULT 1]: whether to start worker pool immediately
* `ORCHESTRA_WORKER_INTERVAL` [DEFAULT 1]: worker-loop interval/interval between queue-polls
* `ORCHESTRA_DAEMON_INTERVAL` [DEFAULT 1]: interval for orchestra-daemon
* `ORCHESTRA_CONTROLLER` [DEFAULT "sqlite"]: controller type; possible values
  * `sqlite`: uses a SQLite3-database (in-memory or persistent)
  * `http`: connects to an orchestra-controller-API via HTTP
* `ORCHESTRA_CONTROLLER_ARGS` [DEFAULT "{}"]: additional controller arguments passed to the constructor as JSON
  * `sqlite`: the SQLite-controller supports the following arguments
    * `path`: path to a SQLite-database file
    * `memory_id`: identifier for a shared in-memory database
    * `name`: optional name tag for this controller (used in logging)
    * `requeue`: whether to requeue jobs that have failed
    * `lock_ttl`: time to live for a lock on a job in the job registry
    * `token_ttl`: time to live for a record in the job registry (null corresponds to no expiration)
    * `message_ttl`: time to live for a message (null corresponds to no expiration)
    * `timeout`: timeout duration for creating a database connection in seconds (mostly relevant for concurrency)
  * `http`: the HTTP-controller supports the following arguments
    * `base_url`: base url for controller API
    * `timeout`: request timeout in seconds
    * `name`: optional name tag for this controller (used in logging)
    * `max_retries`: number of retries if an HTTP-error occurs during a request
    * `retry_interval`: interval between retries in seconds
    * `request_kwargs`: additional kwargs that are passed when calling `requests.request`
* `ORCHESTRA_WORKER_ARGS` [DEFAULT "{}"]: additional worker arguments passed to the constructor as JSON
  * `name`: optional name tag for this worker (used in logging)
  * `process_timeout`: timeout for individual jobs in seconds; exceeding this value causes the worker to abort execution
  * `registry_push_interval`: interval for pushes of job results to the registry in seconds
  * `lock_refresh_interval`: interval for refreshes of locks on jobs in queue in seconds
  * `message_interval`: interval for the message-polling in seconds
* `ORCHESTRA_ABORT_TIMEOUT` [DEFAULT 30]: duration until a timeout-request times out
* `ORCHESTRA_LOGLEVEL` [DEFAULT "info"]: loglevel for components of the `orchestra`-package; possible values are "none", "error", "info", and "debug"
* `ORCHESTRA_MP_METHOD` [DEFAULT "spawn"]: method for creating child processes; see [discussion](https://discuss.python.org/t/concerns-regarding-deprecation-of-fork-with-alive-threads/33555/4)

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
    * requires a directory as argument `dir_` (pathlib-Path)
  * `sqlite`: SQLite3-based implementation with support for threading
    * can be used as persistent or transient database depending on arguments `path` and `memory_id`
    * accepts argument `timeout` (for connecting to database)
* `middleware`: provides creation of flask-apps (factory pattern) that implements the 'LZV.nrw - KeyValueStore-API' using a `backend`-component

  Running this app provides a shared database for multiple clients (ensures correct handling of concurrency).
  Minimal example:
  ```python
  from dcm_common.db import MemoryStore, key_value_store_app_factory

  app = key_value_store_app_factory(MemoryStore(), "db")
  ```
* `adapter`: provides client-side access to key-value store databases regardless of native- or network-databases with a common interface
  * `native`: native python database (be aware that concurrent requests can lead to unexpected results)
    * requires a key-value-store-backend as argument
  * `http`: network-database (like the flask-middleware provided here) that implements the 'LZV.nrw - KeyValueStore-API'
    * requires the `url`-argument (middleware-server)
    * accepts arguments `timeout` and `proxies` (see `requests`-library for details)

The utility-module of this package also provides helper-functions for initializing adapters (and potentially backends).
Using the function `load_adapter` from the module `dcm_common.db.key_value_store.util` can, for example, be used to initialize a `native`-adapter including its backend in a single call.
To this end, write
```python
adapter = load_adapter(
  "my-adapter",  # name
  "native",  # adapter-type
  {  # configuration of backend
    "backend": "sqlite",
    "path": "<path/to/file.db>",
    "timeout": 1
  },
)
```
For an `http`-adapter the settings are passed to the actual adapter instead:
```python
adapter = load_adapter(
  "my-adapter",  # name
  "http",  # adapter-type
  {  # configuration of http-adapter
    "url": "<url-to-key-value-store-api>",
  },
)
```

### SQL-adapter implementation
The implementation contains the definition of a common interface and two adapters for PostgreSQL (`PostgreSQLAdapter14`, based on the `psycopg3`-package) and SQLite (`SQLiteAdapter3`, based on the standard library package `sqlite3`) databases.

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