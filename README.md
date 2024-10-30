# LZV.nrw Common Code
This package provides common functions and components for the project [lzv.nrw](https://lzv.nrw/).

## Setup
Install this package and its dependencies by issuing `pip install .` .

### Extra dependencies
#### Services
This package defines optional components related to flask-webservices.
These can be installed by entering `pip install ".[services]"` instead.

#### Database
Similarly, the `db`-subpackage imposes additional requirements.
These can be installed using `pip install ".[db]"`.

#### Orchestration
The `orchestration`-extra shares its additional requirements with the `db`-extra due to its dependence on the `db`-subpackage.

## Run tests
Install test-related dependencies with `pip install -r dev-requirements.txt`.

Run tests with `pytest -v -s` or `pytest -v -s --cov`.

### Run tests under [coverage](https://coverage.readthedocs.io/en/7.3.0/)
`coverage run -m pytest`\
Use `coverage report` to report on the results.\
Use `coverage html` to get annotated HTML listings detailing missed lines.


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

## Database
The `db`-subpackage requires the extra `db` (see above).

### Overview
Currently, `db` contains only `key_value_store`-type implementations.
This is itself organized in multiple subpackages:
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

### Intended usage in the LZV.nrw-project
The package is designed to seamlessly support both, local in-memory testing and actual deployment of DCM-services.
* For a local test-setup, the adapter for a native database (initialized with an in-memory backend) is used.
* In case of a deployment (e.g. with horizontal scaling and persistent data), the http-adapter is used in conjunction with a deployment of the middleware (internally using the `disk`-backend implementation).

Due to the common interface for adapters, the service implementation can be agnostic regarding the database (aside from initialization).
Furthermore, this approach can be easily extended with other databases like `Redis` by adding a corresponding adapter class.

# Contributors
* Sven Haubold
* Orestis Kazasidis
* Stephan Lenartz
* Kayhan Ogan
* Michael Rahier
* Steffen Richters-Finger
* Malte Windrath
