# Changelog

## [3.18.0] - 2025-02-26

### Added

- added event-handling for `services.extensions`

### Fixed

- fixed delayed stop of `Daemon`-classes

## [3.17.0] - 2025-02-13

### Changed

- updated `sbom.md`

### Added

- added function to load external plugins from file
- added plugin system to Demo-service/API
- added plugin system utility-definitions for `services`-subpackage
- added function `util.qjoin`
- added initial plugin-system

## [3.15.1] - 2024-11-20

### Changed

- updated package metadata and README

## [3.15.0] - 2024-10-22

### Changed

- improved the response time when using `ScalableOrchestrator.abort` with `block=True` by skipping ahead to the cleanup in the processing loop

### Added

- added environment variable `ORCHESTRATION_PROCESSES` for orchestrator's vertical scaling

### Fixed

- fixed `ScalableOrchestrator`-debug mode via `ORCHESTRATION_DEBUG`

## [3.14.1] - 2024-10-16

### Fixed

- fixed an issue with the `services.notification.Topic`-constructor (did not set `db`-attribute)

## [3.14.0] - 2024-10-15

### Added

- added extended child-job class `ChildJobEx` (allows to fetch latest child-report after child-job has been aborted) (`17f7925f`)

## [3.13.0] - 2024-10-04

### Changed

- added concurrency-handling to `NativeKeyValueStoreAdapter` (`5ffaf96d`, `67ed7f6b`)

### Added

- added optional update-hooks for polling in `ServiceAdapter.run` (`1c07fb33`)

### Fixed

- fixed returned status code for `services.views.ReportView` in case of aborted jobs (`9fb957aa`)

## [3.11.1] - 2024-10-03

### Fixed

- fixed orchestration `Job` thread safety issue for abortion (`97f4defc`)

## [3.11.0] - 2024-10-03

### Changed

- improved `DataModel` serialization test-factory (`220c823f`)
- added support for `data-plumber-http` v1 (`0ebd3599`)

### Added

- added a `threading.RLock` to the `Job`-logic (used by `Children`, `push`, and can be requested for the job-cmd) (`08f1baca`)
- added default-deserialization support for `DataModel`-union types and optional `JSONObject`-attributes (`363b8641`, `f2a1c514`)
- added `services.extensions` subpackage (`e76261ec`, `d900054b`)
- added `pop`-keyword arg to `KeyValueStoreAdapter.read` (`2dd5f7b3`)
- added default abort-route helper to OrchestratedView (`d3187783`, `149eb0cd`)
- added demo-service API and application (`44098a27`, `45c1d3b7`)
- added common mechanisms for child job-abortion and abortion in parallel deployment (via Notification API) to orchestration module (`44098a27`, `59823b7e`, `b02ba0bb`)
- added initial Notification API and client (`44098a27`, `e8101da7`, `d1f22e03`)
- added methods `submit`, `poll`, and `_update_info_report` to `ServiceAdapter` interface (`f5be2218`)
- added `/progress`-endpoint to `ReportView` (`9e9fd1ce`)
- added PostgreSQL-adapter to `db`-package and support in `OrchestratedAppConfig` (`aa86a9de`, `d8f17557`)
- added methods `get_info` and `get_report` to `ServiceAdapter` interface (`2219be24`)

### Fixed

- fixed bad type-annotations of orchestration.JobConfig (`77e9c2cc`)
- fixed flask/werkzeug routing of `OPTIONS`-endpoints in `db`-package `middleware` and `services` package (`cc5f2ff8`)
- made `services.tests.run_service`-fixture more robust (`34cab458`)
- fixed `ScalableOrchestrator`'s processing loop skipping sleep if queue is empty (`18763711`)
- fixed issue with `ServiceAdapter`'s report finalization (`6c150b64`)
- fixed an issue with `DataModel`-default serialization of dictionary attributes (non-string keys) (`d25be5b`)
- fixed an issue with `DataModel`-default serialization of dictionary attributes (omitted `None`-values in output) (`a98fc02b`)

## [3.2.0] - 2024-09-11

### Changed

- improved multi-view support for `ScalableOrchestrator` and `OrchestratedView` (`6c179731`)

## [3.1.0] - 2024-09-10

### Added

- added common `ServiceAdapter` interface (`cf149d07`)

## [3.0.2] - 2024-09-09

### Fixed

- fixed issue with `DataModel`s containing `JSONObject` typehints due to missing `JSONable`-`TypeAlias` in local namespace of `from_json` (`54727b5`)

## [3.0.1] - 2024-09-09

### Fixed

- fixed broken `DataModel` handler-inheritance in rare cases (`f84f5bbd`)

## [3.0.0] - 2024-09-05

### Changed

- **Breaking:** renamed package from `lzvnrw-supplements` to `dcm-common` (`b8b63618`)
- **Breaking:** renamed module `supplements.py` to `util.py` (`cfd4d5fa`)
- refactored base-DataModel to implement defaults for (de-)serialization with an override-handler system (`d861680b`, `d7b1e2b6`, `ac31dbfd`)
- **Breaking:** refactored `views`- and `models`-subpackages for support of refactored `orchestation`-subpackage (`1cfac440`)
- **Breaking:** refactored `orchestation.py` into subpackage (with extra-dependencies) (`cd38db6a`)

### Added

- added `View`-class interface (`765912fd`)
- added type-check utility functions for JSONable (`c414eb40`)
- added k8s-deployment template/example (`fe08c325`)
- added `Daemon`-class (`6df17bb6`, `e179b37f`)
- added class `ScalableOrchestrator` (`80f7c70c`, `1cfac440`)
- added aliases `tmp_setup` and `tmp_cleanup` for the `service.tests`-fixtures `fs_setup` and `fs_cleanup` that accept `temp_folder` as target (`59c50dbb`)
- added `db` subpackage (db-backends: memory & disk, flask-middleware (with API-definition), and adapters for native and http-based database) (`76494849`, `17d42389`, `582ef749`, `7e21ffc3`, `f70ba2df`)
- added `AppConfig` variations `BaseConfig`, `FSConfig`, and `OrchestratedAppConfig` (`c271b690`, `34024af9`, `2f605e6d`, `517d38ec`)

### Removed

- **Breaking:** removed several uncommon util-functions (`3d2097c5`, `79ff8dbb`)
- **Breaking:** removed obsolete swagger-codegen-related sdk patch (`1043e841`)
- **Breaking:** removed obsolete `orchestation`-components (`8daad4ce`, `ac4269ba`)

### Fixed

- fixed `models.JSONable`-type (previous version available with `models.JSONObject`) (`a6679624`)

## [2.12.1] - 2024-07-24

### Fixed

- update progress messages in `services.hooks` (`f7f9bfe7`)

## [2.12.0] - 2024-07-19

### Changed

- extended `run_service`-pytest fixture with app factory-pattern (`02f224ce`)

## [2.11.0] - 2024-07-17

### Changed

- changed default self-description to new common format (`f221cd00`)
- refactored generation of `services.config.AppConfig`-self description dictionary (`41b55311`)

### Added

- added processing-loop `Orchestration.Orchestrator.run` (`0a36489d`)
- added `flatten` option to `Logger.fancy` (`dec4ea5c`)
- added `sort_by` option to `Logger.fancy` (`7ff9ce66`)

### Fixed

- fixed issue with `Orchestration.Job`-termination (`39020f95`, `697b8d3e`)

## [2.6.2] - 2024-05-29

### Removed

- removed print statement from function `load_txt_file_as_dict` (`e58f00f5`)

### Fixed

- fixed `services.tests.fixtures.run_service` fixture; now waits until app is actually available before returning (`2c3d8b76`)

## [2.6.0] - 2024-05-16

### Added

- added `services` subpackage defining common code for dcm-services (`43773967`)

## [2.5.0] - 2024-04-25

### Changed

- changed `LoggingContext` for `orchestration.Job`s from `INFO` to `EVENT` (`a19240d6`)

### Added

- added services subpackage with component sdk-patch (`4531c93e`)
- added optional name-argument to Job constructor (`13c027c0`)

### Fixed

- fixed `Job` properties' typehints (`e10b41d5`)
- fixed `Logger` defaults when constructing from json (`253d428a`)

## [2.0.0] - 2024-04-03

### Changed

- **Breaking:** replaced logging-module by new version (`43d4fdf9`, `ed5e6f1d`)
- **Breaking:** switched to running orchestration.Jobs in a multiprocessing.Process (`f631912c`, `ab440031`, `13f02475`, `43b83c16`)
- replaced static json profile by a generalized version (`c3d77d8e`)
- use fakes to test getting remote profile (`28dd4ee1`, `09b579b2`)

### Added

- added py.typed marker to package (`fcee8e14`)
- added data models subpackage (`55c8c896`)
- added abort functionality to orchestration-module (`f631912c`)

### Removed

- **Breaking:** removed module `input_processing` (`869e9ec5`)

### Fixed

- fixed issue with thread safety of job-submission in orchestration (`2ea2cb2e`)

## [1.9.0] - 2024-02-01

### Added

- added get-method to logger.BaseReportKey (`1883b737`)
- added more categories to logger.BaseReportKey (`1883b737`)

## [1.8.0] - 2024-01-17

### Added

- add orchestration-module (`b91f50d0`)

  missing merge of feature branch in version 1.7.0

## [1.7.0] - 2024-01-17

### Changed

- add optional argument `del_keys` to method `flush` of `BasicLogger` (`6588862a`)

### Added

- add input_processing-module (`d59ed39b`)
- add orchestration-module (`4b846ffc`)

## [1.4.0] - 2023-12-14

### Added 
- add method `copy_report` to `BasicLogger` (`3d4b8e45`)
- add (datetime hash-based) function `generate_identifier` (`2cfd8bf9`)
- add function `hash_from_file` and `hash_from_bytes` (`c98aaaaa`, `7c4a9019`)

## [1.1.0] - 2023-11-15

### Added

- Add function and tests `value_from_dict_path_recursive` (`dd2cd47a`, `d24562f1`)

## [1.0.0] - 2023-11-10

### Changed

- Make `categories` argument of `BasicLogger.flush` optional (`a2476d89`, `7a7e4a21`, `bfc06034`)

### Added

- Add function `value_from_dict_path` (`fc6cb928`, `4cc1f80e`)

### Removed

- Remove function `get_bag_creation_time` (`a80df8b7`)

### Fixed

- Fix pipeline dependencies for build job (`078bbc6d`)

## [0.2.0] - 2023-10-18

### Added

- logger `flush()`-method
- datetimed-logger

### Changed

- improve test-coverage
- support for str and Path in filesystem operations
- general clean up in docstrings & typehinting

### Removed

- **Breaking:** remove `load_json_as_dict()` function (`c68a03b9`)
