"""Common test fixtures for DCM web-services."""

from typing import Callable, Optional
import urllib
from time import sleep, time
from uuid import uuid4
from multiprocessing import Process
from shutil import rmtree, copytree

import pytest
from flask import Flask


def _fs_setup(source, target):
    _fs_cleanup(target)
    copytree(source, target)


def _fs_cleanup(target):
    if target.is_dir():
        rmtree(target)


@pytest.fixture(scope="session", autouse=True)
def tmp_setup(fixtures, temp_folder):
    """Set up temp_folder"""
    _fs_setup(fixtures, temp_folder)


@pytest.fixture(scope="session", autouse=True)
def fs_setup(fixtures, file_storage):
    """Set up file_storage"""
    _fs_setup(fixtures, file_storage)


@pytest.fixture(scope="session", autouse=True)
def tmp_cleanup(request, temp_folder):
    """Clean up temp_folder"""
    request.addfinalizer(lambda: _fs_cleanup(temp_folder))


@pytest.fixture(scope="session", autouse=True)
def fs_cleanup(request, file_storage):
    """Clean up file_storage"""
    request.addfinalizer(lambda: _fs_cleanup(file_storage))


@pytest.fixture(name="wait_for_report")
def wait_for_report():
    def _(
        client, token, interval: float = 0.25, max_sleep: int = 250
    ):
        """Helper for report collection."""
        max_sleep = 250
        c_sleep = 0
        while c_sleep < max_sleep:
            sleep(interval)
            response = client.get(
                f"/report?token={token}"
            )
            if response.status_code == 200:
                break
            c_sleep = c_sleep + 1
        return response.json
    return _


@pytest.fixture(name="external_service")
def external_service() -> Callable:
    """
    Returns factory for an external service

    Use as
     >>> from flask import request
     >>> run_service(routes=[("/index", lambda: (jsonify(**request.args), 200), ["POST"])], port=8082)
     >>> print(requests.post("http://localhost:8082/index?arg=123").json())
    """

    def _(
        routes: list[tuple[str, Callable, list[str]]], app_config=None
    ) -> Flask:
        app = Flask(__name__)

        if app_config:
            app.config.from_object(app_config)

        for route, view, methods in routes:
            app.add_url_rule(
                route,
                endpoint=str(uuid4()),
                view_func=view,
                methods=methods
            )
        return app
    return _


@pytest.fixture(name="run_service")
def run_service(request, external_service) -> Callable:
    """
    Returns function that, if called, runs a flask-app in a separate
    process. Before returning, it is ensured that the app is responsive.

    It accepts either of the following
    * from_factory: call a factory to get the app (this is relevant if,
        for example, the factory executes other code that is needed to
        be run within the process where the app itself is running)
    * app: a pre-existing app
    * routes: a list of required endpoints from which an app is built
        dynamically; the resulting app is initialized with the
        `app_config` argument

    The sub-process lives only in pytest's 'function'-scope.
    """
    PROBING_PATH = "fixture-is-running"

    def _(
        app: Optional[Flask] = None,
        from_factory: Optional[Callable[[], Flask]] = None,
        port: str = 8080,
        routes: Optional[list[tuple[str, Callable, list[str]]]] = None,
        app_config=None,
        timeout: float = 5,
        probing_path: Optional[str] = None,
    ) -> Process:
        generate_probing_path = probing_path is None
        if generate_probing_path:
            probing_path = PROBING_PATH

        def run_process():
            if from_factory:
                _app = from_factory()
            else:
                _app = app or external_service(routes, app_config)

            if generate_probing_path:
                @_app.route(
                    f"/{PROBING_PATH}", methods=["GET"],
                    provide_automatic_options=False
                )
                def fixture_is_running():
                    """Used to probe whether service has started up."""
                    return "OK", 200

            _app.run(
                host="0.0.0.0",
                port=port,
                debug=False
            )
        p = Process(target=run_process)
        p.start()

        def kill_process():
            if p.is_alive():
                p.kill()
                p.join()
        request.addfinalizer(kill_process)

        # wait for service to have started up
        t0 = time()
        running = False
        while not running and time() - t0 < timeout:
            try:
                running = urllib.request.urlopen(
                    f"http://localhost:{port}/{probing_path}"
                ).status == 200
            except (urllib.error.URLError, ConnectionResetError):
                sleep(0.01)
        if not running:
            raise RuntimeError("Service did not start.")

        return p
    yield _
