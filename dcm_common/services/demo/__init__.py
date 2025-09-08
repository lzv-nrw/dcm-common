"""
- DCM Demo Service -
This flask app implements the 'Demo'-API (see `openapi.yaml`).
"""

from flask import Flask
from dcm_common.services import DefaultView, ReportView
from dcm_common.services import extensions

from .config import AppConfig
from .views import DemoView


def app_factory(
    config: AppConfig,
    as_process: bool = False
) -> Flask:
    """
    Returns a flask-app-object.

    config -- app config derived from `AppConfig`
    as_process -- whether the app is intended to be run as process via
                  `app.run`; if `True`, startup tasks like starting
                  orchestration-daemon are prepended to `app.run`
                  instead of being run when this factory is executed
                  (default False)
    """

    app = Flask(__name__)
    app.config.from_object(config)

    # create OrchestratedView-class
    view = DemoView(config)
    # and register job-types with the worker-pool
    view.register_job_types()

    # register extensions
    if config.ALLOW_CORS:
        app.extensions["cors"] = extensions.cors_loader(app)
    app.extensions["orchestra"] = extensions.orchestra_loader(
        app, config, config.worker_pool, "Demo", as_process
    )
    app.extensions["db"] = extensions.db_loader(
        app, config, config.db, as_process
    )

    # register blueprints
    app.register_blueprint(
        DefaultView(
            config, ready=app.extensions["orchestra"].ready.is_set
        ).get_blueprint(),
        url_prefix="/",
    )
    app.register_blueprint(
        view.get_blueprint(),
        url_prefix="/"
    )
    app.register_blueprint(
        ReportView(config).get_blueprint(),
        url_prefix="/"
    )

    return app
