import os
from pathlib import Path
from flask import Flask

from app.common.utils import create_app_status_file
from app.prediction.views.forecast_api import forecast_api
from app.controller.views.work_order_views import work_order_api
from app.controller.views.etl_views import etl_api
from app.controller.views.prediction_views import prediction_api
from app.common.views.health import health_api
from app.celery_utils import celery, make_celery
from app.common.utils.db import DataBaseCredential


def get_db_cred_from_app(
    app, db_name: str, is_read_only: bool = False
) -> DataBaseCredential:
    host_name = "_READ_DB_HOST" if is_read_only else "_DB_HOST"
    return DataBaseCredential(
        user=app.config[db_name + "_DB_USER"],
        password=app.config[db_name + "_DB_PASSWORD"],
        db_name=app.config[db_name + "_DB_NAME"],
        host=app.config[db_name + host_name],
        port=app.config[db_name + "_DB_PORT"],
        schema=app.config.get(db_name + "_DB_SCHEMA"),
    )


def create_directories(app):
    directories = ["FILES_DIRECTORY", "STAGED_ORDERS_PATH", "SENT_ORDERS_PATH"]
    for directory in directories:
        path = Path(app.config[directory])
        path.mkdir(parents=True, exist_ok=True)


def load_config(app):
    from app.config import ENV_TO_CONFIG

    deployment = os.getenv("DEPLOYMENT")
    if deployment is not None:
        env_flask_config_name = deployment
    else:
        env_flask_config_name = "local"
    config_class = ENV_TO_CONFIG[env_flask_config_name]
    app.config.from_object(config_class())


def create_workload_app(app):
    app.register_blueprint(work_order_api)
    app.register_blueprint(etl_api)
    app.register_blueprint(prediction_api)
    app.register_blueprint(health_api)
    app.control_db_cred = get_db_cred_from_app(app, "CONTROL")
    app.feeder_write_db_cred = get_db_cred_from_app(app, "FEEDER")
    app.prediction_db_cred = get_db_cred_from_app(app, "PREDICTION")
    app.feeder_read_db_cred = get_db_cred_from_app(
        app, "FEEDER", is_read_only=True
    )
    create_directories(app)


def create_denormalization_app(app):
    app.register_blueprint(health_api)
    app.feeder_write_db_cred = get_db_cred_from_app(app, "FEEDER")
    app.feeder_read_db_cred = get_db_cred_from_app(
        app, "FEEDER", is_read_only=True
    )
    create_directories(app)


def create_prediction_app(app):
    app.register_blueprint(forecast_api)
    app.register_blueprint(health_api)
    app.prediction_db_cred = get_db_cred_from_app(app, "PREDICTION")
    create_directories(app)


def load_services(app):
    app_mode = os.getenv("APP_MODE")
    app.control_service_name = app.config["CONTROL_SERVICE_NAME"]
    # Creating the app based on APP_MODE environment variable.
    if app_mode == "CONTROLLER":
        create_workload_app(app)
    elif app_mode == "DENORMALIZER":
        create_denormalization_app(app)
    elif app_mode == "PREDICTION":
        create_prediction_app(app)
    elif app_mode == "FLOWER":
        pass
    else:
        raise NotImplementedError(f"Invalid APP_MODE of {app_mode}")


def create_app():
    application = Flask(__name__)
    load_config(application)
    with application.app_context():
        load_services(application)
        make_celery(celery, application)
    create_app_status_file()
    return application
