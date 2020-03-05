import pytest
import os
from unittest import mock

from app import create_app


@pytest.fixture()
@mock.patch("psycopg2.connect")
def work_load_client(psycopg2_connection):
    os.environ["APP_MODE"] = "CONTROLLER"
    os.environ["CONFIG_PATH"] = "config/testing"
    app = create_app()
    app.config["SECRET_KEY"] = "secret!"
    client = app.test_client()
    return client


@pytest.fixture()
@mock.patch("psycopg2.connect")
def prediction_client(psycopg2_connection):
    os.environ["APP_MODE"] = "PREDICTION"
    os.environ["CONFIG_PATH"] = "config/testing"
    app = create_app()
    app.config["SECRET_KEY"] = "secret!"
    client = app.test_client()
    return client


@pytest.fixture()
@mock.patch("psycopg2.connect")
def denormalization_client(psycopg2_connection):
    os.environ["APP_MODE"] = "DENORMALIZER"
    os.environ["CONFIG_PATH"] = "config/testing"
    os.environ["STORAGE"] = "LOCAL"
    app = create_app()
    app.config["SECRET_KEY"] = "secret!"
    client = app.test_client()
    return client
