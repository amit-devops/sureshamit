import pytest
from app.common.utils import config


def test_parse_properties_and_secrets():
    mock_config = config.PropertiesConfig(None, None)
    raw_secrets = {"c.val": "super-secret-456"}
    raw_config = {
        "a": "123",
        "b": "${secrets.b.val::b-default-value}",
        "c": "${secrets.c.val}",
    }

    properties = mock_config.getProperties(raw_config, raw_secrets)
    assert properties["a"] == raw_config["a"]
    assert properties["b"] == "b-default-value"
    assert properties["c"] == raw_secrets["c.val"]


def test_failed_secret_lookup():
    mock_config = config.PropertiesConfig(None, None)
    raw_secrets = {}
    raw_config = {
        "a": "123",
        "b": "${secrets.b.val::b-default-value}",
        "c": "${secrets.c.val}",
    }

    with pytest.raises(config.ConfigParseError):
        mock_config.getProperties(raw_config, raw_secrets)


def test_failed_config_parse():
    mock_config = config.PropertiesConfig(None, None)

    with pytest.raises(config.ConfigParseError):
        mock_config.getProperties(None, None)
