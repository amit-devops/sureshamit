import logging
import os
from app.common.utils.config import PropertiesConfig


class Config(object):
    """ Common config options """

    def __init__(self):

        # Path to .properties files
        self.config_path = os.getenv("CONFIG_PATH")

        # Relative config path
        if self.config_path and self.config_path[:1] != "/":
            logging.info("Using relative config path.")

        # No env variable foun
        if not self.config_path:
            self.config_path = "/etc/config/"
            logging.info("Using default config path.")

        # Load config values from .properties files
        self.properties = PropertiesConfig(self.config_path, "app").properties

        # Adding the properties from properties attribute dictionary to current
        # object so that they'll be loaded into flask's app.config
        for i, j in self.properties.items():
            setattr(self, i, j)


class DevelopmentConfig(Config):
    """ Dev environment config options """

    def __init__(self):
        super().__init__()

        self.FLASK_ENV = "deployed"
        self.DEBUG = True
        self.PROFILE = True
        self.SQLALCHEMY_ECHO = True


class TestingConfig(Config):
    """ Testing environment config options """

    def __init__(self):
        super().__init__()

        self.FLASK_ENV = "deployed"
        self.DEBUG = False
        self.STAGING = True
        self.TESTING = True


class ProductionConfig(Config):
    """ Prod environment config options """

    def __init__(self):
        super().__init__()

        self.FLASK_ENV = "deployed"
        self.DEBUG = False
        self.STAGING = False


ENV_TO_CONFIG = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
    "default": ProductionConfig,
    "local": Config,
}
