import os
import re
from configobj import ConfigObj


class PropertiesConfig:
    placeholder_regex = re.compile(r"\$\{(.*)\}")

    def __init__(self, config_dir, module):

        if config_dir:
            self.config_path = os.path.abspath(
                os.path.join(config_dir, module + ".properties")
            )
            self.secrets_path = os.path.abspath(
                os.path.join(config_dir, "secrets.properties")
            )

            self.raw_config = ConfigObj(self.config_path)
            self.raw_secrets = ConfigObj(self.secrets_path)

            self.properties = self.getProperties(
                self.raw_config, self.raw_secrets
            )

    def getProperties(self, config, secrets):
        try:
            return {
                k: self.replacePlaceholder(v, secrets)
                for k, v in config.items()
            }
        except Exception as e:
            raise ConfigParseError(e)

    def getDefaultValue(self, placeholder_value):
        placeholder_parts = placeholder_value.split("::")
        default_value = (
            placeholder_parts[1] if len(placeholder_parts) > 1 else None
        )
        return default_value

    def getSecretReference(self, placeholder_value):
        return ".".join(placeholder_value.split("::")[0].split(".")[1:])

    def getSecretValue(self, placeholder_value, secrets):
        secret_ref = self.getSecretReference(placeholder_value)
        default_value = self.getDefaultValue(placeholder_value)
        secret_value = secrets.get(secret_ref, default_value)

        if secret_value is None:
            msg = f"Could not resolve secret value for {placeholder_value}"
            raise ConfigParseError(msg)

        return secret_value

    def replacePlaceholder(self, value, secrets):
        match = self.placeholder_regex.search(value)

        # No placeholder found, return original value
        if not match:
            return value

        # Placeholder found, replace with secret
        secret_value = self.getSecretValue(match.group(1), secrets)
        return value.replace(match.group(0), secret_value)


class ConfigParseError(Exception):
    """Raised when there is a failure to parse config files"""

    pass
