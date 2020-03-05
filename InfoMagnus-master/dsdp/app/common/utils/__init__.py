from typing import Optional, Type
from pathlib import Path
from marshmallow import Schema, ValidationError

zulu_time_format = "%Y-%m-%d %H:%MZ"


def get_app_status() -> str:
    status_file = Path("/tmp/status")
    status = status_file.read_text()
    return status


def set_app_status(status: str) -> None:
    status_file = Path("/tmp/status")
    status_file.write_text(status)


def create_app_status_file() -> None:
    status_file = Path("/tmp/status")
    if not status_file.exists():
        status_file.write_text("UP")
        status_file.chmod(0o777)


def validate_request(schema_type: Type[Schema], data) -> Optional[dict]:
    try:
        schema = schema_type()
        schema.load(data)
        return None
    except ValidationError as err:
        return err.messages
