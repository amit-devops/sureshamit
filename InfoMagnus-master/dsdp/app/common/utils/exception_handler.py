from flask import jsonify
import logging


def handle_request_exception(message, status_code, exception=None):
    logging.error(exception)

    return (
        jsonify({"error_response": {"message": message}, "status": "error"}),
        status_code,
    )
