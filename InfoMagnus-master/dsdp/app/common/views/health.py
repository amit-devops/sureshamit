import os
from typing import Tuple, Dict, Optional, List
from flask import Blueprint, request, jsonify

from app.celery_utils import (
    check_local_celery_worker,
    start_local_celery_worker_listening,
    stop_local_celery_worker_listening,
)
from app.celery_utils import (
    revoke_and_terminate_running_tasks,
    resend_celery_tasks_to_queue,
)
from app.common.utils import get_app_status, set_app_status


celery_queue_lookup = {
    "CONTROLLER": "default",
    "PREDICTION": "prediction",
    "DENORMALIZER": "feeder",
}

health_api = Blueprint("health_api", __name__)


@health_api.before_app_request
def health_check_test() -> Optional[Tuple[Dict, int]]:
    excluded_paths = [
        "/api/health/up/",
        "/api/health/down/",
        "/api/health/check/",
    ]
    error = "Service is NOT running"
    status = get_app_status()
    if request.path not in excluded_paths and status == "DOWN":
        return jsonify({"status": status, "error": error}), 500
    return None


@health_api.route("/api/health/check/")
def health_check() -> Tuple[Dict, int]:
    error = check_local_celery_worker()
    if error:
        set_app_status("DOWN")
    else:
        set_app_status("UP")
    status_code = 500 if error else 200
    return jsonify({"status": get_app_status(), "error": error}), status_code


@health_api.route("/api/health/up/")
def health_up() -> Tuple[Dict, int]:
    error = ""
    app_mode = os.getenv("APP_MODE", "")
    queue = celery_queue_lookup.get(app_mode)
    if not queue:
        raise ValueError(f"Unknown app_mode found: {app_mode}")
    if not start_local_celery_worker_listening(queue=queue):
        error = f"Failed to start local celery work listening to {queue} queue"
        set_app_status("DOWN")
    else:
        set_app_status("UP")
    status_code = 500 if error else 200
    return jsonify({"status": get_app_status(), "error": error}), status_code


@health_api.route("/api/health/down/")
def health_down() -> Tuple[Dict, int]:
    set_app_status("DOWN")
    error = ""
    resent_task_ids: List[str] = []
    app_mode = os.getenv("APP_MODE", "")
    queue = celery_queue_lookup.get(app_mode)
    if not queue:
        raise ValueError(f"Unknown app_mode found: {app_mode}")
    if not stop_local_celery_worker_listening(queue=queue):
        error = f"Failed to stop celery worker from listening to {queue} queue"
        set_app_status("DOWN")
    else:
        revoked_tasks = revoke_and_terminate_running_tasks()
        resent_task_ids = resend_celery_tasks_to_queue(revoked_tasks)
    status_code = 500 if error else 200
    return (
        jsonify(
            {
                "status": get_app_status(),
                "error": error,
                "resent_task_ids": resent_task_ids,
            }
        ),
        status_code,
    )
