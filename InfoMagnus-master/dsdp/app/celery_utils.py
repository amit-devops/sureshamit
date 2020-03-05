import os
import ast
import socket
import logging
from typing import List, Dict
from celery import Celery
from flask import current_app as app

celery = Celery(__name__)

logger = logging.getLogger(__name__)


def make_celery(celery_app, flask_app):
    celery_app.conf.update(
        result_backend=flask_app.config["CELERY_RESULT_BACKEND"],
        broker_url=flask_app.config["CELERY_BROKER_URL"],
        worker_concurrency=flask_app.config["CELERYD_CONCURRENCY"],
    )

    class ContextTask(celery_app.Task):
        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return self.run(*args, **kwargs)

    celery_app.Task = ContextTask
    return celery_app


def check_local_celery_worker() -> str:
    global celery
    error = ""
    host_name = socket.gethostname()
    app_mode = os.getenv("APP_MODE")
    worker_name = f"{app_mode}@{host_name}"
    app.logger.info(f"Checking celery worker: {worker_name}")
    response = celery.control.ping([worker_name])
    if not response:
        error = f"Celery Worker {worker_name} is not responding to Ping"
        app.logger.error(error)
    else:
        work_queues = celery.control.inspect([worker_name]).active_queues()
        active_queues = work_queues[worker_name]
        if not active_queues:
            error = f"Work {worker_name} is not listening to any queues"
            app.logger.error(error)
    return error


def stop_local_celery_worker_listening(queue: str) -> bool:
    global celery
    host_name = socket.gethostname()
    app_mode = os.getenv("APP_MODE")
    worker_name = f"{app_mode}@{host_name}"
    app.logger.info(
        f"starting health check down for celery worker: {worker_name}"
    )
    response = celery.control.cancel_consumer(
        queue=queue, destination=[worker_name], reply=True
    )
    if response:
        return True
    app.logger.error(
        f"Failed health check down for celery worker: {worker_name}"
    )
    return False


def start_local_celery_worker_listening(queue: str) -> bool:
    global celery
    host_name = socket.gethostname()
    app_mode = os.getenv("APP_MODE")
    worker_name = f"{app_mode}@{host_name}"
    app.logger.info(
        f"Starting health check up for celery worker: {worker_name}"
    )
    response = celery.control.add_consumer(
        queue=queue, destination=[worker_name], reply=True
    )
    if response:
        return True
    app.logger.error(
        f"Failed health check up for celery worker: {worker_name}"
    )
    return False


def get_running_tasks() -> List[Dict]:
    global celery
    host_name = socket.gethostname()
    app_mode = os.getenv("APP_MODE")
    worker_name = f"{app_mode}@{host_name}"
    app.logger.info(f"Getting running tasks for: {worker_name}")
    tasks = celery.control.inspect([worker_name]).active()[worker_name]
    app.logger.info(f"found running tasks {tasks}")
    return tasks


def revoke_and_terminate_running_tasks(terminate: bool = True) -> List[Dict]:
    """Sends a revoke/terminate signal to the celery task"""
    tasks = get_running_tasks()
    for task in tasks:
        logger.info(f"Revoking and terminating task {task['id']}")
        celery.control.revoke(task["id"], terminate=terminate)
    return tasks


def resend_celery_tasks_to_queue(tasks: List[Dict]) -> List[str]:
    """Looks up the task function using the celery.signature and resend task to
     celery queue using previous arguments"""
    new_tasks: List[Celery.AsyncResult] = []
    for task in tasks:
        logger.info(f"Resending task: {task}")
        task_function = celery.signature(task["name"])
        args = ast.literal_eval(task["args"])
        kwargs = ast.literal_eval(task["kwargs"])
        new_tasks.append(task_function.delay(*args, **kwargs))
    return [t.id for t in new_tasks]
