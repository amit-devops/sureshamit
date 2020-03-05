import requests
from flask import current_app as app
from celery.utils.log import get_task_logger
from app.celery_utils import celery
from app.controller.constants import WorkOrderStatus

logger = get_task_logger(__name__)


@celery.task(queue="feeder")
def run_denormalization_on_work_order(
    work_order_id: int
) -> celery.AsyncResult:
    """ Run denormalization on work_order """
    try:
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order_id,
                "status": WorkOrderStatus.DENORMALIZATION_IN_PROGRESS.value,
                "message": "",
            },
            timeout=10,
        )
        response = requests.get(
            f"http://{app.control_service_name}/api/work_order/"
            f"{work_order_id}/",
            timeout=10,
        )
        work_order_data = response.json()
        logger.info(
            f"Processing work_order {work_order_id}: {work_order_data}"
        )
        # TODO add denormalization HERE
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order_id,
                "status": WorkOrderStatus.DENORMALIZATION_SUCCESS.value,
                "message": "",
            },
        )
        return work_order_id
    except Exception as e:
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order_id,
                "status": WorkOrderStatus.DENORMALIZATION_FAILED.value,
                "message": f"{e}",
            },
        )
        raise e
