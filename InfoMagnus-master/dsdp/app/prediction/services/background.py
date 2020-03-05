from typing import Optional
from datetime import datetime
import requests
from flask import current_app as app
from app.celery_utils import celery
from celery.utils.log import get_task_logger
from app.controller.constants import WorkOrderStatus
from app.prediction.run_prediction import (
    forecast_and_predict,
    generate_and_send_report,
)
from app.prediction.services.forecast import nine_day_forecast
from app.prediction.services.machine_learning import (
    update_checkpoint_files_from_s3,
)
from app.common.utils import zulu_time_format

logger = get_task_logger(__name__)


@celery.task(queue="prediction")
def run_prediction_on_work_order(
    work_order_id: int, run_datetime_str: Optional[str] = None
) -> celery.AsyncResult:
    """
    :param run_datetime_str:  Optional string of datetime in
        zulu format to use for run_date
    :param work_order_id: work_order_id to do prediction on.
    :return:
    """
    try:
        logger.info(f"Starting prediction for work_order {work_order_id}")
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order_id,
                "status": WorkOrderStatus.PREDICTION_IN_PROGRESS.value,
                "message": "",
            },
            timeout=10,
        )
        if app.config["ML_CHECKPOINT_STORAGE"] == "S3":
            update_checkpoint_files_from_s3(
                bucket_name=app.config["S3_BUCKET_NAME"],
                file_directory=app.config["FILES_DIRECTORY"],
                force_load=True,
            )
        if run_datetime_str:
            run_datetime = datetime.strptime(
                run_datetime_str, zulu_time_format
            )
        else:
            run_datetime = datetime.utcnow()
        logger.info(f"Running prediction with run_date {run_datetime}")
        result = forecast_and_predict(
            db_credential=app.prediction_db_cred,
            wid=work_order_id,
            run_datetime=run_datetime,
            file_directory=app.config["FILES_DIRECTORY"],
            send_mail=False,
        )

        # POST back to Work Load Controller we are done with Work Order
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order_id,
                "status": WorkOrderStatus.PREDICTION_SUCCESS.value,
                "message": "",
            },
            timeout=10,
        )
        return result
    except Exception as e:
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order_id,
                "status": WorkOrderStatus.PREDICTION_FAILED.value,
                "message": f"{e}",
            },
            timeout=10,
        )
        raise e


@celery.task(queue="prediction")
def run_generate_and_send_report(
    run_datetime_str: str, regen: bool
) -> celery.AsyncResult:
    run_datetime = datetime.strptime(run_datetime_str, zulu_time_format)
    generate_and_send_report(
        db_credential=app.prediction_db_cred,
        run_datetime=run_datetime,
        regen=regen,
    )


@celery.task(queue="prediction")
def run_nine_day_forecast(work_order_id: int, run_date: str) -> bool:
    return nine_day_forecast(
        work_order_id=work_order_id,
        forecast_db_cred=app.prediction_db_cred,
        run_date=run_date,
    )
