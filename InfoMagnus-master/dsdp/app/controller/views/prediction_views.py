from typing import Tuple, Dict
from flask import Blueprint, request, jsonify
from flask import current_app as app
from marshmallow import Schema, fields
from app.controller.services.background import (
    run_create_prediction,
    process_next_prediction,
)

from app.prediction.services.background import run_generate_and_send_report

from app.controller.constants import PredictionStatus
from app.controller.models import WorkOrder, Prediction
from app.common.utils import zulu_time_format, validate_request

prediction_api = Blueprint("prediction_api", __name__)


@prediction_api.route("/api/prediction/create/", methods=["POST"])
def create_predictions() -> Tuple[Dict, int]:
    """
    Runs SP to create prediction for specified date
    and creates the corresponding predictions and work_orders
    entries in the control db
    """

    class RequestSchema(Schema):
        run_date = fields.DateTime(format=zulu_time_format)

    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "data": request.json}), 400

    run_date_str = request.json.get("run_date")
    task = run_create_prediction.delay(run_date_str)
    app.logger.info(
        f"Created task {task.id} to create prediction and associated "
        f"work orders for run date {run_date_str}"
    )
    return jsonify({"task_id": task.id}), 200


@prediction_api.route("/api/prediction/<prediction_id>/", methods=["GET"])
def get_prediction(prediction_id: int) -> Tuple[Dict, int]:
    prediction = Prediction.get_from_db_by_id(prediction_id)
    if not prediction:
        error = f"Prediction {prediction_id} not found"
        return jsonify({"error": error}), 404
    return jsonify({"prediction": prediction.to_dict()}), 200


@prediction_api.route(
    "/api/prediction/<prediction_id>/complete/", methods=["POST"]
)
def complete_prediction(prediction_id: int) -> Tuple[Dict, int]:
    prediction = Prediction.get_from_db_by_id(prediction_id)
    if not prediction:
        error = f"Prediction {prediction_id} not found"
        return jsonify({"error": error}), 404
    if prediction.status != PredictionStatus.IN_REVIEW.value:
        error = (
            f"Prediction {prediction_id} is not in review "
            f"and cannot be completed"
        )
        return jsonify({"error": error}), 400
    prediction.save_status_to_db(PredictionStatus.COMPLETED)
    app.logger.info(
        f"Completed prediction {prediction_id} and starting "
        f"next prediction if available."
    )
    task = process_next_prediction.delay()
    return jsonify({"task_id": task.id}), 200


@prediction_api.route("/api/prediction/<prediction_id>/status/")
def get_prediction_status(prediction_id: int) -> Tuple[Dict, int]:
    prediction = Prediction.get_from_db_by_id(prediction_id)
    if not prediction:
        return (
            jsonify({"error": f"Prediction {prediction_id} not found"}),
            404,
        )
    work_orders = WorkOrder.get_work_orders_for_prediction(prediction_id)
    work_order_status_dict: Dict[str, int] = dict()
    for work_order in work_orders:
        work_order_status_dict[work_order.status] = (
            work_order_status_dict.get(work_order.status, 0) + 1
        )
    return (
        jsonify(
            {
                "prediction_id": int(prediction_id),
                "prediction_status": prediction.status,
                "status_totals": work_order_status_dict,
                "work_orders": [
                    work_order.to_dict() for work_order in work_orders
                ],
            }
        ),
        200,
    )


@prediction_api.route("/api/report/generate/", methods=["POST"])
def generate_prediction_report() -> Tuple[Dict, int]:
    class RequestSchema(Schema):
        run_datetime = fields.DateTime(format=zulu_time_format)
        regen = fields.Bool(default=False)

    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "data": request.json}), 400

    run_datetime_str = request.json["run_datetime"]
    regen_flag = request.json.get("regen")

    task = run_generate_and_send_report.delay(
        run_datetime_str=run_datetime_str, regen=regen_flag or False
    )
    return jsonify({"task_id": task.id}), 200
