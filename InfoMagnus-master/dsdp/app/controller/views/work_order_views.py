from typing import Tuple, Dict
from flask import Blueprint, request, jsonify
from flask import current_app as app
from marshmallow import Schema, fields
from marshmallow.validate import OneOf

from app.denormalizer.services.background import (
    run_denormalization_on_work_order,
)
from app.prediction.services.background import run_prediction_on_work_order

from app.controller.constants import WorkOrderStatus
from app.controller.models import WorkOrder
from app.common.utils import zulu_time_format, validate_request

work_order_api = Blueprint("controller_api", __name__)


@work_order_api.route("/api/work_order/status/", methods=["POST"])
def work_order_status_update() -> Tuple[Dict, int]:
    """Updates status for work_order in database and triggers
    prediction run if status changed to Denormalization_Success.
    Note: Used by prediction and denormalization containers"""

    class RequestSchema(Schema):
        work_order_id = fields.Int()
        status = fields.Str(validate=OneOf(WorkOrderStatus.__members__))
        message = fields.Str(default="")

    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "data": request.json}), 400

    work_order_id = request.json.get("work_order_id")
    status = request.json.get("status")
    message = request.json.get("message", "")
    app.logger.info(
        f"Updating status for work_order {work_order_id}" f" to {status}"
    )

    status = WorkOrderStatus(status)
    work_order = WorkOrder.get_from_db_by_id(work_order_id=work_order_id)
    if not work_order:
        return (
            jsonify({"error": f"WorkOrder {work_order_id} not found"}),
            404,
        )
    work_order.save_status_to_db(status, status_message=message)
    if status == WorkOrderStatus.DENORMALIZATION_SUCCESS:
        task = run_prediction_on_work_order.delay(work_order_id)
        app.logger.info(
            f"Created prediction task {task.id} for "
            f"work_order {work_order_id}"
        )
        work_order.save_status_to_db(WorkOrderStatus.SENT_TO_PREDICTION, "")
    work_order.update_prediction_status()
    return jsonify({"work_order": work_order.to_dict()})


@work_order_api.route("/api/work_order/<work_order_id>/")
def get_work_order(work_order_id: int) -> Tuple[Dict, int]:
    """
    :param work_order_id: work_order_id for which the control DB should
    be queried.
    :return: Queries the control database for the work_order ID provided
    in the url and returns json data for that work_order.
    """
    work_order = WorkOrder.get_from_db_by_id(work_order_id=work_order_id)
    if not work_order:
        return (
            jsonify({"error": f"WorkOrder {work_order_id} not found"}),
            404,
        )
    return jsonify({"work_order": work_order.to_dict()})


@work_order_api.route("/api/work_order/denormalize/", methods=["POST"])
def denormalize_work_order() -> Tuple[Dict, int]:
    class RequestSchema(Schema):
        work_order_id = fields.Int()

    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "date": request.json}), 400

    work_order_id = request.json["work_order_id"]
    if not WorkOrder.work_order_exists(work_order_id):
        return jsonify({"error": "Work order ID doesn't exist"}), 404
    task = run_denormalization_on_work_order.delay(work_order_id)
    app.logger.info(
        f"Re-running denormalization on work_order: {work_order_id}. "
        f"TaskID: {task.id}"
    )
    return jsonify({"task_id": task.id}), 201


@work_order_api.route("/api/work_order/predict/", methods=["POST"])
def predict_work_order() -> Tuple[Dict, int]:
    """
    Forces a prediction to be done on a work_order for specified
    run date
    """

    class RequestSchema(Schema):
        work_order_id = fields.Int()
        run_datetime = fields.DateTime(format=zulu_time_format)

    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "data": request.json}), 400

    work_order_id = request.json["work_order_id"]
    run_datetime = request.json.get("run_datetime")
    work_order = WorkOrder.get_from_db_by_id(work_order_id)
    if not work_order:
        return jsonify({"error": "Work order ID doesn't exist"}), 404
    work_order.save_status_to_db(
        status=WorkOrderStatus.SENT_TO_PREDICTION,
        status_message="Prediction triggered from predict request",
    )
    task = run_prediction_on_work_order.delay(work_order_id, run_datetime)
    app.logger.info(
        f"Re-running prediction on work_order: {work_order_id}. "
        f"TaskID: {task.id}"
    )
    return jsonify({"task_id": task.id}), 201
