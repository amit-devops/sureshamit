from typing import Tuple, Dict
from flask import Blueprint, request, jsonify
from flask import current_app as app
from marshmallow import Schema, fields
from app.controller.services.background import (
    process_notification,
    process_final_etl_notification,
)

from app.controller.constants import NotificationType
from app.common.utils import validate_request

etl_api = Blueprint("etl_api", __name__)


@etl_api.route("/api/etl/national_customer/", methods=["POST"])
def etl_national_customer() -> Tuple[Dict, int]:
    class RequestSchema(Schema):
        national_customer_id = fields.Int()

    app.logger.info(
        f"Received etl notification for national customer: {request.json}"
    )
    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "data": request.json}), 400

    task = process_notification.delay(
        notification_id=request.json.get("national_customer_id"),
        notification_type_str=NotificationType.RETAILER.value,
    )
    app.logger.info(
        f"Created task {task.id} for national_customer "
        f"notification: {request.json}"
    )
    return jsonify({"task_id": task.id, "data": request.json}), 200


@etl_api.route("/api/etl/master_distributor/", methods=["POST"])
def etl_master_distributor() -> Tuple[Dict, int]:
    class RequestSchema(Schema):
        master_distributor_id = fields.Int()

    app.logger.info(
        f"Received etl notification for master distributor: {request.json}"
    )
    error = validate_request(RequestSchema, request.json)
    if error:
        app.logger.error(error)
        return jsonify({"error": error, "data": request.json}), 400

    task = process_notification.delay(
        notification_id=request.json.get("master_distributor_id"),
        notification_type_str=NotificationType.DISTRIBUTOR.value,
    )
    app.logger.info(
        f"Created task {task.id} for master_distributor "
        f"notification: {request.json}"
    )
    return jsonify({"task_id": task.id, "data": request.json}), 200


@etl_api.route("/api/etl/final_notification/", methods=["POST"])
def final_etl_notification():
    task = process_final_etl_notification.delay()
    return jsonify({"task_id": task.id}), 200
