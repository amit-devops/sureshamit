from typing import Dict, Tuple
from flask import Blueprint, request, jsonify
from datetime import datetime
from app.common.utils import zulu_time_format
from app.prediction.services.background import run_nine_day_forecast
from flask.globals import current_app as app


forecast_api = Blueprint("prediction_api", __name__)


@forecast_api.route("/api/forecast/", methods=["POST"])
def call_forecast() -> Tuple[Dict, int]:
    forecast_date = request.json.get(
        "forecast_date", str(datetime.utcnow().date())
    )
    work_order_id = request.json.get("work_order_id")
    if not work_order_id:
        return (
            jsonify({"error": "work_order_id missing in request body"}),
            404,
        )
    try:
        datetime.strptime(forecast_date, zulu_time_format)
    except Exception as e:
        return (
            jsonify(
                {
                    "error": (
                        "Invalid forecast_date in request body. "
                        f"Expected format 'YYYY-MM-DD'{e}"
                    )
                }
            ),
            400,
        )
    task = run_nine_day_forecast.delay(
        forecast_conn=app.prediction_conn, forecast_date=forecast_date
    )
    app.logger.info(
        f"Created task {task.id} for forecasting for work_order_id: "
        f" {work_order_id}"
    )
    return jsonify({"task_id": task.id}), 200
