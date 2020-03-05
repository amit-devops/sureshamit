from typing import List
from datetime import datetime
import pytest
from app.controller.models import WorkOrder, Prediction
from app.controller.constants import WorkOrderStatus, PredictionStatus


@pytest.fixture
def work_orders() -> List[WorkOrder]:
    work_orders = []
    for idx in range(10):
        work_orders.append(
            WorkOrder(
                work_order_id=idx,
                prediction_id=1,
                master_distributor_id=3,
                national_customer_id=5,
                create_date=datetime.now(),
                update_date=datetime.now(),
                status=WorkOrderStatus.CREATED.value,
            )
        )
    return work_orders


@pytest.mark.parametrize(
    "work_order_statuses, expected_status",
    [
        (
            [
                WorkOrderStatus.CREATED,
                WorkOrderStatus.CREATED,
                WorkOrderStatus.CREATED,
            ],
            PredictionStatus.CREATED,
        ),
        (
            [
                WorkOrderStatus.PREDICTION_SUCCESS,
                WorkOrderStatus.PREDICTION_SUCCESS,
                WorkOrderStatus.PREDICTION_SUCCESS,
            ],
            PredictionStatus.IN_REVIEW,
        ),
        (
            [
                WorkOrderStatus.DENORMALIZATION_FAILED,
                WorkOrderStatus.PREDICTION_FAILED,
                WorkOrderStatus.PREDICTION_FAILED,
            ],
            PredictionStatus.FAILED,
        ),
        (
            [
                WorkOrderStatus.PREDICTION_SUCCESS,
                WorkOrderStatus.PREDICTION_FAILED,
                WorkOrderStatus.PREDICTION_FAILED,
            ],
            PredictionStatus.IN_REVIEW,
        ),
        (
            [
                WorkOrderStatus.PREDICTION_SUCCESS,
                WorkOrderStatus.PREDICTION_FAILED,
                WorkOrderStatus.DENORMALIZATION_FAILED,
            ],
            PredictionStatus.IN_REVIEW,
        ),
        (
            [
                WorkOrderStatus.SENT_TO_PREDICTION,
                WorkOrderStatus.PREDICTION_FAILED,
                WorkOrderStatus.DENORMALIZATION_FAILED,
            ],
            PredictionStatus.IN_PROGRESS,
        ),
        (
            [
                WorkOrderStatus.CREATED,
                WorkOrderStatus.PREDICTION_SUCCESS,
                WorkOrderStatus.PREDICTION_IN_PROGRESS,
            ],
            PredictionStatus.IN_PROGRESS,
        ),
    ],
)
def test_update_prediction_status(
    mocker,
    work_orders: List[WorkOrder],
    expected_status: PredictionStatus,
    work_order_statuses: List[WorkOrderStatus],
) -> None:
    """Tests that the expected status is returned on the prediction"""
    work_orders = work_orders[:3]
    for idx, status in enumerate(work_order_statuses):
        work_orders[idx].status = status.value
    mocker.patch("app.controller.models.DatabaseCursor")
    mocker.patch("app.controller.models.app")
    mocker.patch(
        "app.controller.models.WorkOrder.get_work_orders_for_prediction",
        return_value=work_orders,
    )
    prediction = Prediction(
        prediction_id=work_orders[0].prediction_id,
        run_date=datetime.now(),
        prediction_date=datetime.now(),
        status=PredictionStatus.CREATED.value,
        update_date=datetime.now(),
        prediction_order=1,
    )
    prediction.update_status_in_db()
    assert prediction.status == expected_status.value
