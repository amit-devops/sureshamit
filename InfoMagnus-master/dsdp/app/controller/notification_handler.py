import logging
from more_itertools import first_true
from typing import List, Optional
from datetime import datetime, date
from app.controller.models import WorkOrder, Notification, Prediction
from app.controller.constants import (
    NotificationType,
    NotificationStatus,
    PredictionStatus,
)
from app.common.exceptions import NotificationException

logger = logging.getLogger(__name__)


class NotificationHandler:
    """
    Process notifications from Workload Notification
    """

    def __init__(
        self, notification_id: int, notification_type: NotificationType
    ):
        self.notification_id = notification_id
        self.notification_type = notification_type
        self.notification: Optional[Notification] = None

    def validate(self) -> Optional[str]:
        existing_notification = Notification.get_from_db_by_create_date(
            id=self.notification_id,
            type=self.notification_type,
            create_date=datetime.utcnow().date(),
        )
        if existing_notification:
            return (
                f"Existing notification found.  Ignoring notification: "
                f"{existing_notification.to_dict()}"
            )
        return None

    @staticmethod
    def is_first_notification(create_date: date) -> bool:
        notification = Notification.get_all_from_db_by_create_date(create_date)
        if not notification:
            logger.info("Notification is the first of the day")
        return not notification

    def create_notification_in_db(self) -> Notification:
        notification = (
            self.notification
        ) = Notification.create_notification_in_db(
            id=self.notification_id, type=self.notification_type
        )
        return notification

    def update_notification_to_loaded(self):
        self.notification.save_status_to_db(status=NotificationStatus.LOADED)

    def _get_work_orders_for_notification(
        self, run_date: Optional[date] = None
    ) -> List[WorkOrder]:
        """ Gets worker orders related to the notification that can now be
        released to work on. """
        if not run_date:
            run_date = datetime.utcnow().date()
        work_orders: List[WorkOrder] = []
        predictions_to_run = Prediction.get_all_from_db_by_run_date(
            run_date=run_date,
            order_by_cols="prediction_date, prediction_order",
            sort_direction="asc",
        )
        current_prediction = first_true(
            predictions_to_run,
            pred=lambda pred: pred.status
            in [
                PredictionStatus.CREATED.value,
                PredictionStatus.IN_PROGRESS.value,
            ],
        )
        if current_prediction:
            if self.notification_type == NotificationType.RETAILER:
                loaded_distributors = (
                    Notification.get_loaded_distributors_by_date()
                )
                if loaded_distributors:
                    work_orders = WorkOrder.get_orders_for_notification_update(
                        master_distributor_ids=loaded_distributors,
                        national_customer_ids=[self.notification_id],
                        prediction_id=current_prediction.prediction_id,
                    )
            elif self.notification_type == NotificationType.DISTRIBUTOR:
                loaded_retailers = Notification.get_loaded_retailers_by_date()
                if loaded_retailers:
                    work_orders = WorkOrder.get_orders_for_notification_update(
                        master_distributor_ids=[self.notification_id],
                        national_customer_ids=loaded_retailers,
                        prediction_id=current_prediction.prediction_id,
                    )
            else:
                raise NotificationException(
                    f"Unknown Type: {self.notification_type.value} found"
                )
        return work_orders

    def get_work_orders_for_notification(self) -> List[WorkOrder]:
        work_orders = self._get_work_orders_for_notification()
        return work_orders
