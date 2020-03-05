from __future__ import annotations
from datetime import datetime, date
from typing import Optional, List, Tuple
from flask import current_app as app
from psycopg2.extras import DictCursor, RealDictCursor
from app.controller.constants import WorkOrderStatus
from app.common.utils.db import DatabaseCursor
from app.controller.constants import (
    NotificationStatus,
    NotificationType,
    PredictionStatus,
)
from app.common.exceptions import ControlDBException


class Prediction:
    # TODO: Need to have a prediction_type column too in db
    #  for AUTOMATED and ADHOC predictions.
    _table_name = "predictions"

    def __init__(
        self,
        prediction_id: int,
        run_date: datetime,
        prediction_date: datetime,
        status: str,
        update_date: datetime,
        prediction_order: int,
    ) -> None:
        self.prediction_id = prediction_id
        self.run_date = run_date
        self.prediction_date = prediction_date
        self.status = status
        self.update_date = update_date
        self.prediction_order = prediction_order

    @classmethod
    def get_from_db_by_id(cls, prediction_id: int) -> Optional[Prediction]:
        query = f"SELECT * FROM {cls._table_name} WHERE prediction_id = %s"
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query, (prediction_id,))
            prediction = cursor.fetchone()
            if prediction:
                prediction = cls(**prediction)
        return prediction

    @classmethod
    def get_all_from_db_by_run_date(
        cls,
        run_date: date,
        order_by_cols: str = "prediction_date, prediction_order",
        sort_direction: str = "asc",
    ) -> List[Prediction]:
        query = (
            f"SELECT * FROM {cls._table_name} "
            f"WHERE run_date = %s "
            f"order by {order_by_cols} {sort_direction }"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query, (run_date,))
            predictions = cursor.fetchall()
        return [cls(**pred) for pred in predictions]

    def save_status_to_db(self, status: PredictionStatus):
        self.status = status.value
        query = (
            f"UPDATE {self._table_name} "
            "SET status = %s, update_date = %s "
            "WHERE prediction_id = %s"
        )
        with DatabaseCursor(app.control_db_cred) as cursor:
            cursor.execute(
                query, (status.value, datetime.utcnow(), self.prediction_id)
            )

    def update_status_in_db(self):
        """Determines and updates the status of the prediction using
        the associated work_order statuses"""
        status_set_for_created = {
            WorkOrderStatus.CREATED.value,
            WorkOrderStatus.VALIDATED.value,
        }
        status_set_for_in_review = {
            WorkOrderStatus.PREDICTION_FAILED.value,
            WorkOrderStatus.DENORMALIZATION_FAILED.value,
            WorkOrderStatus.PREDICTION_SUCCESS.value,
        }
        status_set_for_failed = {
            WorkOrderStatus.DENORMALIZATION_FAILED.value,
            WorkOrderStatus.PREDICTION_FAILED.value,
        }
        work_orders = WorkOrder.get_work_orders_for_prediction(
            prediction_id=self.prediction_id
        )
        work_order_statuses = {work_order.status for work_order in work_orders}
        # Case all work_orders have the same status
        if work_order_statuses.issubset(status_set_for_failed):
            status = PredictionStatus.FAILED
        elif work_order_statuses.issubset(status_set_for_created):
            status = PredictionStatus.CREATED
        elif (
            WorkOrderStatus.PREDICTION_SUCCESS.value in work_order_statuses
            and work_order_statuses.issubset(status_set_for_in_review)
        ):
            status = PredictionStatus.IN_REVIEW
        else:
            status = PredictionStatus.IN_PROGRESS

        self.save_status_to_db(status=status)

    def __str__(self):
        return str(self.to_dict())

    def to_dict(self):
        return self.__dict__


class WorkOrder:

    _table_name = "work_order"

    def __init__(
        self,
        work_order_id: int,
        prediction_id: int,
        master_distributor_id: int,
        national_customer_id: int,
        create_date: datetime,
        update_date: datetime,
        status: str,
    ):
        self.work_order_id = work_order_id
        self.prediction_id = prediction_id
        self.master_distributor_id = master_distributor_id
        self.national_customer_id = national_customer_id
        self.create_date = create_date
        self.update_date = update_date
        self.status = status

    @classmethod
    def get_from_db_by_id(cls, work_order_id) -> Optional[WorkOrder]:
        query = f"SELECT * FROM {cls._table_name} WHERE work_order_id = %s"
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query, (work_order_id,))
            work_order = cursor.fetchone()
            if work_order:
                work_order = cls(**work_order)
        return work_order

    @classmethod
    def get_orders_for_notification_update(
        cls,
        master_distributor_ids: List[int],
        national_customer_ids: List[int],
        prediction_id: int,
        status: str = WorkOrderStatus.CREATED.value,
    ) -> List[WorkOrder]:
        """
        Returns all work orders for a prediction that have a
        master_distributor and national_customer in the provided
        lists and matching status.
        """
        master_distributor_str = ",".join(map(str, master_distributor_ids))
        national_customers_str = ",".join(map(str, national_customer_ids))
        query = (
            f"SELECT * from {cls._table_name} WHERE status = '{status}' "
            f"AND prediction_id = {prediction_id} "
            f"AND master_distributor_id in ({master_distributor_str}) "
            f"AND national_customer_id in ({national_customers_str})"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query)
            work_orders = cursor.fetchall()
            if work_orders:
                work_orders = [cls(**work_order) for work_order in work_orders]
        return work_orders

    def _insert_work_order_history(self, message: str):
        query = (
            "INSERT INTO work_order_history "
            "(work_order_id, status, message)"
            " VALUES (%s, %s, %s)"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query, (self.work_order_id, self.status, message))

    def save_status_to_db(self, status: WorkOrderStatus, status_message: str):
        self.status = status.value
        query = (
            f"UPDATE {self._table_name} SET status = %s, update_date = %s"
            " WHERE work_order_id = %s"
        )
        with DatabaseCursor(app.control_db_cred) as cursor:
            cursor.execute(
                query, (status.value, datetime.utcnow(), self.work_order_id)
            )
        self._insert_work_order_history(message=status_message)

    def update_prediction_status(self) -> None:
        """Updates status of prediction based on work-orders and returns True if
        the status is in list of prediction completed statuses, else False"""
        prediction = Prediction.get_from_db_by_id(self.prediction_id)
        assert (
            prediction is not None
        ), f"Prediction {self.prediction_id} not found."
        prediction.update_status_in_db()

    @classmethod
    def get_work_orders_for_prediction(
        cls, prediction_id: int, status: Optional[str] = None
    ) -> List[WorkOrder]:
        if status:
            query = (
                f"SELECT * FROM {cls._table_name} where "
                f"prediction_id = {prediction_id} and "
                f"status = '{status.upper()}'"
            )
        else:
            query = (
                f"SELECT * FROM {cls._table_name} "
                f"where prediction_id = {prediction_id}"
            )
        with DatabaseCursor(app.control_db_cred, RealDictCursor) as cursor:
            cursor.execute(query)
            work_orders_for_prediction = cursor.fetchall()
        return [cls(**work_order) for work_order in work_orders_for_prediction]

    @classmethod
    def get_table_name(cls):
        return cls._table_name

    @classmethod
    def work_order_exists(cls, wid):
        with DatabaseCursor(app.control_db_cred) as cursor:
            cursor.execute(
                f"SELECT count(*) from {WorkOrder._table_name} "
                f"where work_order_id={int(wid)}"
            )
            wid_count = int(cursor.fetchone()[0])
        return wid_count == 1

    def to_dict(self):
        return self.__dict__


class Notification:

    _table_name = "notifications"

    def __init__(
        self,
        notification_id: int,
        id: int,
        type: str,
        status: str,
        create_date: datetime,
    ):
        self.notification_id = notification_id
        self.id = id
        self.type = type
        self.status = status
        self.create_date = create_date

    @classmethod
    def create_notification_in_db(
        cls,
        id: int,
        type: NotificationType,
        status: NotificationStatus = NotificationStatus.NEW,
        create_date: Optional[date] = None,
    ) -> Notification:
        if not create_date:
            create_date = datetime.utcnow().date()
        query = (
            f"INSERT INTO {cls._table_name} "
            f"(id, type, status, create_date)"
            " VALUES (%s, %s, %s, %s) RETURNING notification_id"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query, (id, type.value, status.value, create_date))
            notification_id = cursor.fetchone()[0]
        notification = cls.get_by_notification_id(notification_id)
        if notification is None:
            raise ControlDBException(
                f"Notification record failed to be created"
                f" for type: {type}, id: {id}"
            )
        return notification

    @classmethod
    def get_by_notification_id(
        cls, notification_id: int
    ) -> Optional[Notification]:
        query = (
            f"SELECT * FROM {cls._table_name} WHERE "
            f"notification_id = {notification_id};"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query)
            notification = cursor.fetchone()
        if notification:
            return cls(**notification)
        return None

    @classmethod
    def get_from_db_by_create_date(
        cls, id: int, type: NotificationType, create_date: date
    ) -> Optional[Notification]:
        query = (
            f"SELECT * FROM {cls._table_name} WHERE "
            f"type = '{type.value}' AND "
            f"id = {id} and create_date = '{create_date}'"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query)
            notification = cursor.fetchone()
        if notification:
            return cls(**notification)
        return None

    @classmethod
    def get_all_from_db_by_create_date(
        cls, create_date: date
    ) -> List[Notification]:
        query = (
            f"SELECT * FROM {cls._table_name} WHERE "
            f"create_date = '{create_date}'"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(query)
            notifications = cursor.fetchall()
        return [cls(**notification) for notification in notifications]

    def save_status_to_db(self, status: NotificationStatus):
        self.status = status.value
        query = (
            f"UPDATE {self._table_name} SET status = %s"
            " WHERE id = %s and type = %s"
        )
        with DatabaseCursor(app.control_db_cred) as cursor:
            cursor.execute(query, (status.value, self.id, self.type))

    @classmethod
    def get_loaded_retailer_distributor_pair(
        cls, create_date: Optional[date] = None
    ) -> Tuple[List[int], List[int]]:
        if not create_date:
            create_date = datetime.utcnow().date()
        retailer_query = (
            f"SELECT * FROM {cls._table_name} "
            f"WHERE create_date = '{create_date}' "
            f"AND type = '{NotificationType.RETAILER.value}'"
            f"AND status = '{NotificationStatus.LOADED.value}' "
        )

        distributor_query = (
            f"SELECT * FROM {cls._table_name} "
            f"WHERE create_date = '{create_date}' "
            f"AND type = '{NotificationType.DISTRIBUTOR.value}' "
            f"AND status = '{NotificationStatus.LOADED.value}'"
        )

        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(retailer_query)
            retailer_ids = [row["id"] for row in cursor.fetchall()]
            cursor.execute(distributor_query)
            distributor_ids = [row["id"] for row in cursor.fetchall()]

        return distributor_ids, retailer_ids

    @classmethod
    def get_loaded_retailers_by_date(
        cls, create_date: Optional[date] = None
    ) -> List[int]:
        if not create_date:
            create_date = datetime.utcnow().date()
        retailer_query = (
            f"SELECT * FROM {cls._table_name} "
            f"WHERE create_date = '{create_date}' "
            f"AND type = '{NotificationType.RETAILER.value}'"
            f"AND status = '{NotificationStatus.LOADED.value}' "
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(retailer_query)
            retailer_ids = [row["id"] for row in cursor.fetchall()]

        return retailer_ids

    @classmethod
    def get_loaded_distributors_by_date(
        cls, create_date: Optional[date] = None
    ) -> List[int]:
        if not create_date:
            create_date = datetime.utcnow().date()
        distributor_query = (
            f"SELECT * FROM {cls._table_name} "
            f"WHERE create_date = '{create_date}' "
            f"AND type = '{NotificationType.DISTRIBUTOR.value}' "
            f"AND status = '{NotificationStatus.LOADED.value}'"
        )
        with DatabaseCursor(app.control_db_cred, DictCursor) as cursor:
            cursor.execute(distributor_query)
            distributor_ids = [row["id"] for row in cursor.fetchall()]
        return distributor_ids

    def to_dict(self):
        return self.__dict__
