from typing import Optional, List, Tuple, Dict
from time import sleep
from datetime import datetime, date
import requests
from more_itertools import first_true
from flask import current_app as app
from celery.utils.log import get_task_logger
from app.celery_utils import celery
from app.controller.notification_handler import NotificationHandler
from app.denormalizer.services.background import (
    run_denormalization_on_work_order,
)
from app.common.utils import zulu_time_format
from app.common.utils.db import run_query_with_db_conn, DataBaseCredential
from app.common.storage_center import get_storage_center
from app.controller.constants import (
    NotificationType,
    NotificationStatus,
    WorkOrderStatus,
    scan_source_lookup,
    FINAL_NOTIFICATION_ID,
)
from app.controller.models import WorkOrder, Prediction, Notification
from app.controller.constants import PredictionStatus
from app.common.exceptions import NotificationException
from app.common.exceptions import ControlDBException

logger = get_task_logger(__name__)


@celery.task(queue="default")
def process_notification(
    notification_id: int, notification_type_str: str
) -> List[int]:
    """
    Process notification:
        Step 1: If first notification of day
            -   Create predictions if first notification.
            -   Sync specific Feeder Tables to Prediction DB
        Step 2: Run SPs to populate dimensional table in feeder
        Step 3: Update prediction db with new data from feeder
        Step 4: Get work order related to notification and
            put them in the feeder queue .i.e submit celery
            task to be denormalized
        Note: notification_type passed as str because celery
            can't deserialize enum as currently configured
    """
    notification_type = NotificationType(notification_type_str)
    notification_handler = NotificationHandler(
        notification_id=notification_id, notification_type=notification_type
    )
    validate_error = notification_handler.validate()
    if not validate_error:
        is_first = notification_handler.is_first_notification(
            create_date=datetime.utcnow().date()
        )
        notification = notification_handler.create_notification_in_db()
        try:
            if is_first:
                create_prediction_in_feeder(
                    run_date=datetime.utcnow().date(),
                    file_id=process_notification.request.id,
                )
                sync_tables_to_prediction(
                    process_notification.request.id,
                    queries_dict=first_notification_sync_queries,
                )
            if notification_type == NotificationType.DISTRIBUTOR:
                load_distributor_dimensional_tables(
                    db_credential=app.feeder_write_db_cred,
                    master_distributor_id=notification_id,
                )
            elif notification_type == NotificationType.RETAILER:
                load_retailer_dimensional_tables(
                    db_credential=app.feeder_write_db_cred,
                    national_customer_id=notification_id,
                )
            else:
                raise NotificationException(
                    f"Unknown notification type {notification_type} found."
                )
            # TODO replace sleep with query to ensure Feeder read db has
            #  been synced from write db. current thinking is use id returned
            #  from dimensional table load SPs
            sleep(5)
            sync_tables_for_notification_to_prediction(
                file_grouping_str=process_notification.request.id,
                notification_type=notification_type,
            )
            notification.save_status_to_db(NotificationStatus.LOADED)
        except Exception as e:
            notification.save_status_to_db(NotificationStatus.FAILED)
            raise e
    else:
        logger.info(validate_error)
    work_orders = notification_handler.get_work_orders_for_notification()
    if work_orders:
        send_worker_orders_to_feeder_queue(work_orders)
    else:
        logger.info(
            f"No work orders found after loading "
            f"distributor {notification_id}"
        )
    return [work_order.work_order_id for work_order in work_orders]


@celery.task(queue="default")
def process_next_prediction(run_date: Optional[date] = None) -> Optional[dict]:
    """ Checks if a prediction is in progress if not starts work-orders that
    are ready for the next prediction on the specified run_date"""
    if not run_date:
        run_date = datetime.utcnow().date()
    work_orders: List[WorkOrder] = []
    current_prediction, next_prediction = get_current_and_next_prediction(
        run_date
    )
    if not current_prediction and next_prediction:
        final_notification = Notification.get_from_db_by_create_date(
            id=FINAL_NOTIFICATION_ID,
            type=NotificationType.FINAL,
            create_date=run_date,
        )
        # Send all work_orders if we had a final notification
        if final_notification:
            work_orders = WorkOrder.get_work_orders_for_prediction(
                prediction_id=next_prediction.prediction_id,
                status=WorkOrderStatus.CREATED.value,
            )
        # Send only work_orders with loaded notifications
        else:
            loaded_distributors = (
                Notification.get_loaded_distributors_by_date()
            )
            loaded_retailers = Notification.get_loaded_retailers_by_date()
            if loaded_distributors and loaded_retailers:
                work_orders = WorkOrder.get_orders_for_notification_update(
                    master_distributor_ids=loaded_distributors,
                    national_customer_ids=loaded_retailers,
                    prediction_id=next_prediction.prediction_id,
                )
        if work_orders:
            send_worker_orders_to_feeder_queue(work_orders)
            return next_prediction.to_dict()
    return None


@celery.task(queue="default")
def process_final_etl_notification(run_date: Optional[date] = None) -> None:
    """Starts work-orders in the CREATED state for
    current or next prediction."""
    if not run_date:
        run_date = datetime.utcnow().date()

    if not Notification.get_from_db_by_create_date(
        type=NotificationType.FINAL,
        id=FINAL_NOTIFICATION_ID,
        create_date=run_date,
    ):
        Notification.create_notification_in_db(
            FINAL_NOTIFICATION_ID,
            NotificationType.FINAL,
            NotificationStatus.LOADED,
        )
    else:
        logger.warning(
            "Final ETL notification already received today. Proceeding anyways"
        )
    current_prediction, next_prediction = get_current_and_next_prediction(
        run_date
    )
    prediction = current_prediction or next_prediction
    if prediction:
        work_orders = WorkOrder.get_work_orders_for_prediction(
            prediction_id=prediction.prediction_id,
            status=WorkOrderStatus.CREATED.value,
        )
        if work_orders:
            send_worker_orders_to_feeder_queue(work_orders)


@celery.task(queue="default")
def run_create_prediction(run_datetime_str: str) -> str:
    """
    Creates predictions for the specified datetime
    :param run_datetime_str: datetime to create prediction for
    :return:
    """
    run_date = datetime.strptime(run_datetime_str, zulu_time_format).date()
    file_id = run_create_prediction.request.id
    create_prediction_in_feeder(run_date=run_date, file_id=file_id)
    return (
        "Successfully processed request to create work orders "
        f"and predictions for {run_date}"
    )


def send_worker_orders_to_feeder_queue(work_orders: List[WorkOrder]) -> None:
    logger.info(f"Sending {len(work_orders)} work_orders to feeder queue.")
    for work_order in work_orders:
        task = run_denormalization_on_work_order.delay(
            work_order.work_order_id
        )
        requests.post(
            f"http://{app.control_service_name}/api/work_order/status/",
            json={
                "work_order_id": work_order.work_order_id,
                "status": WorkOrderStatus.SENT_TO_FEEDER.value,
                "message": "",
            },
            timeout=10,
        )
        logger.info(
            f"Starting task {task.id} for denormalization "
            f"of work_order {work_order.work_order_id}"
        )


def get_current_and_next_prediction(
    run_date: date
) -> Tuple[Optional[Prediction], Optional[Prediction]]:
    """Returns the current and next prediction if found else None"""
    # order_by_cols use to ensure predictions are returned in the order they
    #   should be run
    predictions_to_run = Prediction.get_all_from_db_by_run_date(
        run_date=run_date,
        order_by_cols="prediction_date, prediction_order",
        sort_direction="asc",
    )
    current_prediction = first_true(
        predictions_to_run,
        pred=lambda pred: pred.status == PredictionStatus.IN_PROGRESS.value,
    )
    next_prediction = first_true(
        predictions_to_run,
        pred=lambda pred: pred.status == PredictionStatus.CREATED.value,
    )
    return current_prediction, next_prediction


def load_retailer_dimensional_tables(
    db_credential: DataBaseCredential,
    national_customer_id: int,
    run_date: Optional[date] = None,
) -> None:
    scan_source = scan_source_lookup.get(national_customer_id)
    if scan_source:
        scan_source = f"'{scan_source}'"
    else:
        scan_source = "NULL"

    load_retailer_query = (
        f"select usp_load_retailer({national_customer_id},"
        f"{scan_source},{run_date or 'NULL'});"
    )
    logger.info(f"Running query: {load_retailer_query}")
    run_query_with_db_conn(db_credential, query=load_retailer_query)


def load_distributor_dimensional_tables(
    db_credential: DataBaseCredential,
    master_distributor_id: int,
    run_date: Optional[date] = None,
) -> None:
    load_distributor_query = (
        f"select usp_load_distributor("
        f"{master_distributor_id},{run_date or 'NULL'})"
    )
    logger.info(f"Running query: {load_distributor_query}")
    run_query_with_db_conn(db_credential, query=load_distributor_query)


def sync_tables_for_notification_to_prediction(
    file_grouping_str: str, notification_type: NotificationType
) -> None:
    """
    Feeder to Prediction DB sync done on every ETL notification
    """
    db_storage_center = get_storage_center(
        source_db_cred=app.feeder_read_db_cred,
        destination_db_cred=app.prediction_db_cred,
    )
    sync_queries = notification_sync_queries["common"]
    sync_queries.update(notification_sync_queries[notification_type])
    logger.info(
        f"Syncing prediction db for notification type "
        f"{notification_type.value}. Tables being update: "
        f"{sync_queries.keys()}"
    )
    for table_name, table_queries in sync_queries.items():
        max_id_query = table_queries[0]
        table_select_query = table_queries[1]
        file_name = f"{file_grouping_str}_{table_name}.csv"
        logger.info(f"Getting Max id using query: {max_id_query}")
        max_id_query_result = run_query_with_db_conn(
            app.prediction_db_cred, query=max_id_query
        )
        max_id = max_id_query_result[0][0] or 0
        export_query = table_select_query.format(max_id=max_id)
        logger.info(f"Exporting to {file_name} using query {export_query}")
        db_storage_center.export_query_from_source_to_file(
            file_name=file_name, query=export_query
        )
        logger.info(f"Importing into {table_name} from file {file_name}")
        db_storage_center.import_to_destination_from_file(
            file_name=file_name, table_name=table_name
        )
        logger.info(f"Removing file {file_name}")
        db_storage_center.remove_file(file_name=file_name)


def sync_tables_to_prediction(
    file_grouping_str: str, queries_dict: Dict[str, Tuple[str, str]]
) -> None:
    """
    Syncs Feeder tables to Prediction using the provided query dictionary
    """
    db_storage_center = get_storage_center(
        source_db_cred=app.feeder_read_db_cred,
        destination_db_cred=app.prediction_db_cred,
    )
    for table_name, table_queries in queries_dict.items():
        prediction_query = table_queries[0]
        feeder_query = table_queries[1]
        file_name = f"{file_grouping_str}_{table_name}.csv"
        logger.info(f"Running Feeder Query: {feeder_query}")
        db_storage_center.export_query_from_source_to_file(
            file_name=file_name, query=feeder_query
        )
        logger.info(f"Running Prediction Query: {prediction_query}")
        run_query_with_db_conn(
            app.prediction_db_cred,
            query=prediction_query,
            return_results=False,
        )
        logger.info(
            f"Importing into Prediction {table_name} from file {file_name}"
        )
        db_storage_center.import_to_destination_from_file(
            file_name=file_name, table_name=table_name
        )
        logger.info(f"Removing file {file_name}")
        db_storage_center.remove_file(file_name=file_name)


def create_prediction_in_feeder(run_date: date, file_id: str) -> None:
    """
        Runs stored procedure with run_date to populate
        work_order_items table in Feeder db, then exports file for
        import into control db work_order and prediction table.
        :return:
    """
    control_file_name = f"{file_id}_control-work_order.csv"
    prediction_file_name = f"{file_id}_control-predictions.csv"
    logger.info("Creating work orders and prediction schedules")
    prediction_creation_sp = (
        f"select usp_populate_work_order_items(" f"'{str(run_date)}');"
    )
    logger.info(f"Running stored procedure query: {prediction_creation_sp}")
    run_query_with_db_conn(
        app.feeder_write_db_cred, query=prediction_creation_sp
    )
    db_storage_center = get_storage_center(
        source_db_cred=app.feeder_write_db_cred,
        destination_db_cred=app.control_db_cred,
    )
    feeder_work_order_query = """
    select distinct prediction_id,
                    work_order_id,
                    oa_master_distributor_id as master_distributor_id,
                    national_customer_id,
                    create_date,
                    'CREATED' as status
                from work_order_items where prediction_id in ({pred_ids})
    """

    feeder_prediction_query = (
        "select prediction_id, run_date, prediction_date,"
        f" 'CREATED' as status, '{datetime.utcnow()}' as"
        " update_date, prediction_order from predictions "
        f"where run_date = '{run_date}'"
    )

    logger.info(f"Starting export for Prediction Schedule table ")
    db_storage_center.export_query_from_source_to_file(
        file_name=prediction_file_name, query=feeder_prediction_query
    )
    logger.info(f"Starting import for Prediction Schedule table ")
    db_storage_center.import_to_destination_from_file_nothing_on_conflict(
        columns=[
            ("prediction_id", "int"),
            ("run_date", "date"),
            ("prediction_date", "date"),
            ("status", "char(100)"),
            ("update_date", "timestamp"),
            ("prediction_order", "int"),
        ],
        table_name="predictions",
        file_name=prediction_file_name,
    )
    run_day_predictions = run_query_with_db_conn(
        app.control_db_cred,
        f"select * from predictions where run_date = '{run_date}'",
    )
    pred_ids_str = ",".join(
        [str(pred_row[0]) for pred_row in run_day_predictions]
    )
    if not pred_ids_str:
        raise ControlDBException(f"No predictions found for {run_date}.")
    logger.info(f"Starting export for Work Order table ")
    # Export to file from feeder
    db_storage_center.export_query_from_source_to_file(
        file_name=control_file_name,
        query=feeder_work_order_query.format(pred_ids=pred_ids_str),
    )
    logger.info(f"Starting import for Work Order table ")
    db_storage_center.import_to_destination_from_file_nothing_on_conflict(
        columns=[
            ("prediction_id", "int"),
            ("work_order_id", "int"),
            ("master_distributor_id", "int"),
            ("national_customer_id", "int"),
            ("create_date", "date"),
            ("status", "char(100)"),
        ],
        table_name="work_order",
        file_name=control_file_name,
    )

    for file_name in [control_file_name, prediction_file_name]:
        logger.info(f"Removing files {file_name}")
        db_storage_center.remove_file(file_name)


# Format for sync queries is:
#   destination_table_name: (prediction_query, feeder_query)
first_notification_sync_queries = {
    "categories": ("truncate table categories", "select * from categories"),
    "customer_distributor_category_triad": (
        "truncate table customer_distributor_category_triad",
        "select * from customer_distributor_category_triad",
    ),
    "customer_store_distributor_schedule": (
        "truncate table customer_store_distributor_schedule",
        "select * from customer_store_distributor_schedule",
    ),
    "work_order_items": (
        "truncate table work_order_items",
        "select * from work_order_items",
    ),
}

notification_sync_queries = {
    NotificationType.DISTRIBUTOR: {
        "oa_distributors": (
            "select max(oa_distributor_id) from oa_distributors",
            "select * from oa_distributors where oa_distributor_id > {max_id}",
        ),
        "customer_distributor_dyad": (
            "select max(customer_distributor_dyad_id) "
            "from customer_distributor_dyad",
            "select * from customer_distributor_dyad "
            "where customer_distributor_dyad_id > {max_id}",
        ),
        "customer_store_distributor_triad": (
            "select max(customer_store_distributor_triad_id) "
            "from customer_store_distributor_triad",
            "select * from customer_store_distributor_triad "
            "where customer_store_distributor_triad_id > {max_id}",
        ),
        "items": (
            "select max(item_id) from items",
            "select * from items where item_id > {max_id}",
        ),
        "distributor_items": (
            "select max(distributor_item_id) from distributor_items",
            "select * from distributor_items "
            "where distributor_item_id > {max_id}",
        ),
        "shipments": (
            "select max(shipment_id) from shipments",
            "select * from shipments where shipment_id > {max_id}",
        ),
        "customer_store_item_distributor_dyad": (
            "select max(customer_store_item_distributor_dyad_id) "
            "from customer_store_item_distributor_dyad",
            "select * from customer_store_item_distributor_dyad "
            "where customer_store_item_distributor_dyad_id > {max_id}",
        ),
        "spoils_adjustments": (
            "select max(spoils_adjustment_id) from spoils_adjustments",
            "select * from spoils_adjustments where "
            "spoils_adjustment_id > {max_id}",
        ),
        "conversion_factors": (
            "select max(conversion_factor_id) from conversion_factors",
            "select * from conversion_factors where "
            "conversion_factor_id > {max_id}",
        ),
    },
    NotificationType.RETAILER: {
        "oa_scans": (
            "select max(oa_scan_id) from oa_scans",
            "select * from oa_scans where oa_scan_id > {max_id}",
        ),
        "oa_scans_sales": (
            "select max(oa_scan_sales_id) from oa_scans_sales",
            "select * from oa_scans_sales "
            "where oa_scan_sales_id > {max_id}",
        ),
    },
    "common": {
        "oa_stores": (
            "select max(oa_store_id) from oa_stores",
            "select * from oa_stores where oa_store_id > {max_id}",
        ),
        "customer_store_item_triad": (
            "select max(customer_store_item_triad_id) "
            "from customer_store_item_triad",
            "select * from customer_store_item_triad "
            "where customer_store_item_triad_id > {max_id} ",
        ),
    },
}
