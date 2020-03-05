from datetime import datetime
from celery.utils.log import get_task_logger
from app.prediction import prediction_functions, sql_queries
from app.prediction.services.forecast import nine_day_forecast
from app.prediction.services import machine_learning
from app.common.utils.db import DatabaseCursor, DataBaseCredential

logger = get_task_logger(__name__)


def forecast_and_predict(
    db_credential: DataBaseCredential,
    wid: int,
    run_datetime: datetime,
    file_directory: str,
    send_mail: bool,
) -> bool:
    try:

        # Pre-steps:
        logger.info("Running pre-forecast queries")
        run_pre_forecast_queries(db_credential, run_datetime, wid)
        # 1. R file queries - forecast.py
        logger.info("Running seven day forecast")
        nine_day_forecast(wid, db_credential, str(run_datetime.date()))
        # 2. Moving Average forecast - sa_queries – OrderForecasts
        logger.info("Running moving average forecast")
        run_moving_average_forecast(db_credential, run_datetime, wid)
        # 3. clean up and prep - sa_queries – CleanUpAndPrep
        logger.info("Running clean up and prep")
        prediction_functions.clean_up_and_prep(
            db_credential, wid, run_datetime
        )
        # 4. build initial orders - sa_queries – RecommendedDeliveryDates
        logger.info("Building initial_orders")
        orders, delivery_dates, _ = prediction_functions.build_initial_orders(
            db_credential, wid, run_datetime
        )
        # 5. remove_existing_orders
        logger.info("Removing existing orders ")
        orders = prediction_functions.remove_existing_orders(
            db_credential, orders
        )
        # 6. append ship delivery dates - sa_queries – LastShipDates
        logger.info("Appending shipment delivery dates")
        orders = prediction_functions.append_ship_delivery_dates(
            db_credential, wid, orders, run_datetime
        )
        # 7. add last deliveries
        logger.info("Adding last deliveries")
        orders = prediction_functions.add_last_deliveries(
            db_credential, wid, orders, run_datetime
        )
        # 8. create pre-orders - sa_queries – AddActualScans
        logger.info("Creating pre-orders")
        orders = prediction_functions.create_pre_orders(
            db_credential, wid, orders, run_datetime
        )
        # 8a. calc forecasts - sa_queries – ForecastOrders
        logger.info("Calculating forecasts")
        orders = prediction_functions.calc_forecasts(
            db_credential, wid, orders, run_datetime, delivery_dates
        )
        # 8b.create base order
        logger.info("Creating Base Orders")
        prediction_functions.create_base_orders(
            db_credential, wid, orders, run_datetime
        )
        # 8c.prepare adjustments
        logger.info("Preparing adjustments")
        prediction_functions.prepare_adjustments(
            db_credential, wid, run_datetime
        )
        # 8e.create and send staged orders - sa_queries – StagedOrderReport
        logger.info("Creating and Sending staged orders")
        prediction_functions.create_and_send_staged_orders(
            db_credential,
            wid,
            send_mail,
            run_datetime,
            f"Elapsed time: {str(datetime.utcnow() - run_datetime)}",
        )
        # 8f.apply business adjustments - usp_BusinessAdjustments
        logger.info("Running Business adjustments")
        prediction_functions.business_adjustments(db_credential, wid, "BA")
        # 9a. Apply the adjustments - sa_queries – 10 – OperatorAdjustments
        logger.info("Operator Adjustments")
        prediction_functions.operator_adjustments(
            db_credential, wid, run_datetime
        )
        # 9b.Apply machine learning adjustments – code in the SOMLAdjustments
        logger.info("Running ML prediction")
        machine_learning.predict(
            db_credential=db_credential,
            work_order_id=wid,
            order_gen_datetime=run_datetime,
            base_file_directory=file_directory,
            insert=True,
            export=False,
        )
        # 10a. Apply the adjustments - sa_queries – 10 – OperatorAdjustments
        logger.info("Operator adjustments")
        prediction_functions.operator_adjustments(
            db_credential, wid, run_datetime
        )
        # 10b. Apply post ML business adjustments -
        logger.info("Running PostML adjustments")
        prediction_functions.business_adjustments(db_credential, wid, "ML")
        # 10c. Apply the adjustments - sa_queries – 10 – OperatorAdjustments
        logger.info("Running Operator adjustments (PostML)")
        prediction_functions.operator_adjustments(
            db_credential, wid, run_datetime
        )
        logger.info("Running Final adjustments")
        prediction_functions.business_adjustments(db_credential, wid, "Final")
        logger.info("Running Operator adjustments (Final)")
        prediction_functions.operator_adjustments(
            db_credential, wid, run_datetime
        )
        return True
    except Exception as e:
        logger.error("Exception while running forecast_and_predict")
        raise Exception(e)


def generate_and_send_report(
    db_credential: DataBaseCredential, run_datetime: datetime, regen: bool
) -> None:
    try:
        logger.info("Generating orders")
        prediction_functions.generate_orders(
            db_credential, run_datetime, regen=regen
        )
        logger.info("Creating and sending distributor files")
        # TODO: Update code after deciding how to process
        #  if there are failed predictions
        prediction_functions.create_distributor_file(
            db_credential, run_datetime
        )
    except Exception as e:
        logger.error("Exception while running generate_and_send_report")
        raise Exception(e)


def run_moving_average_forecast(
    db_credential: DataBaseCredential, run_datetime: datetime, wid: int
):
    with DatabaseCursor(db_credential) as cursor:
        ma_query = sql_queries.queries["OrderForecasts"].format(
            work_order_id=wid, run_date=run_datetime.date()
        )
        cursor.execute(ma_query)


def run_pre_forecast_queries(
    db_credential: DataBaseCredential, run_datetime: datetime, wid: int
):
    with DatabaseCursor(db_credential) as cursor:
        cursor.execute(
            f"select usp_refresh_retailer_last_scan_dt("
            f"{wid}, '{run_datetime.date()}')"
        )
        cursor.execute(f"select usp_refresh_item_averages({wid})")
        cursor.execute(
            f"select usp_populate_true_up_adjustments("
            f"{wid},'{run_datetime.date()}')"
        )
