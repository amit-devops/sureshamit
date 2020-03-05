import pandas as pd
from math import ceil
from statistics import mean
from typing import List
from celery.utils.log import get_task_logger
from datetime import date, datetime, timedelta
from app.common.utils.db import (
    DataBaseCredential,
    DatabaseCursor,
    get_db_connection,
    psycopg_paused_thread,
    df_to_csv,
)
from app.common.exceptions import PredictionException

forecast_stg_table_name = "stg_order_forecasts"
logger = get_task_logger(__name__)


def sma(
    ts_list: List[int],
    num_to_average: int = 3,
    counter: int = 1,
    num_pred: int = 9,
) -> List[int]:
    """
    :param num_to_average: last n numbers to perform average on
    :param ts_list: time-series list on which mean is calculated
    :param counter: Number of times to perform mean
    :param num_pred: Number of last items to be returned
    :return: Elements created from recursive average of time-series
    """
    if ts_list is None or type(ts_list) != list:
        raise Exception("List is invalid")
    while counter < num_pred:
        ts_list.append(ceil(mean(ts_list[-num_to_average:])))
        counter += 1
    return ts_list[-num_pred:]


def create_csiti_forecast(
    csiti: int, time_series_list: List[int], last_known_date: str
) -> pd.DataFrame:
    """
    :param csiti: customer_store_item_triad_id for which forecast is performed
    :param time_series_list: Time-series list for last 3 weeks for that csiti
    :param last_known_date: Last scan for the csiti in database
    :return: A data frame with 7 days forecast data from time-series list
    """
    logger.debug(
        f"Creating data frame for customer_store_item_triad_id: {csiti}"
    )
    logger.debug(f"Last known date: {last_known_date}")
    results_set = sma(time_series_list)
    csiti_forecast_df = pd.DataFrame(
        {
            "customer_store_item_triad_id": csiti,
            "units": results_set,
            "forecast_date": pd.date_range(
                last_known_date, periods=len(results_set)
            ),
            "forecast_source": "Python",
            "create_date": str(datetime.utcnow()),
        }
    )
    logger.debug(
        f"Data frame created for customer_store_item_triad_id: {csiti}"
    )
    return csiti_forecast_df


def nine_day_forecast(
    work_order_id: int,
    forecast_db_cred: DataBaseCredential,
    run_date: str = str(date.today()),
) -> bool:
    """
    :param work_order_id: integer
    :param run_date: Date for which forecast needs to be run
    :param forecast_db_cred: forecast database credential
    :return: Loads the stg_order_forecasts order_forecasts tables with
            time-series data
    :explanation: Creates time series for a prediction_id which contains
                    list of customer_store_item_triad_ids.
                    Converts that data frame into in-memory CSV file and
                    inserts into database.
                    Deletes stale data for the csitis in the table.
                    Dumps newly inserted data into order_forecasts table
                    from Python source.
    """
    logger.info(f"Forecast started.")
    logger.info(f"Forecast date: {run_date}.")
    forecast_time_series_sp = (
        f"select * from udf_forecast_time_series"
        f"({work_order_id}, '{run_date}')"
    )
    with DatabaseCursor(forecast_db_cred) as cursor:
        max_refresh_query = """select max(refresh_date) from
                        dbo.retailer_last_scan_date a
                            inner join
                        dbo.work_order_items woi
                        on woi.work_group_id = a.work_group_id
                        and woi.work_order_id = {wid}""".format(
            wid=work_order_id
        )
        cursor.execute(max_refresh_query)
        refresh = cursor.fetchone()
        if refresh:
            refresh = str(refresh[0])
    logger.info(f"Last refresh date: {refresh}")
    if refresh is None or refresh != str(date.today()):
        try:
            with DatabaseCursor(forecast_db_cred) as cursor:
                cursor.execute(
                    f"select usp_refresh_retailer_last_scan_dt("
                    f"{work_order_id}, '{run_date}')"
                )
        except Exception as e:
            logger.error(
                "Error while executing SP: usp_refresh_retailer_last_scan_dt"
            )
            raise e
    logger.info(f"Reading last scans data")
    with get_db_connection(forecast_db_cred) as conn:
        last_scans_query = """
                            select * from dbo.retailer_last_scan_date a
                                inner join
                            dbo.work_order_items woi
                                on
                            woi.work_group_id = a.work_group_id
                                and
                            woi.work_order_id = {wid}
                            """.format(
            wid=work_order_id
        )
        last_scans = pd.read_sql_query(last_scans_query, conn)
        logger.info(f"Retrieving data for SP: {forecast_time_series_sp}")
        order_data_df = pd.read_sql_query(forecast_time_series_sp, conn)
    logger.info(f"Creating time series for customer_store_item_triad_ids")
    forecast_object_list = (
        order_data_df.groupby("customer_store_item_triad_id")["ttlu"]
        .apply(list)
        .to_dict()
    )
    df_of_forecast_list = []
    logger.info(f"Records to loop: {len(forecast_object_list)}")
    if not forecast_object_list:
        raise PredictionException("No records found for forecast")
    for csiti, time_series_list in forecast_object_list.items():
        last_known_date = last_scans[
            last_scans["customer_store_item_triad_id"] == csiti
        ].max_date.values[0] + timedelta(1)
        temporary_forecast_table = create_csiti_forecast(
            csiti, time_series_list, last_known_date
        )
        df_of_forecast_list.append(temporary_forecast_table)
    logger.info(
        f"Creating data frame for inserting in {forecast_stg_table_name}"
    )
    df = pd.concat(df_of_forecast_list)
    df["run_date"] = run_date
    headers, file = df_to_csv(df)
    csitis_data_to_delete = tuple(forecast_object_list.keys())
    try:
        with DatabaseCursor(forecast_db_cred) as cursor:
            # Deleting existing csitis from the table
            cursor.execute(
                "delete from {0} where "
                "customer_store_item_triad_id in {1}".format(
                    forecast_stg_table_name, csitis_data_to_delete
                )
            )
            # Inserting new data for csitis into table
            with psycopg_paused_thread():
                cursor.copy_from(
                    file, forecast_stg_table_name, sep=",", columns=headers
                )
            # Updating order_forecasts table with data from Python source
            cursor.execute(
                f"select usp_order_forecast_update({work_order_id},'Python',"
                f" '{run_date}')"
            )
    except Exception as e:
        logger.error(f"Error loading data into Prediction DB ")
        raise e
    logger.info(f"Forecast complete.")
    return True
