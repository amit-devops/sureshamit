import csv
import json
import smtplib
import paramiko
import requests
from celery.utils.log import get_task_logger
from os.path import basename
from typing import List, Tuple, Set
from datetime import datetime
from email.mime.text import MIMEText
from flask import current_app as app
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from app.prediction import sql_queries, models
from app.common.utils.db import (
    DataBaseCredential,
    DatabaseCursor,
    get_db_connection,
    get_list_of_tuples,
)
from app.common.exceptions import PredictionException

credit_weeks = 2
case_pack_factor = 0.75
d = models.base_dict["BaseOrder"]
logger = get_task_logger(__name__)
order_file = {
    "PrairieFarms": {"Headers": False, "ReportName": "SendOrders"},
    "SunnyFlorida": {"Headers": False, "ReportName": "SendOrders"},
}


class Connections:
    @staticmethod
    def sftp_conn(name):
        name = name.upper()
        try:
            t = paramiko.Transport(
                app.config[name + "_SFTP_HOST"],
                app.config[name + "_SFTP_PORT"],
            )
            t.connect(
                username=app.config[name + "_SFTP_USER"],
                password=app.config[name + "_SFTP_PASSWORD"],
            )
            sftp = paramiko.SFTPClient.from_transport(t)
            return sftp
        except Exception as e:
            msg = "Error while creating sftp object."
            logger.error(msg)
            raise PredictionException(e)


def send_email(
    db_name, account_name, send_to, subject, message, wid, file=None
) -> None:
    func_name = "send_email"
    account_name = account_name.upper()
    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = app.config[account_name + "_MAIL_LOGIN"]
        msg["To"] = send_to.replace(";", ", ")
        recipients = send_to.split(";")
        msg.attach(MIMEText(message))

        if file:
            with open(file, "rb") as file_obj:
                part = MIMEApplication(file_obj.read(), Name=basename(file))
            part[
                "Content-Disposition"
            ] = 'attachment; filename="%s"' % basename(file)
            msg.attach(part)

        smtp_obj = smtplib.SMTP(
            app.config[account_name + "_MAIL_HOST"],
            app.config[account_name + "_MAIL_PORT"],
        )
        smtp_obj.starttls()
        smtp_obj.login(
            app.config[account_name + "_MAIL_LOGIN"],
            app.config[account_name + "_MAIL_PASSWORD"],
        )
        smtp_obj.sendmail(
            app.config[account_name + "_MAIL_LOGIN"],
            recipients,
            msg.as_string(),
        )
        logger.info(
            f"{func_name}: Success for work_order_id:"
            f" {str(wid)} on db: {db_name}"
        )
    except smtplib.SMTPException as e:
        f"{func_name}: Failure for work_order_id: {str(wid)}. Reason: {e}"


def create_csv(list_of_tuples, directory_path, file_name, wid=None):
    func_name = "create_csv"
    try:
        with open(directory_path + file_name, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(list_of_tuples)
            logger.info(f"{func_name}: Success for work_order_id: {str(wid)}")
            return True
    except Exception as e:
        logger.error(
            f"{func_name}: Failure for work_order_id: {str(wid)}. Reason: {e}"
        )
        return False


def clean_up_and_prep(
    db_credential: DataBaseCredential, wid: int, run_date: datetime
) -> None:
    func_name = "clean_up_and_prep"
    query = sql_queries.queries["CleanUpAndPrep"].format(
        work_order_id=wid,
        run_date=str(run_date.date()),
        credit_weeks=credit_weeks,
    )
    try:
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(query)
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure for {query}. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def build_initial_orders(
    db_credential: DataBaseCredential, wid: int, run_datetime: datetime
) -> Tuple[List[models.BaseOrder], Set[str], Set[int]]:
    func_name = "build_initial_orders"
    query = sql_queries.queries["RecommendedDeliveryDates"].format(
        work_order_id=wid, timestamp=str(run_datetime)
    )
    categories = set()
    rec_delivery_dates = set()
    try:
        r = get_list_of_tuples(db_credential, query)
        order_collection = []
        for x in r:
            dd = d.copy()
            dd["oa_master_distributor_id"] = x[0]
            dd["customer_store_distributor_triad_id"] = x[1]
            dd["category_id"] = x[2]
            dd["rec_delivery_date"] = x[3]
            dd["create_date"] = x[4]
            dd["use_true_up"] = x[5]
            m = models.BaseOrder(**dd)
            order_collection.append(m)
            rec_delivery_dates.add(str(x[3]))
            categories.add(int(x[2]))
        logger.info(f"{func_name}: Success")
        return order_collection, rec_delivery_dates, categories
    except Exception as e:
        msg = f"{func_name}: Failure while executing {query}. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def remove_existing_orders(
    db_credential: DataBaseCredential, order_collection: List[models.BaseOrder]
) -> List[models.BaseOrder]:
    func_name = "remove_existing_orders"
    try:
        order_collection = [
            order
            for order in reversed(order_collection)
            if not order.check_for_order(db_credential)
        ]
        logger.info(f"{func_name}: Success")
        return order_collection
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def append_ship_delivery_dates(
    db_credential: DataBaseCredential,
    wid: int,
    order_collection: List[models.BaseOrder],
    run_datetime: datetime,
) -> List[models.BaseOrder]:
    func_name = "append_ship_delivery_dates"
    try:
        query = sql_queries.queries["LastShipDates"].format(
            work_order_id=wid, run_date=str(run_datetime)
        )
        records = get_list_of_tuples(db_credential, query)
        if len(records) > 0:
            for order in order_collection:
                for record in records:
                    if (
                        order.customer_store_distributor_triad_id == record[0]
                        and order.category_id == record[3]
                    ):
                        order.last_scheduled_delivery = record[2]
                        order.last_ship_date = record[1]
                        order.inc_in_anomaly = int(record[4])
                        order.inc_in_file = int(record[5])
                        order.inc_in_billing = int(record[6])
                        break
        logger.info(f"{func_name}: Success")
        return order_collection
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def add_last_deliveries(
    db_credential: DataBaseCredential,
    wid: int,
    order_collection: List[models.BaseOrder],
    run_datetime: datetime,
) -> List[models.BaseOrder]:
    func_name = "add_last_deliveries"
    try:
        last_deliveries_list = []
        delete_query = """
        delete from dbo.last_deliveries ld
            using
        dbo.work_order_items woi
            where
        woi.customer_store_distributor_triad_id =
        ld.customer_store_distributor_triad_id
            and
        woi.category_id = ld.category_id
            and
        woi.work_order_id = {work_order_id}
            and
        ld.run_date = '{create_date}'""".format(
            work_order_id=wid, create_date=str(run_datetime.date())
        )
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(delete_query)
        for order in order_collection:
            if not order.last_ship_date:
                # TODO update with correct value when last_ship_date is empty
                ship_date = datetime.min
            elif order.last_ship_date >= order.last_scheduled_delivery:
                ship_date = order.last_ship_date
            else:
                ship_date = order.last_scheduled_delivery
            last_deliveries_list.append(
                """
                    insert into dbo.last_deliveries
                    (
                         customer_store_distributor_triad_id
                        ,category_id
                        ,last_delivery
                        ,run_date
                        ,create_date
                    )
                        select
                    {csdti},
                    {category_id},
                    '{ship_date}', '{run_date}', '{current_date}'
                    """.format(
                    csdti=order.customer_store_distributor_triad_id,
                    category_id=order.category_id,
                    ship_date=ship_date,
                    run_date=run_datetime.date(),
                    current_date=datetime.utcnow(),
                )
            )
        with DatabaseCursor(db_credential) as cursor:
            for insert_query in last_deliveries_list:
                cursor.execute(insert_query)
        logger.info(f"{func_name}: Success")
        return order_collection
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def create_pre_orders(
    db_credential: DataBaseCredential,
    wid: int,
    order_collection: List[models.BaseOrder],
    run_datetime: datetime,
) -> List[models.BaseOrder]:
    func_name = "create_pre_orders"
    pre_orders = []
    records = []
    try:
        actual_scans_query = sql_queries.queries["AddActualScans"].format(
            work_order_id=wid, run_date=str(run_datetime)
        )
        records.extend(get_list_of_tuples(db_credential, actual_scans_query))
        for o in order_collection:
            matched_records = list(
                filter(
                    lambda x: o.customer_store_distributor_triad_id == x[0]
                    and o.category_id == x[1],
                    records,
                )
            )
            if not matched_records:
                logger.warning(
                    f"No matched_records in {func_name} for order: {o}"
                )
                continue
            for record in matched_records:
                dd = d.copy()
                dd["oa_master_distributor_id"] = o.oa_master_distributor_id
                dd[
                    "customer_store_distributor_triad_id"
                ] = o.customer_store_distributor_triad_id
                dd["category_id"] = o.category_id
                dd["rec_delivery_date"] = o.rec_delivery_date
                dd["create_date"] = o.create_date
                dd["last_scheduled_delivery"] = o.last_scheduled_delivery
                dd["last_ship_date"] = o.last_ship_date
                dd["use_true_up"] = o.use_true_up
                dd["inc_in_anomaly"] = o.inc_in_anomaly
                dd["inc_in_file"] = o.inc_in_file
                dd["inc_in_billing"] = o.inc_in_billing
                dd["customer_store_item_triad_id"] = record[2]
                dd["actual_scans"] = record[3]
                dd["last_scan_date"] = record[4]
                dd["customer_store_item_distributor_dyad_id"] = record[5]
                dd["customer_distributor_dyad_id"] = record[6]
                dd["customer_distributor_category_triad_id"] = record[8]
                dd["distributor_items_id"] = record[9]
                dd["over_under"] = record[10]
                tu = record[11]
                if o.use_true_up == "Suppress":
                    tu = 0
                dd["true_up_applied"] = tu
                m = models.BaseOrder(**dd)
                pre_orders.append(m)
        logger.info(f"{func_name}: Success")
        return pre_orders
    except Exception as e:
        logger.error(f"{func_name}: Failure. Reason: {e}")
        raise PredictionException(e)


def calc_forecasts(
    db_credential: DataBaseCredential,
    wid: int,
    order_collection: List[models.BaseOrder],
    run_datetime: datetime,
    rec_delivery_dates: Set[str],
) -> List[models.BaseOrder]:
    func_name = "calc_forecasts"
    records = []
    try:
        query = sql_queries.queries["ForecastOrders"]
        for rdd in rec_delivery_dates:
            forecast_orders_query = query.format(
                work_order_id=wid,
                rec_delivery_date=rdd,
                run_date=str(run_datetime.date()),
            )
            records.extend(
                get_list_of_tuples(db_credential, forecast_orders_query)
            )
        for order in order_collection:
            matched_records = list(
                filter(
                    lambda x: order.customer_store_distributor_triad_id == x[0]
                    and order.customer_store_item_triad_id == x[1]
                    and str(order.rec_delivery_date) == str(x[2]),
                    records,
                )
            )
            if len(matched_records) > 0:
                last_record = matched_records[-1]
                order.forecasted_scans = last_record[3]
                order.average_scans = last_record[4]
            else:
                logger.debug(
                    f"No matched_records in {func_name} for order: {order}"
                )
        logger.info(f"{func_name}: Success")
        return order_collection
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def create_base_orders(
    db_credential: DataBaseCredential,
    wid: int,
    order_collection: List[models.BaseOrder],
    run_datetime: datetime,
    average_threshold: int = 6,
) -> List[models.BaseOrder]:
    func_name = "create_base_orders"
    base_order_cnt = 0
    try:
        with DatabaseCursor(db_credential) as cursor:
            for o in order_collection:
                base_orders_creation_query = (
                    "select usp_create_base_order("
                    "{0},{1},{2},{3},{4},{5},{6},"
                    "{7},'{8}',{9},{10},{11},'{12}',"
                    "'{13}','{14}',{15},{16},{17})"
                )
                act = 0
                forecast = 0
                average_scans = 0
                model_used = "None"
                if o.actual_scans is not None and o.actual_scans != "":
                    if int(o.actual_scans) > 0:
                        act = int(o.actual_scans)
                if o.forecasted_scans is not None and o.forecasted_scans != "":
                    if int(o.forecasted_scans) >= 0:
                        forecast = int(o.forecasted_scans)
                        model_used = "Python"
                if o.average_scans is not None and o.average_scans != "":
                    if int(o.average_scans) > 0:
                        average_scans = int(o.average_scans)
                if (
                    average_scans >= average_threshold
                    or forecast > average_threshold
                ):
                    forecast = average_scans
                    model_used = "MovingAverage"
                base_order = act + forecast
                o.base_order = base_order
                adjustments = 0
                adjustments += o.get_over_under(cursor, str(run_datetime))
                if o.uses_true_up(cursor, run_datetime):
                    adjustments += o.get_true_up(cursor)
                porder = base_order + adjustments
                cf = o.get_conversion_factor(cursor, str(run_datetime))
                if cf is not None and porder > 0:
                    residual = o.get_conversion_residual(cursor)
                    if (residual + porder) < (cf * case_pack_factor):
                        if base_order != 0:
                            csiddi = o.customer_store_item_distributor_dyad_id
                            cr_insert_query = """
                                insert into dbo.conversion_residual
                                    (
                                    customer_store_item_distributor_dyad_id,
                                    residual_quantity,
                                    residual_date
                                    )
                                select {csiddi},{base_order},'{residual_date}'
                                """.format(
                                csiddi=csiddi,
                                base_order=base_order,
                                residual_date=run_datetime.date(),
                            )
                            cursor.execute(cr_insert_query)
                        porder = 0
                    else:
                        cr_update_query = """
                                update conversion_residual
                                    set
                                applied_date = '{applied_date}'
                                    where
                                customer_store_item_distributor_dyad_id =
                                 {csiddi}
                                    and
                                applied_date is NULL
                                """.format(
                            applied_date=str(run_datetime.date()),
                            csiddi=o.customer_store_item_distributor_dyad_id,
                        )
                        cursor.execute(cr_update_query)
                        porder = o.case_pack_adjustment(
                            cursor,
                            residual + porder,
                            case_pack_factor,
                            str(run_datetime),
                        )
                o.projected_order = porder
                abase_order = porder - adjustments
                boc_sql = base_orders_creation_query.format(
                    o.oa_master_distributor_id,
                    o.customer_distributor_dyad_id,
                    o.distributor_items_id,
                    o.customer_store_distributor_triad_id,
                    o.customer_store_item_distributor_dyad_id,
                    o.customer_store_item_triad_id,
                    o.customer_distributor_category_triad_id,
                    o.category_id,
                    o.rec_delivery_date,
                    act,
                    forecast,
                    abase_order,
                    run_datetime.date(),
                    datetime.utcnow(),
                    model_used,
                    bool(o.inc_in_anomaly),
                    bool(o.inc_in_file),
                    bool(o.inc_in_billing),
                )
                cursor.execute(boc_sql)
                base_order_cnt += 1
        logger.info(f"Created {base_order_cnt} Base Orders")
        final_clean_up_query = sql_queries.queries["FinalCleanUp"].format(
            work_order_id=wid, run_date=str(run_datetime)
        )
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(final_clean_up_query)
        logger.info(f"{func_name}: Success")
        return order_collection
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def prepare_adjustments(
    db_credential: DataBaseCredential, wid: int, run_datetime: datetime
) -> None:
    func_name = "prepare_adjustments"
    try:
        adjustments_query = sql_queries.queries["PrepareAdjustments"].format(
            work_order_id=wid, run_date=str(run_datetime)
        )
        override_query = sql_queries.queries["Overrride2"].format(
            work_order_id=wid, run_date=str(run_datetime.date())
        )
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(adjustments_query)
            cursor.execute(override_query)
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise Exception(msg)


def create_and_send_staged_orders(
    db_credential: DataBaseCredential,
    wid: int,
    send_mail: bool,
    run_datetime: datetime,
    body: str = "",
    adjust: str = "",
) -> None:
    func_name = "create_and_send_staged_orders"
    try:
        with get_db_connection(db_credential) as connection:
            dbname = connection.get_dsn_parameters().get("dbname")
        so = get_staged_orders(db_credential, wid, run_datetime)
        file_name = f"{str(run_datetime.date())}_{str(wid)}_{adjust}.csv"
        file_name = f"staged_orders_{file_name}"
        csv_created = create_csv(
            so, app.config["STAGED_ORDERS_PATH"], file_name, wid
        )
        body_text = str(len(so) - 1) + " Staged orders " + body
        slack_text = {"text": dbname + "\n" + body_text}
        if send_mail and csv_created:
            email_list = app.config["STAGED_ORDERS_RECIPIENTS"]
            send_email(
                dbname,
                "DSDREPORTS",
                email_list,
                f"Staged orders for {str(run_datetime.date())}",
                body_text,
                wid,
                app.config["STAGED_ORDERS_PATH"] + file_name,
            )
            webhook_url = app.config["SLACK_WEBHOOK_URL"]
            requests.post(
                webhook_url,
                data=json.dumps(slack_text),
                headers={"Content-Type": "application/json"},
            )
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure for WID: {str(wid)}. Reason: {e}"
        logger.error(msg)
        raise Exception(msg)


def create_distributor_file(
    db_credential: DataBaseCredential, run_datetime: datetime
) -> None:
    func_name = "create_distributor_file"
    try:
        distributor_file_query = sql_queries.queries["GetDistributors"].format(
            run_date=str(run_datetime)
        )
        distributors = get_list_of_tuples(
            db_credential, distributor_file_query
        )
        for dist in distributors:
            distributor = dist[0]
            transmit = dist[1]
            transmit_type = dist[2]
            report_name = order_file[distributor].get(
                "ReportName", "SendOrders"
            )
            headers = order_file[distributor].get("Headers", False)
            reports_query = sql_queries.reports[distributor][
                str(report_name)
            ].format(run_date=run_datetime.date())
            reports_list = get_list_of_tuples(
                db_credential, reports_query, bool(headers)
            )
            ds = (
                str(datetime.now())
                .replace("-", "")
                .replace(" ", "_")
                .replace(":", "")
            )
            ds = ds[: ds.find(".")]
            file_name = f"{distributor}_Orders_{ds}.csv"
            create_csv(reports_list, app.config["SENT_ORDERS_PATH"], file_name)
            logger.info(f"{func_name}: Success for {distributor}")
            if transmit:
                if transmit_type == "FTP":
                    sftp = Connections.sftp_conn(distributor)
                    sftp.chdir("DSDPOutbound")
                    sftp.put(
                        app.config["SENT_ORDERS_PATH"] + file_name, file_name
                    )
                    logger.info(
                        f"File transmitted for distributor {distributor} "
                        f"via FTP: {file_name}"
                    )
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def operator_adjustments(
    db_credential: DataBaseCredential, wid: int, run_datetime: datetime
) -> None:
    func_name = "operator_adjustments"
    try:
        operator_adjustments_query = sql_queries.queries[
            "OperatorAdjustments"
        ].format(work_order_id=wid, run_date=str(run_datetime.date()))
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(operator_adjustments_query)
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def get_staged_orders(
    db_credential: DataBaseCredential, wid: int, run_datetime: datetime
) -> List[Tuple]:
    func_name = "get_staged_orders"
    try:
        staged_orders_query = sql_queries.queries["StagedOrderReport"].format(
            work_order_id=wid, run_date=str(run_datetime)
        )
        records = get_list_of_tuples(
            db_credential, staged_orders_query, include_headers=True
        )
        logger.info(f"{func_name}: Success")
        return records
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def business_adjustments(
    db_credential: DataBaseCredential, wid: int, adjustment_type: str
) -> None:
    func_name = "business_adjustments"
    adjustment_dict = {
        "BA": sql_queries.queries["BusinessAdjustments"],
        "ML": sql_queries.queries["BusinessAdjustmentsPostML"],
        "Final": sql_queries.queries["BusinessAdjustmentsFinal"],
    }
    try:
        query = adjustment_dict.get(adjustment_type)
        assert (
            query is not None
        ), f"Adjustment type {adjustment_type} not found."
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(query.format(work_order_id=wid))
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)


def generate_orders(
    db_credential: DataBaseCredential, run_datetime: datetime, regen: bool
) -> None:
    func_name = "generate_orders"
    if not regen:
        generate_smart_order_sp = """select
        usp_generate_smart_order('{0}')""".format(
            run_datetime
        )
    else:
        generate_smart_order_sp = """select
        usp_generate_smart_order('{0}', 1)""".format(
            run_datetime
        )
    try:
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(generate_smart_order_sp)
        logger.info(f"{func_name}: Success")
    except Exception as e:
        msg = f"{func_name}: Failure. Reason: {e}"
        logger.error(msg)
        raise PredictionException(msg)
