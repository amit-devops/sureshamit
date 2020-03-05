import os
import copy
from datetime import datetime
from typing import Any, Optional
from pathlib import Path
import boto3
import numpy as np
import pandas as pd
import tensorflow as tf
from celery.utils.log import get_task_logger
from app.common.utils.db import (
    DataBaseCredential,
    DatabaseCursor,
    df_to_csv,
    psycopg_paused_thread,
    get_db_connection,
)
from app.common.exceptions import PredictionException

logger = get_task_logger(__name__)

pd.options.mode.chained_assignment = None


x_columns = [
    "forecasted_scans",
    "ten_day_avg_quantity",  # base
    "true_up",
    "math_order",  # base
    "calculated_order",
    "case_packres",
    "inventory_change",  # base
    "date_of_week",  # base
    "case_pack",  # base
    "spoils_adjustments",
    "date_of_month",
    "actual_scans",
    "High Volume",
    "Low Volume",
    "Medium Volume",
    "Zeros",
]


update_poor_perfs_null_sql = """
       update dbo.stg_auto_operator_adjustment as a
            set operator_adjustment = null
        from
            dbo.base_order bo
           ,dbo.work_order_items woi
        where
           bo.base_order_id = a.base_order_id
           and woi.work_group_id = bo.work_group_id
           and woi.work_order_id = {0}
            and pred_value_null is not null
            and operator_adjustment is not null
    """

update_poor_perfs_sql = """
    do $$
    declare v_maxdt timestamp without time zone =
     (select max(date_time_created) from dbo.ml_poor_performers);
    begin
        with cte_a
        as
        (
           select * from dbo.ml_poor_performers
            where date_time_created = v_maxdt
        )
        update dbo.stg_auto_operator_adjustment as a
            set pred_value_null = operator_adjustment
        from
            dbo.base_order bo
           ,cte_a pp
         ,dbo.work_order_items woi
        where
            bo.base_order_id = a.base_order_id
         and woi.work_group_id = bo.work_group_id
         and woi.work_order_id = {1}
            and pp.customer_store_item_triad_id =
                bo.customer_store_item_triad_id
            and a.run_date = '{0}';
    end
    $$;
        """

prediction_insert_sql = """
    insert into dbo.operator_adjustments (
        customer_store_item_distributor_dyad_id,
        adjustment_quantity, rec_delivery_date,
        create_user, create_date,
        operator_adjustments_reason_id,
        run_date, work_group_id
    )
    select bo.customer_store_item_distributor_dyad_id
        , a.operator_adjustment
        , bo.rec_delivery_date
        , 'AutoAdjustML'
        , now() :: timestamp without time zone
        , 2
        , '{0}'
        , bo.work_group_id
    from dbo.vw_base_orders bo
    inner join dbo.stg_auto_operator_adjustment a
        on a.base_order_id = bo.base_order_id
    inner join dbo.work_order_items woi
        on woi.work_group_id = bo.work_group_id
        and woi.work_order_id = {1}
    left outer join dbo.operator_adjustments ad
        on bo.customer_store_item_distributor_dyad_id =
            ad.customer_store_item_distributor_dyad_id
        and bo.rec_delivery_date = ad.rec_delivery_date
    left outer join dbo.applied_operator_adjustments aa
        on aa.base_order_id = bo.base_order_id
    left outer join dbo.override_adjustments ooa
        on ooa.customer_store_item_distributor_dyad_id =
            bo.customer_store_item_distributor_dyad_id
    and ooa.is_used = true
    left outer join dbo.applied_override_adjustments aoa
        on aoa.customer_store_item_distributor_dyad_id =
         bo.customer_store_item_distributor_dyad_id
        and cast(aoa.referenced_create_date as date) =
     '{0}'
    where (cast(a.run_date as date) = '{0}'
    and a.operator_adjustment + bo.proposed_order_quantity > 0
    and a.operator_adjustment is not null
    and a.operator_adjustment != 0
    and ad.adjustment_id is null
    and aa.adjustment_id is null
    and ooa.customer_store_item_distributor_dyad_id is null
    and aoa.customer_store_item_distributor_dyad_id is null)
    or ( cast(a.run_date as date) = '{0}'
    and bo.proposed_order_quantity > 0
    and a.operator_adjustment < 0
    and ad.adjustment_id is null
    and aa.adjustment_id is null
    and ooa.customer_store_item_distributor_dyad_id is null
    and aoa.customer_store_item_distributor_dyad_id is null
   )
    """


def update_checkpoint_files_from_s3(
    bucket_name: str,
    file_directory: str,
    force_load: bool = False,
    last_modified: Optional[datetime] = None,
) -> datetime:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    check_point_last_modified = bucket.Object("ml/checkpoint").last_modified
    if force_load or last_modified < check_point_last_modified:
        logger.info("Updating ML checkpoint files")
        ml_path = Path(file_directory) / "ml"
        ml_path.mkdir(exist_ok=True)
        for s3_object in bucket.objects.all():
            if s3_object.key == "ml/checkpoint" or s3_object.key.startswith(
                "ml/cp"
            ):
                logger.info(f"downloading {s3_object.key}")
                bucket.download_file(
                    s3_object.key, f"{file_directory}{s3_object.key}"
                )
    return check_point_last_modified


def is_acc(value: Any) -> int:
    unit_diff_threshold: int = 4
    return 1 if value <= unit_diff_threshold else 0


def is_cspk(value: Any) -> int:
    return 0 if value == 1 else 1


def pull_new_order_data(
    db_credential: DataBaseCredential,
    work_order_id: int,
    pull_datetime: datetime,
) -> pd.DataFrame:
    """
    Pulls data for a specific date.
    """
    logger.info("Pulling order data for ML process")
    with get_db_connection(db_credential) as conn:
        dat = pd.read_sql(
            f"SELECT * from public.udf_operator_adjustments("
            f"{work_order_id}, '{pull_datetime.date()}')",
            conn,
        )
    logger.info("Finished pulling order data for ML process")
    dat.loc[pd.isna(dat["max_delivery_quantity"]), "max_delivery_quantity"] = 0
    dat.loc[
        pd.isna(dat["days_since_last_delivery"]), "days_since_last_delivery"
    ] = -1
    return dat


def classify_qty(df: pd.DataFrame) -> pd.DataFrame:
    avg_q = df.ten_day_avg_quantity
    no_zero = avg_q[avg_q > 0]
    avg = np.mean(no_zero)
    sd = np.std(no_zero)
    max_range = avg + (4 * sd)
    no_anomaly = no_zero[no_zero < max_range]
    new_avg = np.mean(no_anomaly)
    new_sd = np.std(no_anomaly)
    low_vol_max = new_avg
    med_vol_max = new_avg + (2 * new_sd)
    df["VolumeClass"] = ""
    df["VolumeClass"][df.ten_day_avg_quantity <= 0] = "Zeros"
    df.VolumeClass[df.ten_day_avg_quantity > med_vol_max] = "High Volume"
    df.VolumeClass[
        (df.ten_day_avg_quantity > 0)
        & (df.ten_day_avg_quantity <= low_vol_max)
    ] = "Low Volume"
    df.VolumeClass[
        (df.ten_day_avg_quantity > low_vol_max)
        & (df.ten_day_avg_quantity <= med_vol_max)
    ] = "Medium Volume"
    df = pd.concat(
        [df.drop("VolumeClass", axis=1), pd.get_dummies(df["VolumeClass"])],
        axis=1,
    )
    for column_name in ["High Volume", "Low Volume", "Medium Volume"]:
        if column_name not in df.columns:
            logger.warning(
                f"Column {column_name} not found. Adding and setting to 0"
            )
            df[column_name] = 0
    return df


def create_model():
    """
    Create the tensor flow model
    """

    learning_rate = 0.0001  # Learning sensitivity
    loss_function = "mean_absolute_error"  # 'mean_squared_error'
    model = tf.keras.Sequential()
    model.add(
        tf.keras.layers.Dense(
            256, input_dim=len(x_columns), activation=tf.nn.relu
        )
    )
    model.add(tf.keras.layers.Dropout(rate=0.1))
    model.add(
        tf.keras.layers.Dense(
            256, input_dim=len(x_columns), activation=tf.nn.relu
        )
    )
    model.add(
        tf.keras.layers.Dense(
            256, input_dim=len(x_columns), activation=tf.nn.relu
        )
    )
    model.add(
        tf.keras.layers.Dense(
            256, input_dim=len(x_columns), activation=tf.nn.relu
        )
    )
    model.add(tf.keras.layers.Dense(1, input_dim=len(x_columns)))
    # run_eagerly fixed celery error in Tensorflow 2.0.0.
    #   uncomment if using 2.0.0
    model.compile(
        loss=loss_function,
        metrics=["accuracy"],
        optimizer=tf.keras.optimizers.Adam(learning_rate),
        # run_eagerly=True,
    )
    return model


def run_prediction(
    df: pd.DataFrame, trained_model, target: str
) -> pd.DataFrame:
    """
    Run the predictions on this "recent" data set
    """
    df = df.reset_index()
    df_x = df[x_columns]
    y_preds = trained_model.predict(df_x)
    preds = pd.DataFrame(y_preds)
    preds[
        np.isnan(preds)
    ] = 0  # Remove all nans and set to 0. May want to set to null?
    preds = preds[0].apply(lambda x: int(round(x, 0)))  # Round the results
    df["BasePrediction"] = preds
    df["case_pack"] = df.case_pack.astype(float)
    if target == "operator_adjustments":
        df["FinalPrediction"] = (
            ((df["calculated_order"] + df["BasePrediction"]) / df["case_pack"])
            .astype(float)
            .round()
            * df["case_pack"]
        ) - df["calculated_order"]
        # Find negative calculated orders and update the operator adjustments.
        negs = df[df["calculated_order_nocspk"] < 0]
        df.FinalPrediction[
            df["index"].isin(negs["index"])
        ] = df.FinalPrediction + (df.calculated_order_nocspk * -1)

    if target == "AdjustedOrder":
        df["FinalPrediction"] = (
            (df["BasePrediction"] - df["calculated_order"]) / df["case_pack"]
        ).astype(float).round() * df["case_pack"]

        # No need to add or subtract CalculatedOrder since we are finding the
        # Operator adjustment only! Also, CalculatedOrder is ALREADY in case
        # Pack quantities, so no need to worry about that.

        # Find negative calculated orders and update the operator adjustments.
        # CalculatedOrderNoCsPK vs CalculatedOrder vs other approach
        negs = df[df["calculated_order_nocspk"] < 0]
        df.FinalPrediction[
            df["index"].isin(negs["index"])
        ] = df.FinalPrediction + (df.calculated_order_nocspk * -1)
        # Ease of use add'l variables
        df["FinalPrediction"] = df["BasePrediction"]

    df["Target"] = target  # Target name
    df["RealOpAdj"] = df[
        "operator_adjustments"
    ]  # New column for output purposes
    # Find out if prediction was accurate or not
    df["Diff"] = abs(df["RealOpAdj"] - df["FinalPrediction"])
    df["isAcc"] = df["Diff"].apply(is_acc)
    return df


def predict(
    db_credential: DataBaseCredential,
    work_order_id: int,
    order_gen_datetime: datetime,
    base_file_directory: str,
    insert: bool = False,
    export: bool = True,
    target: str = "operator_adjustments",
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    weights_directory = f"{base_file_directory}/ml"
    weights_path = Path(weights_directory)
    if not weights_path.exists() or not (weights_path / "checkpoint").exists():
        raise OSError(f"ML Checkpoint files missing at {weights_path}")
    trained_model = create_model()
    trained_model.load_weights(f"{weights_directory}/cp.ckpt")
    # Pull new data for prediction
    new_info_base = pull_new_order_data(
        db_credential=db_credential,
        work_order_id=work_order_id,
        pull_datetime=order_gen_datetime,
    )
    if new_info_base.shape[0] == 0:
        raise PredictionException("No orders found for ML process")
    df = copy.deepcopy(new_info_base)
    df = classify_qty(df)
    df = run_prediction(df=df, trained_model=trained_model, target=target)

    # If you want to export the results into a CSV for manual review
    if export:
        # Find the records that are included in the output files
        #   to distributors
        idSQL = """
        select distinct s.customer_store_distributor_triad_id
        from customer_store_distributor_schedule s
        where s.inc_in_file = true
        """
        with get_db_connection(db_credential) as conn:
            csds = pd.read_sql(idSQL, conn)
        df = df.merge(
            csds,
            how="inner",
            left_on="customer_store_distributor_triad_id",
            right_on="customer_store_distributor_triad_id",
        )
        # TODO make sure the date we pass here is correct
        #   should it be order_gen_datetime or max(date_time_created)?
        ppf = """
        select distinct customer_store_item_triad_id from ml_poor_performers
               where cast(date_time_created as date) = '{0}'
       """.format(
            datetime.utcnow().date()
        )
        with get_db_connection(db_credential) as conn:
            pperf = list(pd.read_sql(ppf, conn).iloc[:, 0])

        now_file = datetime.now().strftime("%Y%m%d_%H%M%S")
        df["FinalPrediction"][
            df["customer_store_item_triad_id"].isin(pperf)
        ] = np.NaN
        df["isAcc"][df["customer_store_item_triad_id"].isin(pperf)] = np.NaN
        csv_output_dir = base_file_directory + "/ml/csv_output"
        Path(csv_output_dir).mkdir(exist_ok=True)
        logger.info(f"Exporting ML adjustment csv to {csv_output_dir}")
        df.to_csv(f"{csv_output_dir}/SOPreds{now_file}.csv")

    if insert:
        # Insert the adjustments as is, however, then update to store
        #   predicted vals, but nullify poor performers
        dSQL = df[["base_order_id", "FinalPrediction"]]
        dSQL["operator_adjustment"] = dSQL["FinalPrediction"].astype(int)
        dSQL.drop("FinalPrediction", axis=1, inplace=True)
        dSQL["create_date"] = now
        dSQL["run_date"] = order_gen_datetime.date()
        dSQL["source_location"] = "Machine Learning"
        model_date = datetime.utcfromtimestamp(
            os.path.getmtime(weights_directory + "/checkpoint")
        ).strftime("%Y-%m-%d")
        dSQL[
            "source_type"
        ] = f"TensorFlow{model_date}"  # Need to add something for version!

        logger.info("Updating stg_auto_operator_adjustment table")
        headers, file = df_to_csv(dSQL)
        with DatabaseCursor(db_credential) as cursor:
            with psycopg_paused_thread():
                cursor.copy_from(
                    file,
                    "stg_auto_operator_adjustment",
                    sep=",",
                    columns=headers,
                )
        logger.info("Updating Poor Performers in ML prediction")
        with DatabaseCursor(db_credential) as cursor:
            query = update_poor_perfs_sql.format(
                order_gen_datetime.date(), work_order_id
            )
            cursor.execute(query)
            query = update_poor_perfs_null_sql.format(work_order_id)
            cursor.execute(query)
        logger.info("Inserting ML predictions")
        with DatabaseCursor(db_credential) as cursor:
            query = prediction_insert_sql.format(
                order_gen_datetime.date(), work_order_id
            )
            cursor.execute(query)
        logger.info("Completed inserting ML predictions")
        return
