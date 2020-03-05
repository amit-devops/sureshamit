queries = {
    "CleanUpAndPrep": (
        "select usp_cleanup_and_prep("
        "{work_order_id},"
        "'{run_date}',"
        "{credit_weeks})"
    ),
    "RecommendedDeliveryDates": (
        "select * from udf_get_recommended_delivery_dates("
        "{work_order_id},"
        "'{timestamp}')"
    ),
    "ForecastOrders": (
        "select * from udf_get_forecasts("
        "{work_order_id},"
        "'{rec_delivery_date}',"
        "'{run_date}')"
    ),
    "LastShipDates": (
        "select * from udf_get_last_ship_dates("
        "{work_order_id},"
        "'{run_date}')"
    ),
    "AddActualScans": (
        "select * from udf_get_actual_scans("
        "{work_order_id},"
        "'{run_date}')"
    ),
    "FinalCleanUp": "select usp_final_cleanup({work_order_id}, '{run_date}')",
    "GetDistributors": "select * from udf_get_distributors('{run_date}')",
    "PrepareAdjustments": (
        "select usp_do_adjustments({work_order_id},'{run_date}')"
    ),
    "CalculateShrink": "select usp_calculate_shrink({0},'{1}')",
    "OperatorAdjustments": (
        "select usp_populate_applied_operator_adjustments("
        "{work_order_id},"
        "'{run_date}')"
    ),
    "Overrride2": (
        "select usp_populate_applied_override_adjustments({work_order_id}, "
        "'{run_date}')"
    ),
    "StagedOrderReport": (
        "select * from udf_get_staged_orders("
        "{work_order_id},"
        "'{run_date}')"
    ),
    "OrderForecasts": (
        "select usp_order_forecast_update"
        "({work_order_id},'MovingAverage','{run_date}')"
    ),
    "BusinessAdjustments": (
        "select usp_business_adjustments({work_order_id})"
    ),
    "BusinessAdjustmentsPostML": (
        "select usp_business_adjustments_postml({work_order_id})"
    ),
    "BusinessAdjustmentsFinal": (
        "select usp_business_adjustments_final({work_order_id})"
    ),
}

reports = {
    # There is a param - p_suppress_zero in usp_pf_oa_output
    # that is defaulted to 1. It hasn't been passed anywhere.
    "PrairieFarms": {
        "SendOrders": "select * from usp_pf_oa_output(4,'{run_date}')"
    },
    "SunnyFlorida": {
        "SendOrders": "select * from usp_pf_oa_output(5,'{run_date}')"
    },
}
