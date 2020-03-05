from typing import Optional
from datetime import datetime
from psycopg2.extensions import cursor as psycop2_cursor
from app.common.utils.db import DatabaseCursor, DataBaseCredential


class BaseOrder:
    def __init__(self, **kwargs):
        self.oa_master_distributor_id = kwargs["oa_master_distributor_id"]
        self.customer_distributor_dyad_id = kwargs[
            "customer_distributor_dyad_id"
        ]
        self.distributor_items_id = kwargs["distributor_items_id"]
        self.customer_store_distributor_triad_id = kwargs[
            "customer_store_distributor_triad_id"
        ]
        self.customer_store_item_distributor_dyad_id = kwargs[
            "customer_store_item_distributor_dyad_id"
        ]
        self.customer_store_item_triad_id = kwargs[
            "customer_store_item_triad_id"
        ]
        self.customer_distributor_category_triad_id = kwargs[
            "customer_distributor_category_triad_id"
        ]
        self.category_id = kwargs["category_id"]
        self.rec_delivery_date = kwargs["rec_delivery_date"]
        self.actual_scans = kwargs["actual_scans"]
        self.forecasted_scans = kwargs["forecasted_scans"]
        self.average_scans = kwargs["average_scans"]
        self.base_order = kwargs["base_order"]
        self.create_date = kwargs["create_date"]
        self.last_ship_date = kwargs["last_ship_date"]
        self.last_scheduled_delivery = kwargs["last_scheduled_delivery"]
        self.last_scan_date = kwargs["last_scan_date"]
        self.true_up_applied = kwargs["true_up_applied"]
        self.projected_order = kwargs["projected_order"]
        self.over_under = kwargs["over_under"]
        self.use_true_up = kwargs["use_true_up"]
        self.conversion_factor = kwargs["conversion_factor"]
        self.inc_in_anomaly = kwargs["inc_in_anomaly"]
        self.inc_in_file = kwargs["inc_in_file"]
        self.inc_in_billing = kwargs["inc_in_billing"]

    def __str__(self):
        return """
            oa_master_distributor_id: {0},
            customer_distributor_dyad_id: {1},
            distributor_items_id: {2},
            customer_store_distributor_triad_id: {3},
            customer_store_item_distributor_dyad_id: {4},
            customer_store_item_triad_id: {5},
            customer_distributor_category_triad_id: {6},
            category_id: {7},
            rec_delivery_date: {8},
            actual_scans: {9},
            forecasted_scans: {10},
            average_scans: {11},
            base_order: {12},
            create_date: {13},
            last_ship_date: {14},
            last_scheduled_delivery: {15},
            last_scan_date: {16},
            true_up_applied: {17},
            projected_order: {18},
            over_under: {19},
            use_true_up: {20},
            conversion_factor: {21},
            inc_in_anomaly: {22},
            inc_in_file: {23},
            inc_in_billing: {24}
            """.format(
            self.oa_master_distributor_id,
            self.customer_distributor_dyad_id,
            self.distributor_items_id,
            self.customer_store_distributor_triad_id,
            self.customer_store_item_distributor_dyad_id,
            self.customer_store_item_triad_id,
            self.customer_distributor_category_triad_id,
            self.category_id,
            self.rec_delivery_date,
            self.actual_scans,
            self.forecasted_scans,
            self.average_scans,
            self.base_order,
            self.create_date,
            self.last_ship_date,
            self.last_scheduled_delivery,
            self.last_scan_date,
            self.true_up_applied,
            self.projected_order,
            self.over_under,
            self.use_true_up,
            self.conversion_factor,
            self.inc_in_anomaly,
            self.inc_in_file,
            self.inc_in_billing,
        )

    def __repr__(self):
        return self.__str__()

    def get_over_under(self, cursor: psycop2_cursor, run_date: str) -> int:
        n = 0
        if self.over_under is None:
            get_over_under_query = """
                select over_under from weight_data
                where
                customer_store_item_distributor_dyad_id = {csiddi}
                and
                run_date = '{run_date}'
                """.format(
                csiddi=self.customer_store_item_distributor_dyad_id,
                run_date=run_date,
            )
            cursor.execute(get_over_under_query)
            records = cursor.fetchall()
            if len(records) > 0:
                last_record = records[-1]
                n = int(last_record[0])
            self.over_under = n
        return self.over_under

    def uses_true_up(
        self, cursor: psycop2_cursor, run_datetime: datetime
    ) -> bool:
        r = True
        if self.use_true_up is None:
            uses_true_up_query = """
                select attribute_value from
                customer_store_distributor_attributes
                    where
                customer_store_distributor_triad_id = {csdti}
                    and
                attribute = 'TrueUpAdjustments'
                    and
                '{run_datetime}' between
                effective_date and expiry_date
                """.format(
                csdti=self.customer_store_distributor_triad_id,
                run_datetime=run_datetime,
            )
            cursor.execute(uses_true_up_query)
            records = cursor.fetchall()
            if len(records) > 0:
                last_record = records[-1]
                self.use_true_up = last_record[0]
                if last_record[0] == "Suppress":
                    r = False
        elif self.use_true_up == "Suppress":
            r = False
        return r

    def get_true_up(self, cursor: psycop2_cursor) -> int:
        if self.true_up_applied is None:
            n = 0
            get_true_up_query = """
                select sum(variance) from true_up_adjustments
                    where
                customer_store_item_triad_id = {csiti}
                    and
                base_order_id is null
                """.format(
                csiti=self.customer_store_item_triad_id
            )
            cursor.execute(get_true_up_query)
            record = cursor.fetchone()
            if record[0] is not None:
                n = int(record[0])
            self.true_up_applied = n
        return self.true_up_applied

    def check_for_order(self, db_credential: DataBaseCredential) -> bool:
        tstr = """
                select count(*) from orders
                    where
                customer_store_distributor_triad_id = {0}
                    and
                category_id = {1}
                    and
                rec_delivery_date = '{2}'
                """.format(
            self.customer_store_distributor_triad_id,
            self.category_id,
            str(self.rec_delivery_date),
        )
        with DatabaseCursor(db_credential) as cursor:
            cursor.execute(tstr)
            record_count = int(cursor.fetchone()[0])
        return record_count > 0

    def case_pack_adjustment(
        self,
        cursor: psycop2_cursor,
        order_qty: int,
        case_pack_factor: float,
        as_of_date: str,
    ) -> int:
        cf = self.conversion_factor
        if cf is None:
            cf = self.get_conversion_factor(cursor, as_of_date)
        u = int(order_qty / cf)
        m = order_qty % cf
        if m / cf > case_pack_factor:
            u += 1
        else:
            if m != 0:
                case_pack_query = """
                    insert into conversion_residual (
                    customer_store_item_distributor_dyad_id,
                    residual_quantity, residual_date)
                     select {csiddi}, {m}, '{rd}'
                    """.format(
                    csiddi=self.customer_store_item_distributor_dyad_id,
                    m=m,
                    rd=as_of_date,
                )
                cursor.execute(case_pack_query)
        order = u * cf
        return order

    def get_conversion_factor(
        self, cursor: psycop2_cursor, as_of_date: str
    ) -> Optional[int]:
        conversion_factor_query = """
            select conversion_units from conversion_factors
                where
            customer_store_item_distributor_dyad_id = {csiddi}
                and
            '{as_of_date}' between effective_date and expiry_date
            """.format(
            csiddi=self.customer_store_item_distributor_dyad_id,
            as_of_date=as_of_date,
        )
        ret = None
        cursor.execute(conversion_factor_query)
        records = cursor.fetchall()
        if len(records) > 0:
            last_record = records[-1]
            ret = last_record[0]
        self.conversion_factor = ret
        return ret

    def get_conversion_residual(self, cursor: psycop2_cursor) -> int:
        conversion_residual_query = """
            select c.customer_store_item_distributor_dyad_id,
            coalesce(sum(residual_quantity),0)
            as residual_quantity
            from conversion_residual c
            where
                c.customer_store_item_distributor_dyad_id = {csiddi} and
                c.applied_date is null
            group by c.customer_store_item_distributor_dyad_id
            """.format(
            csiddi=self.customer_store_item_distributor_dyad_id
        )
        ret = 0
        cursor.execute(conversion_residual_query)
        records = cursor.fetchall()
        if len(records) > 0:
            last_record = records[-1]
            ret = last_record[1]
        return ret


base_dict = {
    "BaseOrder": {
        "oa_master_distributor_id": "",
        "customer_distributor_dyad_id": "",
        "distributor_items_id": "",
        "customer_store_distributor_triad_id": "",
        "customer_store_item_distributor_dyad_id": "",
        "customer_store_item_triad_id": "",
        "customer_distributor_category_triad_id": "",
        "category_id": "",
        "rec_delivery_date": "",
        "actual_scans": "",
        "forecasted_scans": "",
        "average_scans": "",
        "base_order": "",
        "create_date": "",
        "last_ship_date": "",
        "last_scheduled_delivery": "",
        "last_scan_date": "",
        "projected_order": "",
        "true_up_applied": "",
        "over_under": "",
        "use_true_up": "",
        "conversion_factor": "",
        "inc_in_anomaly": "",
        "inc_in_file": "",
        "inc_in_billing": "",
    }
}
