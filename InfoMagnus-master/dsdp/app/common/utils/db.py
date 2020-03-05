import csv
from dataclasses import dataclass
import psycopg2
import pandas as pd
from io import StringIO
from pathlib import Path
from contextlib import contextmanager
from typing import List, Tuple, Optional, Callable

stdout_query = "copy ({query}) to stdout with delimiter ',' csv header;"

stdin_query_with_update = (
    "CREATE TEMPORARY TABLE temp ({columns_with_types}) ON COMMIT DROP; "
    "COPY temp ({column_names}) FROM stdin WITH DELIMITER AS ',' csv header; "
    "INSERT INTO {table_name}({column_names}) SELECT * FROM temp"
    " ON CONFLICT ({pk_column}) DO UPDATE"
    " SET {update_columns} WHERE {table_name}.status = 'CREATED';"
)

stdin_query_do_nothing_on_conflict = (
    "CREATE TEMPORARY TABLE temp ({columns_with_types}) ON COMMIT DROP; "
    "COPY temp ({column_names}) FROM stdin WITH DELIMITER AS ',' csv header; "
    "INSERT INTO {table_name}({column_names}) SELECT * FROM temp"
    " ON CONFLICT DO NOTHING;"
)

stdin_query_with_columns = (
    "COPY {table_name}({column_names}) FROM stdin "
    "WITH DELIMITER AS ',' csv header; "
)

stdin_query = (
    "COPY {table_name} FROM stdin " "WITH DELIMITER AS ',' csv header; "
)


@dataclass
class DataBaseCredential:
    user: str
    password: str
    db_name: str
    host: str
    port: int
    schema: str = ""


def get_db_connection(db_cred: DataBaseCredential):
    connection_dict = dict(
        user=db_cred.user,
        password=db_cred.password,
        dbname=db_cred.db_name,
        host=db_cred.host,
        port=db_cred.port,
    )
    if db_cred.schema:
        connection_dict["options"] = f"-c search_path={db_cred.schema},public"
    return psycopg2.connect(**connection_dict)


class DatabaseCursor:
    def __init__(
        self,
        db_credential: DataBaseCredential,
        cursor_factory: Optional[Callable] = None,
    ):
        self._connection = get_db_connection(db_credential)
        self.cursor_factory = cursor_factory

    def __enter__(self):
        """Creates cursor at the start of with block"""
        if self.cursor_factory:
            self.cursor = self._connection.cursor(
                cursor_factory=self.cursor_factory
            )
        else:
            self.cursor = self._connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ commits or rolls back changes dependant on if a exception is
        encountered and close connection"""
        if exc_val is not None:
            self._connection.rollback()
        else:
            self.cursor.close()
            self._connection.commit()
        self._connection.close()


@contextmanager
def psycopg_paused_thread():
    """Prevents asynchronous errors with psycopg. Mainly occurs with
     calls to cursor.copy_expert"""
    try:
        thread = psycopg2.extensions.get_wait_callback()
        psycopg2.extensions.set_wait_callback(None)
        yield
    finally:
        psycopg2.extensions.set_wait_callback(thread)


def run_query_with_db_conn(
    db_credential: DataBaseCredential, query: str, return_results: bool = True
) -> List[Tuple]:
    with DatabaseCursor(db_credential) as cursor:
        cursor.execute(query)
        if not return_results:
            return []
        query_result = cursor.fetchall()
    return query_result


def export_query_to_file(
    db_credential: DataBaseCredential, query: str, file_path: Path
) -> None:
    """
    exports a database table to a csv
    :param db_credential:
    :param query: sql query to create table i.e. select statement
    :param file_path: path to save file
    :return: None
    """
    with DatabaseCursor(db_credential) as cursor:
        with psycopg_paused_thread():
            with file_path.open("w") as fout:
                sql = stdout_query.format(query=query)
                cursor.copy_expert(sql, fout)


def import_table_from_file_with_update(
    db_credential: DataBaseCredential,
    columns: List[Tuple[str, str]],
    pk_column: str,
    update_columns: List[str],
    table_name: str,
    file_path: Path,
) -> None:
    """
    Imports csv file into database using a postgres connection pool
    :param db_credential:
    :param columns: tuple of column_name, type
    :param pk_column: primary key of table used from conflict update
    :param update_columns: column names to update on conflict
    :param table_name: name of target table
    :param file_path: path of csv
    :return: None
    """
    with DatabaseCursor(db_credential) as cursor:
        column_names = ",".join([col[0] for col in columns])
        columns_with_types = ",".join(
            [f"{col[0]} {col[1]}" for col in columns]
        )
        update_column_str = ", ".join(
            [f"{col}=EXCLUDED.{col}" for col in update_columns]
        )
        sql = stdin_query_with_update.format(
            table_name=table_name,
            columns_with_types=columns_with_types,
            column_names=column_names,
            pk_column=pk_column,
            update_columns=update_column_str,
        )
        with psycopg_paused_thread():
            with file_path.open("r") as fin:
                cursor.copy_expert(sql, fin)


def import_table_from_file_do_nothing_on_conflict(
    db_credential: DataBaseCredential,
    columns: List[Tuple[str, str]],
    table_name: str,
    file_path: Path,
) -> None:
    """
    Imports csv file into database using a postgres connection pool.
    On conflict row is ignored
    :param db_credential:
    :param columns: tuple of column_name, type
    :param table_name: name of target table
    :param file_path: path of csv
    :return: None
    """
    with DatabaseCursor(db_credential) as cursor:
        column_names = ",".join([col[0] for col in columns])
        columns_with_types = ",".join(
            [f"{col[0]} {col[1]}" for col in columns]
        )
        sql = stdin_query_do_nothing_on_conflict.format(
            table_name=table_name,
            columns_with_types=columns_with_types,
            column_names=column_names,
        )
        with psycopg_paused_thread():
            with file_path.open("r") as fin:
                cursor.copy_expert(sql, fin)


def import_table_from_file(
    db_credential: DataBaseCredential,
    table_name: str,
    column_names: Optional[List[str]],
    file_path: Path,
) -> None:
    if column_names:
        query = stdin_query_with_columns.format(
            table_name=table_name, column_names=",".join(column_names)
        )
    else:
        query = stdin_query.format(table_name=table_name)
    with DatabaseCursor(db_credential) as cursor:
        with psycopg_paused_thread():
            with file_path.open("r") as file_out:
                cursor.copy_expert(sql=query, file=file_out)


def import_table_from_s3_bucket(
    db_credential: DataBaseCredential,
    table_name: str,
    s3_bucket_path: str,
    iam_role: str,
    using_manifest_file: bool = False,
    ignore_header: bool = True,
):
    """Imports table from a file in s3 bucket.
    This file can be a csv or manifest file"""
    if ignore_header:
        import_query = (
            f"copy {table_name} from '{s3_bucket_path}' "
            f"delimiter ',' ignoreheader 1 iam_role '{iam_role}'"
        )
    else:
        import_query = (
            f"copy {table_name} from '{s3_bucket_path}' "
            f"iam_role '{iam_role}'"
        )

    import_query += f" {'manifest;' if using_manifest_file else ';'}"

    with DatabaseCursor(db_credential) as cursor:
        cursor.execute(import_query)


def get_list_of_tuples(
    db_credential: DataBaseCredential,
    query: str,
    include_headers: bool = False,
) -> List[Tuple]:
    records = []
    with DatabaseCursor(db_credential) as cursor:
        cursor.execute(query)
        result_set = cursor.fetchall()
        if include_headers:
            columns = [i[0] for i in cursor.description]
            records.append(tuple(columns))
        records.extend(result_set)
    return records


def df_to_csv(df: pd.DataFrame) -> Tuple[List[str], StringIO]:
    """
    :param df: Dataframe
    :return: headers: List of column names from the data frame
             string_buffer: In-memory CSV file with data from data frame
    """
    headers = df.columns
    data = df.values.tolist()
    string_buffer = StringIO()
    csv_writer = csv.writer(string_buffer)
    csv_writer.writerows(data)
    string_buffer.seek(0)
    return headers, string_buffer
