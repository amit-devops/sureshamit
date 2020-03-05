import os
import json
import logging
from typing import Optional, List, Tuple
from flask import current_app as app
from pathlib import Path

import boto3
from abc import ABC, abstractmethod
from app.common.utils.db import (
    DataBaseCredential,
    import_table_from_s3_bucket,
    import_table_from_file,
    export_query_to_file,
    import_table_from_file_do_nothing_on_conflict,
    import_table_from_file_with_update,
)

logger = logging.getLogger(__name__)


class AbstractDataBaseStorageCenter(ABC):
    @abstractmethod
    def export_query_from_source_to_file(self, file_name: str, query: str):
        ...

    @abstractmethod
    def import_to_destination_from_file(
        self,
        file_name: str,
        table_name: str,
        column_names: Optional[List[str]] = None,
    ) -> None:
        ...

    @abstractmethod
    def import_to_destination_from_file_with_update(
        self,
        file_name: str,
        table_name: str,
        pk_column: str,
        columns: List[Tuple[str, str]],
        update_columns: List[str],
    ) -> None:
        ...

    @abstractmethod
    def import_to_destination_from_file_nothing_on_conflict(
        self, file_name: str, table_name: str, columns: List[Tuple[str, str]]
    ) -> None:
        ...

    @abstractmethod
    def remove_file(self, file_name: str):
        ...


class S3DataBaseStorageCenter(AbstractDataBaseStorageCenter):
    def __init__(
        self,
        directory: str,
        bucket_name: str,
        source_db_cred: DataBaseCredential,
        destination_db_cred: DataBaseCredential,
        iam_role: str,
    ) -> None:
        self.base_directory = directory
        self.bucket_name = bucket_name
        self.source_db_cred = source_db_cred
        self.destination_db_cred = destination_db_cred
        self.s3 = boto3.resource("s3")
        self.iam_role = iam_role

    def export_query_from_source_to_file(
        self, file_name: str, query: str
    ) -> None:
        bucket = self.s3.Bucket(self.bucket_name)
        file_path = Path(f"{self.base_directory}/{file_name}")
        export_query_to_file(
            self.source_db_cred, query=query, file_path=file_path
        )
        bucket.upload_file(str(file_path), file_name)
        file_path.unlink()

    def import_to_destination_from_file(
        self,
        file_name: str,
        table_name: str,
        column_names: Optional[List[str]] = None,
    ) -> None:
        s3_bucket_path = f"s3://{self.bucket_name}/{file_name}"
        import_table_from_s3_bucket(
            db_credential=self.destination_db_cred,
            table_name=table_name,
            s3_bucket_path=s3_bucket_path,
            iam_role=self.iam_role,
        )

    def import_to_destination_from_manifest_file(
        self, manifest_file_name: str, file_names: List[str], table_name: str
    ) -> None:
        """Creates manifest file, uploads, and imports table
        using manifest file"""
        self.upload_manifest_file(
            file_names=file_names, manifest_file_name=manifest_file_name
        )
        import_table_from_s3_bucket(
            self.destination_db_cred,
            table_name=table_name,
            s3_bucket_path=f"s3://{self.bucket_name}/{manifest_file_name}",
            iam_role=self.iam_role,
            using_manifest_file=True,
        )

    def import_to_destination_from_file_with_update(
        self,
        file_name: str,
        table_name: str,
        pk_column: str,
        columns: List[Tuple[str, str]],
        update_columns: List[str],
    ) -> None:
        raise NotImplementedError("S3 import with update not implemented ")

    def import_to_destination_from_file_nothing_on_conflict(
        self, file_name: str, table_name: str, columns: List[Tuple[str, str]]
    ) -> None:
        raise NotImplementedError("S3 import function not implemented ")

    def remove_file(self, file_name: str) -> None:
        try:
            self.s3.Object(self.bucket_name, file_name).delete()
        except Exception as e:
            raise Exception(
                f"Exception while deleting S3 file {file_name}: {e}"
            )

    def upload_manifest_file(
        self, file_names: List[str], manifest_file_name: str
    ) -> None:
        """Creates a manifest file from a list of file name in a bucket
         and uploads to S3"""
        bucket = self.s3.Bucket(self.bucket_name)
        manifest_entries = [
            {"url": f"s3://{self.bucket_name}/{file_name}", "mandatory": True}
            for file_name in file_names
        ]
        manifest = {"entries": manifest_entries}
        manifest_path = Path(f"{self.base_directory}/{manifest_file_name}")
        with manifest_path.open("w") as fout:
            json.dump(manifest, fout, indent=4)
        bucket.upload_file(str(manifest_path), manifest_file_name)
        manifest_path.unlink()


class LocalDataBaseStorageCenter(AbstractDataBaseStorageCenter):
    def __init__(
        self,
        directory: str,
        source_db_cred: DataBaseCredential,
        destination_db_cred: DataBaseCredential,
    ) -> None:
        self.base_directory = directory
        self.source_db_cred = source_db_cred
        self.destination_db_cred = destination_db_cred

    def export_query_from_source_to_file(
        self, file_name: str, query: str
    ) -> None:
        file_path = Path(f"{self.base_directory}/{file_name}")
        logging.info(f"Exporting query to file {file_path.absolute()}")
        export_query_to_file(
            self.source_db_cred, query=query, file_path=file_path
        )

    def import_to_destination_from_file(
        self,
        file_name: str,
        table_name: str,
        column_names: Optional[List[str]] = None,
    ):
        file_path = Path(f"{self.base_directory}/{file_name}")
        import_table_from_file(
            self.destination_db_cred,
            file_path=file_path,
            table_name=table_name,
            column_names=column_names,
        )

    def import_to_destination_from_file_with_update(
        self,
        file_name: str,
        table_name: str,
        pk_column: str,
        columns: List[Tuple[str, str]],
        update_columns: List[str],
    ) -> None:
        file_path = Path(f"{self.base_directory}/{file_name}")
        import_table_from_file_with_update(
            self.destination_db_cred,
            columns=columns,
            table_name=table_name,
            pk_column=pk_column,
            update_columns=update_columns,
            file_path=file_path,
        )

    def import_to_destination_from_file_nothing_on_conflict(
        self, file_name: str, table_name: str, columns: List[Tuple[str, str]]
    ) -> None:
        file_path = Path(f"{self.base_directory}/{file_name}")
        import_table_from_file_do_nothing_on_conflict(
            self.destination_db_cred,
            columns=columns,
            table_name=table_name,
            file_path=file_path,
        )

    def remove_file(self, file_name: str) -> None:
        try:
            os.chdir(self.base_directory)
            os.remove(file_name)
        except Exception as e:
            raise Exception(f"Exception while deleting {file_name}: {e} ")


def get_storage_center(
    source_db_cred: DataBaseCredential,
    destination_db_cred: DataBaseCredential,
    storage_type: Optional[str] = None,
    directory: Optional[str] = None,
    bucket_name: Optional[str] = None,
    iam_role: Optional[str] = None,
) -> AbstractDataBaseStorageCenter:
    if not storage_type:
        storage_type = app.config["STORAGE"]
    if storage_type == "LOCAL":
        return LocalDataBaseStorageCenter(
            directory=directory or app.config["FILES_DIRECTORY"],
            source_db_cred=source_db_cred,
            destination_db_cred=destination_db_cred,
        )
    elif storage_type == "S3":
        return S3DataBaseStorageCenter(
            directory=directory or app.config["FILES_DIRECTORY"],
            bucket_name=bucket_name or app.config["S3_BUCKET_NAME"],
            source_db_cred=source_db_cred,
            destination_db_cred=destination_db_cred,
            iam_role=iam_role or app.config["S3_IAM_ROLE"],
        )
    else:
        raise OSError(
            f"Environment variable STORAGE not set or "
            f"{os.getenv('STORAGE')} is not a valid value."
        )
