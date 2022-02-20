from abc import ABC, abstractmethod
import boto3
import pandas as pd
from utilities import config_types


class Container(ABC):
    PANDAS_READ_WRITE_FUNCTIONS = {
        "csv": pd.read_csv,
        "excel": pd.read_excel,
        "json": pd.read_json,
        "parquet": pd.read_parquet,
    }

    def __init__(
        self,
        storage_config: config_types.StorageConfig,
    ):
        self.storage_config = storage_config

    @abstractmethod
    def read(self, table_config: config_types.TableConfig) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(
        self, table_config: config_types.TableConfig, df: pd.DataFrame
    ) -> pd.DataFrame:
        pass

    def _get_function(self, file_type: str) -> callable:
        return self.PANDAS_READ_WRITE_FUNCTIONS[file_type]


class S3Container(Container):
    def __init__(
        self,
        storage_config: config_types.StorageConfig,
        s3_credentials: config_types.S3Credentials,
    ):
        super().__init__(storage_config)
        self.s3_credentials = s3_credentials
        self.s3_client = self._connect(s3_credentials)

    def _get_s3_path(self, table_config: config_types.TableConfig) -> str:
        return f"s3://{self.s3_credentials.bucket}/{table_config.layer}/{table_config.prefix}/{table_config.file_name}"  # noqa: E501

    def _connect(self, s3_credentials: config_types.S3Credentials):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_credentials.aws_access_key_id,
            aws_secret_access_key=s3_credentials.aws_secret_access_key,
            region_name=s3_credentials.region,
        )
        return s3_client

    def read(self, table_config: config_types.TableConfig) -> pd.DataFrame:
        file_type = table_config.file_name.split(".")[-1]
        function = self._get_function(file_type)
        path = self._get_s3_path(table_config)
        return function(
            path,
            storage_options={
                "key": self.s3_credentials.aws_access_key_id,
                "secret": self.s3_credentials.aws_secret_access_key,
            },
        )

    def write(self, table_config: config_types.TableConfig, df: pd.DataFrame):
        df.to_csv(
            self._get_s3_path(table_config),
            index=False,
            storage_options={
                "key": self.s3_credentials.aws_access_key_id,
                "secret": self.s3_credentials.aws_secret_access_key,
            },
        )
