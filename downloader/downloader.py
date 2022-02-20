from abc import ABC, abstractmethod
import os
import tempfile
import boto3
from typing import Iterable
import shutil
from utilities import config_types


class Downloader(ABC):
    def __init__(
        self,
        config=config_types.DatasetConfig,
        credentials=config_types.Credentials,  # noqa: E501
    ):
        self.config = config
        self.credentials = credentials
        self.kaggle_api = self.authorize(credentials)

    @abstractmethod
    def download(self):
        pass

    @staticmethod
    def authorize(credentials: config_types.Credentials):
        # Very unclean way.. TODO think of a better way to do it.
        os.environ["KAGGLE_USERNAME"] = credentials.kaggle.username
        os.environ["KAGGLE_KEY"] = credentials.kaggle.key
        from kaggle.api.kaggle_api_extended import KaggleApi  # noqa: E402

        api = KaggleApi()
        api.authenticate()
        return api

    @staticmethod
    def create_path(base_path: str, *path_components: Iterable[str]) -> str:
        """
        Creates a path from the base path and the path components.

        Args:
            base_path: Base path.
            path_components: Path components.

        Returns:
            Path.
        """
        path = base_path
        for component in path_components:
            path = os.path.join(path, component)
        return path


class S3Downloader(Downloader):

    RAW_LAYER_PATH = "RAW"

    def __init__(
        self,
        config=config_types.DatasetConfig,
        credentials=config_types.Credentials,  # noqa: E501
    ):
        super().__init__(config, credentials)
        self.client = boto3.client(
            "s3",
            aws_access_key_id=credentials.s3.aws_access_key_id,
            aws_secret_access_key=credentials.s3.aws_secret_access_key,
            region_name=credentials.s3.region,
        )

    def download(self):
        temp_dir = tempfile.mkdtemp()
        self.kaggle_api.dataset_download_files(
            self.config.dataset_name, path=temp_dir, unzip=True
        )
        if not os.path.exists(temp_dir):
            raise Exception("Data path does not exist.")
        for file in os.listdir(temp_dir):
            self.client.upload_file(
                self.create_path(temp_dir, file),
                self.credentials.s3.bucket,
                self.create_path(
                    self.RAW_LAYER_PATH,
                    self.config.dataset_name.split("/")[-1],  # noqa
                    file,
                ).replace("\\", "/"),
            )
        shutil.rmtree(temp_dir)
