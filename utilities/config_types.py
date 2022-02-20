from dataclasses import dataclass


@dataclass
class StorageConfig:
    raw_layer: str
    computation_layer: str
    model_layer: str


@dataclass
class S3Credentials:
    aws_access_key_id: str
    aws_secret_access_key: str
    bucket: str
    region: str


@dataclass
class TableConfig:
    file_name: str
    layer: str
    prefix: str


@dataclass
class DatasetConfig:
    dataset_name: str
    platform: str


@dataclass
class KaggleCredentials:
    username: str
    key: str


@dataclass
class Credentials:
    kaggle: KaggleCredentials
    s3: S3Credentials
