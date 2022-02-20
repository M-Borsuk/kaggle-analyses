import yaml
from . import config_types


def load_credentials(config_path: str) -> config_types.Credentials:
    """
    Loads the credentials from the yaml file into Credentials object.

    Args:
        config_path: Path to the yaml file.

    Returns:
        Credentials object.
    """
    config = _load_yaml(config_path)
    return config_types.Credentials(
        kaggle=config_types.KaggleCredentials(
            username=config["kaggle"]["username"], key=config["kaggle"]["key"]
        ),
        s3=load_s3_credentials(config_path),
    )


def load_config(config_path: str) -> config_types.DatasetConfig:
    """
    Loads the config from the yaml file into DatasetConfig object.

    Args:
        config_path: Path to the yaml file.

    Returns:
        DatasetConfig object.
    """
    config = _load_yaml(config_path)
    return config_types.DatasetConfig(
        dataset_name=config["dataset_name"], platform=config["platform"]
    )


def load_s3_credentials(config_path: str) -> config_types.S3Credentials:
    """
    Loads the credentials from the yaml file into Credentials object.

    Args:
        config_path: Path to the yaml file.

    Returns:
        Credentials object.
    """
    config = _load_yaml(config_path)
    return config_types.S3Credentials(
        aws_access_key_id=config["s3"]["aws_access_key_id"],
        aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        bucket=config["s3"]["bucket"],
        region=config["s3"]["region"],
    )


def load_storage_config(config_path: str) -> config_types.StorageConfig:
    """
    Loads the storage config from the yaml file into StorageConfig object.

    Args:
        config_path: Path to the yaml file.

    Returns:
        StorageConfig object.
    """
    config = _load_yaml(config_path)
    return config_types.StorageConfig(
        raw_layer=config["storage"]["raw_layer"],
        computation_layer=config["storage"]["computation_layer"],
        model_layer=config["storage"]["model_layer"],
    )


def _load_yaml(path: str) -> dict:
    """
    Loads the yaml file into a dictionary.

    Args:
        path: Path to the yaml file.

    Returns:
        Dictionary containing the yaml file.
    """
    with open(path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config
