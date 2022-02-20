from . import downloader
from utilities import config_types
from utilities import load_config, load_credentials


def get_downloader(
    config: config_types.DatasetConfig, credentials: config_types.Credentials
) -> downloader.Downloader:
    """
    Returns the downloader based on the dataset config.

    Args:
        config: Dataset config.

    Returns:
        Downloader object.
    """
    if config.platform.lower() == "s3":
        return downloader.S3Downloader(config, credentials)
    raise ValueError("Unknown platform: {}".format(config.platform))


def download(config_file: str, credentials_file: str) -> None:
    """
    Downloads the dataset.

    Args:
        config_file: Path to the config file.
        credentials_file: Path to the credentials file.
    """
    config = load_config(config_file)
    credentials = load_credentials(credentials_file)
    downloader_instance = get_downloader(config, credentials)
    downloader_instance.download()
