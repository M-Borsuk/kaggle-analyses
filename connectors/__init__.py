import pandas as pd
from . import connector
from utilities import config_types
from utilities import load_storage_config, load_s3_credentials


def build_s3_connector(storage_config: str, s3_credentials: str) -> connector.Container:  # noqa: E501
    """
    Builds the connector.

    Args:
        storage_config: Path to the storage config yaml file.
        s3_credentials: Path to the s3 credentials yaml file.

    Returns:
        Connector object.
    """
    storage_config = load_storage_config(storage_config)
    s3_credentials = load_s3_credentials(s3_credentials)
    return connector.S3Container(storage_config, s3_credentials)


def read_data(
    name: str, prefix: str, layer: str, container: connector.Container
) -> pd.DataFrame:
    """
    Reads the data from the container.

    Args:
        name: Name of the data.
        prefix: Prefix of the data.
        layer: Layer of the data.
        container: Container object.

    Returns:
        DataFrame object.
    """
    table_config = config_types.TableConfig(name, layer, prefix)
    return container.read(table_config)
