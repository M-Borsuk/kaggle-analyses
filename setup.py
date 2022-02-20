import setuptools


setuptools.setup(
    name="kaggle_analysis",
    version="0.1.0",
    packages=setuptools.find_packages(
        include=["connectors", "downloader", "utilities", "config"]
    ),  # noqa: E501
)
