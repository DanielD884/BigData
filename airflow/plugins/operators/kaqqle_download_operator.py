import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from kaggle.api.kaggle_api_extended import KaggleApi

class KaggleDownloadOperator(BaseOperator):
    """
    Custom Operator to download datasets from Kaggle.
    """

    @apply_defaults
    def __init__(self, dataset_name, download_path, *args, **kwargs):
        super(KaggleDownloadOperator, self).__init__(*args, **kwargs)
        self.dataset_name = dataset_name
        self.download_path = download_path

    def execute(self, context):
        self.log.info(f"Downloading Kaggle dataset '{self.dataset_name}' to '{self.download_path}'")

        # Authentificate the Client with the Kaggle API
        api = KaggleApi()
        api.authenticate()

        # the download path must exist
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

        # try to download the dataset from Kaggle
        try:
            api.dataset_download_files(self.dataset_name, path=self.download_path, unzip=True)
            self.log.info(f"Dataset '{self.dataset_name}' successfully downloaded to '{self.download_path}'")
        except Exception as e:
            self.log.error("Failed to download dataset from Kaggle")
            raise AirflowException(e)