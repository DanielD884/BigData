from airflow.plugins_manager import AirflowPlugin
from operators.kaggle_download_operator import KaggleDownloadOperator

class KagglePlugin(AirflowPlugin):
    name = "kaggle_plugin"
    operators = [KaggleDownloadOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []