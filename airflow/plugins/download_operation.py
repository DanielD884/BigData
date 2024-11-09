from airflow.plugins_manager import AirflowPlugin
from operators.kaqqle_download_operator import *


class KagglePlugin(AirflowPlugin):
    name = "download_operation"
    operators = [KaggleDownloadOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []