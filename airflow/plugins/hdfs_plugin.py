from airflow.plugins_manager import AirflowPlugin
from operators.hdfs_mkdir_file_operator import HdfsMkdirsFileOperator, HdfsPutFileOperator


class HdfsPlugin(AirflowPlugin):
    name = "hdfs_plugins"
    operators = [HdfsMkdirsFileOperator, HdfsPutFileOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []