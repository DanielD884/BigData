from airflow.plugins_manager import AirflowPlugin
from operators.hdfs_put_file_operator import *
from operators.hdfs_mkdirs_file_operator import *
from hooks.hdfs_hook import *

class HdfsPlugin(AirflowPlugin):
    name = "hdfs_operations"
    operators = [HdfsPutFileOperator, HdfsMkdirsFileOperator]
    hooks = [HdfsHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []