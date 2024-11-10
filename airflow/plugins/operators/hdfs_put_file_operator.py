import ast
from os import path
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.hdfs_hook import HdfsHook

class HdfsPutFileOperator(BaseOperator):

    template_fields = ("local_path", "remote_path", "file_names", "hdfs_conn_id")
    ui_color = "#fcdb03"

    @apply_defaults
    def __init__(self, local_path, remote_path, file_names, hdfs_conn_id='hdfs_default', *args, **kwargs):
        """
        :param local_path: local directory containing files to upload to HDFS
        :type local_path: string
        :param remote_path: HDFS directory to upload files to
        :type remote_path: string
        :param file_names: list of file names to upload
        :type file_names: list
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """
        super(HdfsPutFileOperator, self).__init__(*args, **kwargs)
        self.local_path = local_path
        self.remote_path = remote_path
        self.file_names = file_names
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):
        self.log.info("HdfsPutFilesOperator execution started.")

        # Evaluate the file names if they are passed as a string
        if isinstance(self.file_names, str):
            file_names = ast.literal_eval(self.file_names)
        else:
            file_names = self.file_names

        # Create a list of local and remote file paths
        self.files = [
            [
                path.join(self.local_path, "{}-hubway-tripdata.csv".format(file)),
                path.join(self.remote_path, file, "{}-hubway-tripdata.csv".format(file)),
            ]
            for file in file_names
        ]

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)

        for index, file in enumerate(self.files):
            local_file = file[0]
            remote_file = file[1]

            self.log.info(f"{index}: Upload file '{local_file}' to HDFS '{remote_file}'.")

            hh.put_file(local_file, remote_file)

        self.log.info("HdfsPutFilesOperator done.")