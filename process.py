import os
import sys
import random

import luigi
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import SparkSubmitTask


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class FileExistsTask(luigi.ExternalTask):
    file_path = luigi.Parameter()
    def output(self):
        return HdfsTarget(str(self.file_path))

class ChatHourlyAggregate(SparkSubmitTask):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    app = "./jobs/job_14_1.py"

    def output(self):
        return HdfsTarget(self.output_path)

    def requires(self):
        return [FileExistsTask(self.input_path)]

    def app_command(self):
        return [
            self.app,
            "--input_path", self.input_path,
            "--output_path", self.output_path
        ]

class ChatMonthlyAggregate(SparkSubmitTask):
    input_path = luigi.Parameter()
    app = "./jobs/job_14_2.py"

    def requires(self):
        return [FileExistsTask(self.input_path)]

    def app_command(self):
        return [
            self.app,
            "--input_path", self.input_path
        ]


    



    