import os
import sys
import random

import luigi
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import PySparkTask
import pyspark.sql.functions as f
from pyspark import SQLContext


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class FileExistsTask(PySparkTask):
    file_path = luigi.Parameter()
    def output(self):
        return HdfsTarget(str(self.file_path))
    def main(self, sc, *args):
        # self.sql_context = SQLContext(sc)
        # fid = self.sql_context.read.parquet(str(self.file_path))
        pass


class ChatHourlyAggregate(PySparkTask):
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return HdfsTarget(self.output_path)

    def requires(self):
        return [FileExistsTask(self.input_path)]

    def main(self, sc, *args):
        self.sql_context = SQLContext(sc)
        df = self.sql_context.read.parquet(self.input_path)
        df.withColumn("normalized_src_id", f.least("src_id", "des_id"))\
            .withColumn("normalized_des_id", f.greatest("src_id", "des_id"))\
            .withColumn("is_sender", f.when(f.col("src_id") == f.col("normalized_src_id"), 1).otherwise(0))\
            .withColumn("hour", f.hour(f.col("datetime").cast("timestamp")))\
            .drop("src_id", "des_id")\
            .withColumnRenamed("normalized_src_id", "src_id")\
            .withColumnRenamed("normalized_des_id", "des_id")\
            .groupBy("src_id", "des_id", "is_sender", "hour")\
            .agg(
                f.count("*").alias("msgs_count"),
                f.sum("msgs_word").alias("msgs_word"),
                f.sum("msgs_length").alias("msgs_length")
            )\
            .write.parquet(self.output_path)


class ChatMonthlyAggregate(PySparkTask):
    input_path = luigi.Parameter()

    def requires(self):
        return [FileExistsTask(self.input_path)]

    def main(self, sc, *args):
        self.sql_context = SQLContext(sc)
        df = self.sql_context.read.parquet(self.input_path)

        df.filter(f.col("is_sender") == 1)\
            .groupBy("src_id")\
            .agg(
                f.sum("msgs_length").alias("chat_sum_msgs_length_out_d30")
            )\
            .sort("src_id")\
            .show(5)
        
        df.filter(f.col("is_sender") == 0)\
            .groupBy("src_id")\
            .agg(
                f.sum("msgs_word").alias("chat_sum_mgs_word_in_d30")
            )\
            .sort("src_id")\
            .show(5)
    
        df.filter(f.col("is_sender") == 0)\
            .groupBy("src_id", "des_id")\
            .agg(
                f.count("*").alias("total_msgs")
            )\
            .groupBy("src_id")\
            .agg(
                f.slice(f.sort_array(f.collect_list(f.struct(f.col("total_msgs"), f.col("des_id"))), asc=False), 1, 5).alias("list_top_chat_out_d30")
            )\
            .withColumn("list_top_chat_out_d30", f.transform("list_top_chat_out_d30", lambda x: x.des_id))\
            .sort("src_id")\
            .show(5, truncate=False)
        
        df.filter(f.col("is_sender") == 1)\
            .groupBy("src_id", "des_id")\
            .agg(
                f.count("*").alias("total_msgs")
            )\
            .groupBy("src_id")\
            .agg(
                f.slice(f.sort_array(f.collect_list(f.struct(f.col("total_msgs"), f.col("des_id"))), asc=False), 1, 5).alias("list_top_chat_out_d30")
            )\
            .withColumn("list_top_chat_out_d30", f.transform("list_top_chat_out_d30", lambda x: x.des_id))\
            .sort("src_id")\
            .show(5, truncate=False)
    



    