import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def process_chat_monthly_aggregate(input_path):
    spark = SparkSession.builder \
        .master("local") \
        .appName("ChatMonthlyAggregate") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    df = spark.read.parquet(input_path)

    df.filter(f.col("is_sender") == 1) \
        .groupBy("src_id") \
        .agg(
        f.sum("msgs_length").alias("chat_sum_msgs_length_out_d30")
    ) \
        .sort("src_id") \
        .show(5)

    df.filter(f.col("is_sender") == 0) \
        .groupBy("src_id") \
        .agg(
        f.sum("msgs_word").alias("chat_sum_mgs_word_in_d30")
    ) \
        .sort("src_id") \
        .show(5)

    df.filter(f.col("is_sender") == 0) \
        .groupBy("src_id", "des_id") \
        .agg(
        f.count("*").alias("total_msgs")
    ) \
        .groupBy("src_id") \
        .agg(
        f.slice(f.sort_array(f.collect_list(f.struct(f.col("total_msgs"), f.col("des_id"))), asc=False), 1, 5).alias("list_top_chat_out_d30")
    ) \
        .withColumn("list_top_chat_out_d30", f.transform("list_top_chat_out_d30", lambda x: x.des_id)) \
        .sort("src_id") \
        .show(5, truncate=False)

    df.filter(f.col("is_sender") == 1) \
        .groupBy("src_id", "des_id") \
        .agg(
        f.count("*").alias("total_msgs")
    ) \
        .groupBy("src_id") \
        .agg(
        f.slice(f.sort_array(f.collect_list(f.struct(f.col("total_msgs"), f.col("des_id"))), asc=False), 1, 5).alias("list_top_chat_out_d30")
    ) \
        .withColumn("list_top_chat_out_d30", f.transform("list_top_chat_out_d30", lambda x: x.des_id)) \
        .sort("src_id") \
        .show(5, truncate=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    args = parser.parse_args()

    process_chat_monthly_aggregate(args.input_path)

if __name__ == "__main__":
    main()
