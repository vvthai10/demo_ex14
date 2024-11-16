import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def process_chat_hourly_aggregate(input_path, output_path):
    spark = SparkSession.builder \
        .master("local") \
        .appName("ChatHourlyAggregate") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    df = spark.read.parquet(input_path)

    result_df = df.withColumn("normalized_src_id", f.least("src_id", "des_id")) \
        .withColumn("normalized_des_id", f.greatest("src_id", "des_id")) \
        .withColumn("is_sender", f.when(f.col("src_id") == f.col("normalized_src_id"), 1).otherwise(0)) \
        .withColumn("hour", f.hour(f.col("datetime").cast("timestamp"))) \
        .drop("src_id", "des_id") \
        .withColumnRenamed("normalized_src_id", "src_id") \
        .withColumnRenamed("normalized_des_id", "des_id") \
        .groupBy("src_id", "des_id", "is_sender", "hour") \
        .agg(
        f.count("*").alias("msgs_count"),
        f.sum("msgs_word").alias("msgs_word"),
        f.sum("msgs_length").alias("msgs_length")
    )

    result_df.write.mode("overwrite").parquet(output_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    process_chat_hourly_aggregate(args.input_path, args.output_path)

if __name__ == "__main__":
    main()
