# load_to_iceberg.py
from pyspark.sql import SparkSession
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="Data location in S3", default="")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("Load to Iceberg").getOrCreate()

    data_file_location = args.input
    data_file = spark.read.parquet(data_file_location)
    data_file.writeTo("s3tablesbucket.data.raw") \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    spark.stop()