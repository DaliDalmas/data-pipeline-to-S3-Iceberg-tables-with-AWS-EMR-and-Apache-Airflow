# emr.py
from typing import Any

SPARK_CONFIG = [
    {
        "Classification": "iceberg-defaults",
        "Properties": {
            "iceberg.enabled": "true"
        }
    },
    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.0",
            "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
            "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:us-east-1:456315565178:bucket/airflow-stock-data-table",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        }
    }
]

CLUSTER_CONFIG: dict[str, Any] = {
    "Name": "Spark Iceberg Cluster",
    "ReleaseLabel": "emr-7.5.0",
    "LogUri": "s3://airflow-stock-data-dalicodes",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": "subnet-0e08e5b2aae562f39",
    },
    "Configurations": SPARK_CONFIG,
    "ServiceRole": "EMR_DefaultRole",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "VisibleToAllUsers": True,
}