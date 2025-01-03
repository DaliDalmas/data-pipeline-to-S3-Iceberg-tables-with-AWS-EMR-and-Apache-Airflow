from airflow.decorators import task, dag
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.io.path import ObjectStoragePath
import polars as pl
from include.spark.emr import CLUSTER_CONFIG
from include.spark.spark_steps import STEPS
from airflow.models.baseoperator import chain

bucket = ObjectStoragePath("s3://airflow-stock-data-dalicodes", conn_id="aws")

@dag(
        schedule=None,
        catchup=False
)
def etl():
    
    @task(
            task_id='local_to_s3'
    )
    def local_to_s3():
        # ensure that the bucket exists
        bucket.mkdir(exist_ok=True)
        path = bucket / "all_stock_data.parquet"

        df = pl.read_csv("include/data/all_stock_data.csv")

        with path.open("wb") as f:
            df.write_parquet(f, compression="zstd", row_group_size=10000)

        return f"{path._protocol}://{path.path}"

    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        aws_conn_id="aws",
        job_flow_overrides=CLUSTER_CONFIG
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        aws_conn_id="aws",
        job_flow_id=create_job_flow.output,
        steps=STEPS,
        deferrable=True,
        wait_for_completion=True
    )

    terminate_job_flow = EmrTerminateJobFlowOperator(
        task_id="terminate_job_flow",
        job_flow_id=add_steps.output["job_flow_id"],
        aws_conn_id="aws"
    )

    chain(
        local_to_s3(),
        create_job_flow,
        add_steps,
        terminate_job_flow.as_teardown(
            setups=[
                create_job_flow
            ]
        )
    )
    
etl()