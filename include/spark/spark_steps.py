# spark_steps.py
_create_namespace_sql = """
    CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.data
    """
    
_create_table_sql = """
    CREATE TABLE IF NOT EXISTS s3tablesbucket.data.raw (
        date STRING,
        ticker STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        dividend DOUBLE,
        stock_split DOUBLE
    ) USING iceberg
    """
    
    
# Create steps for EMR
STEPS = [
    {
        'Name': 'Create Namespace',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-sql', '-e', _create_namespace_sql]
        }
    },
    {
        'Name': 'Create Table',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-sql', '-e', _create_table_sql]
        }
    },
    {
        'Name': 'Load Data',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://airflow-stock-data-dalicodes/load_to_iceberg.py",
                "--input",
                "{{ task_instance.xcom_pull(task_ids='local_to_s3', dag_id='etl', key='return_value') }}"
            ]
        }
    }
]