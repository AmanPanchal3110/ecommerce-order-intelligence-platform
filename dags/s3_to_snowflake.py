from airflow.sdk import dag, task
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime
@dag(
    schedule=None,
    start_date=datetime(2026, 5, 20),
    catchup=False,
    tags=["copy", "s3_to_snowflake"]
)
def s3_to_snowflake():
        CopyFromExternalStageToSnowflakeOperator(
            task_id="order_items_s3_to_snowflake",
            snowflake_conn_id="snowflake_default",
            table="raw_order_items",
            schema="raw",
            stage="my_s3_stage/streaming/order_items",
            file_format="my_parquet_format",
            pattern=".*[.]snappy[.]parquet",
            copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        )
        CopyFromExternalStageToSnowflakeOperator(
            task_id="order_status_s3_to_snowflake",
            snowflake_conn_id="snowflake_default",
            table="raw_order_status",
            schema="raw",
            stage="my_s3_stage/streaming/order_status",
            file_format="my_parquet_format",
            pattern=".*[.]snappy[.]parquet",
            copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        )
        CopyFromExternalStageToSnowflakeOperator(
            task_id="payment_s3_to_snowflake",
            snowflake_conn_id="snowflake_default",
            table="raw_payments",
            schema="raw",
            stage="my_s3_stage/streaming/payments",
            file_format="my_parquet_format",
            pattern=".*[.]snappy[.]parquet",
            copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        )
        CopyFromExternalStageToSnowflakeOperator(
            task_id="return_s3_to_snowflake",
            snowflake_conn_id="snowflake_default",
            table="raw_returns",
            schema="raw",
            stage="my_s3_stage/streaming/returns",
            file_format="my_parquet_format",
            pattern=".*[.]snappy[.]parquet",
            copy_options="MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        )

s3_to_snowflake()