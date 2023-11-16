from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
import boto3
import os
import json

class UploadXComToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, xcom_key, bucket, key, aws_conn_id='aws_default', *args, **kwargs):
        super(UploadXComToS3Operator, self).__init__(*args, **kwargs)
        self.xcom_key = xcom_key
        self.s3_bucket = bucket
        self.s3_key = key
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        ti = context['ti']
        # xcom_data = ti.xcom_pull(task_ids=self.task_id, key=self.xcom_key)
        xcom_data = ti.xcom_pull(key=self.xcom_key)
        if not xcom_data:
            self.log.warning(f"No XCom data found for key {self.xcom_key}")
            return

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        try:
            xcom_data_json = json.dumps(xcom_data)
            # Use S3Hook to upload the XCom data to S3
            s3_hook.load_string(
                string_data=xcom_data_json,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
            self.log.info(f"XCom data uploaded to S3 at s3://{self.s3_bucket}/{self.s3_key}")
        except Exception as e:
            self.log.error(f"Failed to upload XCom data to S3: {e}")
            raise e
