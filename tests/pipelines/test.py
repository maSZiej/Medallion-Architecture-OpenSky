import pytest
import boto3

# @pytest.fixture(scope="session", autouse=True)
# def setup_minio_bucket():
#     s3 = boto3.client(
#         "s3",
#         endpoint_url="http://127.0.0.1:9000",
#         aws_access_key_id="minioadmin",
#         aws_secret_access_key="minioadmin"
#     )
#     bucket_name = "test-bucket"
#     buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
#     if bucket_name not in buckets:
#         s3.create_bucket(Bucket=bucket_name)