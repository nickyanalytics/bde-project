# test_s3.py

from dotenv import load_dotenv
import boto3
import os

load_dotenv()

bucket = os.getenv("AWS_S3_BUCKET")

s3 = boto3.client("s3")

print("Buckets:")

for b in s3.list_buckets()["Buckets"]:
    print("-", b["Name"])

print(f"\nTesting access to bucket: {bucket}")
try:
    s3.head_bucket(Bucket=bucket)
    print(f"Access to bucket '{bucket}' is successful.")
except boto3.exceptions.Boto3Error as e:
    print(f"Failed to access bucket '{bucket}': {e}")   