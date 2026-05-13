from dotenv import load_dotenv
import boto3
import os

load_dotenv()

bucket = os.getenv("AWS_S3_BUCKET")

s3 = boto3.client("s3")

s3.upload_file(
    "test.txt",
    bucket,
    "test/test.txt"
)

print("Upload successful")