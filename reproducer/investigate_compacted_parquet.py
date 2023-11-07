import json
import os

import boto3
import pyarrow.parquet as pq

# The compactor spits out the s3 path for you.
S3_BUCKET_NAME = "koku-bucket"
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "kokuminioaccess"
S3_SECRET = "kokuminiosecret"

# Download from minio
compacted_parquet_file = "data/parquet/compactor_test/data_8c27620e8a2d49b78dc3b55475681c86.parquet"
aws_session = boto3.Session(
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET,
)
s3_resource = aws_session.resource("s3", endpoint_url=S3_ENDPOINT)
s3_bucket = s3_resource.Bucket(S3_BUCKET_NAME)
for s3_object in s3_bucket.objects.filter(Prefix=compacted_parquet_file):
    s3_object_key = s3_object.key
    local_file_path = os.path.basename(s3_object_key)
    s3_bucket.download_file(s3_object_key, local_file_path)

log_info = {
    "file_created_with_reindex": "fake-output1.parquet",
    "new_version": "fake-output2.parquet",
    "Compacted file": "data_d8ed942575ca41b28acdfb6df83aff96.parquet",
    # "different backend": "different_backend.parquet"
}

prefix = "files/"
for log_msg, filename in log_info.items():
    file_path = prefix + filename
    print("\n--")
    print(log_msg)
    print("*********")
    schema = pq.read_schema(file_path)
    print(schema)
    metadata = schema.metadata
    for key, metadata_bytes in metadata.items():
        metadata_string = metadata_bytes.decode("utf-8")
        metadata_dict = json.loads(metadata_string)
        columns_metadata = metadata_dict.get("columns")

        if columns_metadata:
            for column in columns_metadata:
                if column.get("name") in ["extrafloat", "extrastrcol", "extratime"]:
                    print(column)
