import contextlib
import os

import boto3
import ciso8601
import pandas as pd


# borrowing the function so this can run as a script
def safe_float(val):
    """
    Convert the given value to a float or 0f.
    """
    result = float(0)
    with contextlib.suppress(ValueError, TypeError):
        result = float(val)
    return result


S3_BUCKET_NAME = "koku-bucket"
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "kokuminioaccess"
S3_SECRET = "kokuminiosecret"

# "ciso8601.parse_datetime"
EXTRA_COLUMNS = ["extrastrcol", "extrafloat", "extratime"]
converters = {
    "filledstrcol": str,
    "emptystrcol": str,
    "filledfloat": safe_float,
    "emptyfloat": safe_float,
    "filledtime": ciso8601.parse_datetime,
}
my_df = pd.read_csv("trino-bug.csv", converters=converters)

columns = set(my_df)
columns = set(EXTRA_COLUMNS).union(columns)
columns = sorted(columns)

file_paths = []
file_path = "fake-output1.parquet"
my_df = my_df.reindex(columns=columns)
my_df.to_parquet(file_path, allow_truncated_timestamps=True, coerce_timestamps="ms", index=False)
file_paths.append(file_path)

my_df_2 = pd.read_csv("trino-bug.csv", converters=converters)
file_path = "fake-output2.parquet"
my_df_2["extrastrcol"] = pd.Series(dtype=str)
my_df_2["extrafloat"] = pd.Series(dtype=float)
my_df_2["extratime"] = pd.Series(dtype="datetime64[ns]")
my_df_2.to_parquet(file_path, allow_truncated_timestamps=True, coerce_timestamps="ms", index=False)
file_paths.append(file_path)

aws_session = boto3.Session(
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET,
)
s3_resource = aws_session.resource("s3", endpoint_url=S3_ENDPOINT)
for file_path in file_paths:
    s3_obj = {"bucket_name": S3_BUCKET_NAME, "key": file_path}
    upload = s3_resource.Object(**s3_obj)
    with open(file_path, "rb") as data:
        upload.upload_fileobj(data)
    os.remove(file_path)


"""
CREATE TABLE hive.org1234567.fake_parquet(
   filledstrcol varchar,
   emptystrcol varchar,
   extrastrcol varchar,
   filledfloat double,
   emptyfloat double,
   extrafloat double,
   filledtime timestamp,
   extratime timestamp
)
WITH (
   external_location = 's3a://koku-bucket/',
   format = 'PARQUET'
) ;

"""
