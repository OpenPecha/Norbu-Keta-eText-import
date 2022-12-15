import pandas as pd
import boto3
import botocore
import os
import hashlib

# S3 config
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "~/.aws/credentials"
ARCHIVE_BUCKET = "archive.tbrc.org"
OCR_OUTPUT_BUCKET = "ocr.bdrc.io"
S3 = boto3.resource("s3")
S3_client = boto3.client("s3")
archive_bucket = S3.Bucket(ARCHIVE_BUCKET)
ocr_output_bucket = S3.Bucket(OCR_OUTPUT_BUCKET)

def read_csv(path):
    df = pd.read_csv(path,header=None)
    df = df.sort_values([3,2])
    return df

def get_s3_prefix_path(
    work_local_id, imagegroup, service_id=None, batch_id=None, data_types=None
):
    """
    the input is like W22084, I0886. The output is an s3 prefix ("folder"), the function
    can be inspired from
    https://github.com/buda-base/volume-manifest-tool/blob/f8b495d908b8de66ef78665f1375f9fed13f6b9c/manifestforwork.py#L94
    which is documented
    """
    md5 = hashlib.md5(str.encode(work_local_id))
    two = md5.hexdigest()[:2]

    pre, rest = imagegroup[0], imagegroup[1:]
    if pre == "I" and rest.isdigit() and len(rest) == 4:
        suffix = rest
    else:
        suffix = imagegroup

    base_dir = f"Works/{two}/{work_local_id}"
    if service_id is not None:
        batch_dir = f"{base_dir}/{service_id}/{batch_id}"
        paths = {BATCH_PREFIX: batch_dir}
        for dt in data_types:
            paths[dt] = f"{batch_dir}/{dt}/{work_local_id}-{suffix}"
        return paths
    return f"{base_dir}/images/{work_local_id}-{suffix}"


if __name__ == "__main__":
    df = read_csv("sample.csv")
    