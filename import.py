import pandas as pd
import boto3
import botocore
import os
import hashlib
import json
from pathlib import Path
import re
import requests
from openpecha.buda.api import get_buda_scan_info,get_image_list

# S3 config
BATCH_PREFIX = 'batch'
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "~/.aws/credentials"
OCR_OUTPUT_BUCKET = "ocr.bdrc.io"
S3 = boto3.resource("s3")
S3_client = boto3.client("s3")
ocr_output_bucket = S3.Bucket(OCR_OUTPUT_BUCKET)

def read_csv(path):
    df = pd.read_csv(path)
    df = df.sort_values(["row_number","page_ID"])
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
    return f"s3://ocr.bdrc.io/{base_dir}/norbuketaka/batch-0001"

def get_info_json():
    info_json = {
   "timestamp": "2022-08-25T00:00:00Z"
}
    return info_json

def archive_on_s3(s3_path,csv_file:Path):
    info_json=get_info_json()
    csv_content = csv_file.read_text(encoding="utf-8")
    s3_ocr_info_path = f"{s3_path}/{INFO_FN}"
    s3_ocr_csv_path = f"{s3_path}/{csv_file.stem}.csv"
    ocr_output_bucket.put_object(
        Key=s3_ocr_info_path, Body=(bytes(json.dumps(info_json).encode("UTF-8")))
    )
    ocr_output_bucket.put_object(Key=s3_ocr_csv_path,Body= csv_content)
    print(s3_ocr_csv_path)
    print(s3_ocr_info_path)

def get_csvFiles(dir):
    csv_files = [path for path in Path(dir).iterdir()]
    return csv_files

def extract_ids(str):
    x = re.match("(.*)-(.*)",str)
    work_id = x.group(1)
    image_group_id = x.group(2)
    return work_id,image_group_id
    

def main(csv_file:Path):
    work_id,image_group_id = extract_ids(csv_file.stem)
    s3_prefix = get_s3_prefix_path(work_id,image_group_id)
    archive_on_s3(s3_prefix,csv_file)


def extract_meta():
    res = get_image_list("W00EGS1016761","I01JW66")
    print(res)

def get_image(bdrc_scan_id,image_group_id):
        buda_il = get_image_list(bdrc_scan_id, image_group_id)
        # format should be a list of image_id (/ file names)
        return map(lambda ii: ii["filename"], buda_il)

if __name__ == "__main__":
    #df = read_csv("sample.csv")
    #prefix = get_s3_prefix_path("W1K2118","I1K2126")
    #archive_on_s3(prefix)
    #S3_client.download_file(OCR_OUTPUT_BUCKET,f"s3://ocr.bdrc.io/Works/37/W4CZ1042/norbuketaka/batch-0001/info.json","demo.json")
    #S3_client.download_file(OCR_OUTPUT_BUCKET,f"s3://ocr.bdrc.io/Works/37/W4CZ1042/norbuketaka/batch-0001/W4CZ1042-I1PD108815.csv","demo.csv")
    #extract_meta()
    #df = read_csv("08152022_queenieluo/W00EGS1016761-I01JW66.csv")
    #df.to_csv("new.csv")
    image_list = get_image("W00EGS1016761","I01JW66")
    for image_number, image_filename in enumerate(image_list):
        print(image_number,image_filename)
    