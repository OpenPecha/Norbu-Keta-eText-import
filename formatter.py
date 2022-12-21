from openpecha.buda.api import get_buda_scan_info
import pandas as pd
import re
from pathlib import Path


def get_work_metadata(work_id):
    res = get_buda_scan_info(work_id)
    return res

def read_csv(path):
    df = pd.read_csv(path)
    df = df.sort_values(["row_number","page_ID"])
    return df

def extract_ids(str):
    x = re.match("(.*)-(.*)",str)
    work_id = x.group(1)
    image_group_id = x.group(2)
    return work_id,image_group_id

def get_csvFiles(dir):
    csv_files = [path for path in Path(dir).iterdir()]
    return csv_files

def main():
    files = get_csvFiles("08152022_queenieluo")
    for file in files:
        work_id,image_group_id = extract_ids(file.stem)
        df = read_csv(file)
        print(df)
        break

if __name__ == "__main__":
    main()

