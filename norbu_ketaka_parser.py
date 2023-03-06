from openpecha.formatters import BaseFormatter
import pandas as pd
from openpecha.core.pecha import OpenPechaFS
from openpecha.core.layer import Layer, LayerEnum
from openpecha.core.annotation import AnnBase, Span,Page
from uuid import uuid4
from openpecha.core.ids import get_initial_pecha_id
from openpecha.core.metadata import InitialPechaMetadata,InitialCreationType
from openpecha.buda.api import get_buda_scan_info,get_image_list
from pathlib import Path
from openpecha import config
from openpecha import github_utils
from pydantic import parse_obj_as, AnyHttpUrl
import os
import re
import logging
import datetime
import shutil



class csvFormatter(BaseFormatter):
    col_headers = ["work_id","image_group_id","text","image_name","line_number"]

    def __init__(self,output_path=None,metadata=None,csv_file:str=None):
        self.buda_il = {}
        super().__init__(output_path,metadata)
        if csv_file:
            self.csv_df = self.read_csv()

    def read_csv(self,path):
        mod_csv_path = update_csv_hearders(path)  
        df = pd.read_csv(mod_csv_path)
        return df

    def get_base_text(self):
        base_text = ""
        prev_image_name = self.csv_df["image_name"].iloc[0]
        for index,row in self.csv_df.iterrows():
            cur_image_name = row["image_name"]
            if prev_image_name == cur_image_name:
                base_text+=row["text"]+"\n"
            else:
                base_text+="\n"+row["text"]+"\n"
            prev_image_name = cur_image_name
        return base_text

    def order_df(self,col_priority_order):
        self.csv_df = self.csv_df.sort_values(col_priority_order)



    def get_pagination_layer(self):
        page_annotations = {}
        char_walker=0
        grouped = self.csv_df.groupby("image_name")
        for name,df in grouped:
            page_annotation,char_walker= self.get_page_annotation(df,char_walker)
            page_annotations.update(page_annotation)
        segment_layer = Layer(annotation_type=LayerEnum.pagination,annotations=page_annotations)
        return segment_layer
    
    
    def get_page_annotation(self,df,char_walker):
        start = char_walker
        try:
            res = self.get_image_meta(df)
            image_number,image_filename = res
        except:
            image_number,image_filename = None,None

        base_text = self.convert_text_list_to_string(df)
        end = char_walker + len(base_text)
        page_annotation = {uuid4().hex:Page(span=Span(start=start,end=end),imgnum=image_number,reference=image_filename)}
        return page_annotation,end+2


    def convert_text_list_to_string(self,df):
        base_text = ""
        for _,row in df.iterrows():
            base_text+=row["text"]+"\n"
        return base_text[:-1]

    def get_image_meta(self,df):
        row = df.iloc[0]
        work_id = row["work_id"]
        image_group_id = row["image_group_id"]
        image_name = str(row["image_name"])
        if (work_id,image_group_id) not in self.buda_il.keys():
            res = get_image_list(work_id, image_group_id)
            self.buda_il.update({(work_id,image_group_id):res})
            buda_il = res
        else:
            buda_il = self.buda_il[(work_id,image_group_id)]

        for image_number, image_filename in enumerate(map(lambda ii: ii["filename"], buda_il)):
            ex = re.match("(.*)\..*",image_filename)
            if ex.group(1) == image_name:
                return image_number+1,image_filename 


    def get_work_metadata(self,work_id):
        res = get_buda_scan_info(work_id)
        return res

    def get_meta(self,pecha_id,base_ids):
        bases = {}
        order = 1
        source_metadata = ""
        self.title = ""
        index,row = list(self.csv_df.iterrows())[0]
        parser = "https://github.com/OpenPecha/Norbu-Keta-eText-import/blob/main/norbu_ketaka_parser.py"
        work_id = row["work_id"]
        res = self.get_work_metadata(work_id)
        if res != None:  
            source_metadata = res["source_metadata"] 
            self.title = res["source_metadata"]["title"]

            for base_id in base_ids:
                bases.update({base_id:{
                        "source_metadata":{
                            "id":f"http://purl.bdrc.io/resource/{base_id}",
                            "total_pages":res["image_groups"][base_id]["total_pages"],
                            "volume_number":res["image_groups"][base_id]["volume_number"],
                            "volume_pages_bdrc_intro":res["image_groups"][base_id]["volume_pages_bdrc_intro"]
                        },
                        "base_file":f"{base_id}.txt",
                        "order":order
                    }})
                order+=1
            

        meta = InitialPechaMetadata(
            id = pecha_id,
            ocr_import_info={
                "bdrc_scan_id":work_id,
                "source":"bdrc",
                "ocr_info":{
                    "timestamp":"2023-02-01T00:00:00Z"
                },
                "batch_id":"batch-0002",
                "software_id":"norbuketaka",
                "expected_default_language":"bo",
                "op_import_options":None
            },
            default_language="bo",
            source = "https://library.bdrc.io",
            source_file=None,
            initial_creation_type=InitialCreationType.ocr,
            imported=datetime.datetime.now(),
            last_modified="2023-02-01T00:00:00Z",
            parser= parser,
            source_metadata=source_metadata,
            quality=None,
            bases = bases,
            copyright={
                "status":"Public domain",
                "notice":"""The proofread texts are donated by the Norbu Ketaka project led by Queenie Luo (Harvard University), Zhiying Li (Sichuan University) and Leonard van der Kuijp (Harvard University). Correspondence should be directed to  queenieluo@g.harvard.edu"""
            },
            license="CC0"
        )
        return meta

    
    def create_opf(self,csv_files:list,col_priority_order:list=None):
        """
        Paramneters:
        csv_file: str
            csv file path to format
        col_priority_order: list
            list of col to priotize in descending order priority
        """
        pecha_id = get_initial_pecha_id()
        opf_path = f"opfs/{pecha_id}/{pecha_id}.opf"
        opf = OpenPechaFS(path=opf_path)
        base_ids = []

        for csv_file in csv_files:
            self.csv_df = self.read_csv(csv_file)
            if col_priority_order:
                self.order_df(col_priority_order)
            base_text = self.get_base_text()
            pagination_layer = self.get_pagination_layer()
            base_id = self.get_base_id()
            opf.bases.update({base_id:base_text})
            opf.layers.update({base_id:{LayerEnum.pagination:pagination_layer}})
            base_ids.append(base_id)
        opf._meta = self.get_meta(pecha_id,base_ids)
        opf.save_base()
        opf.save_layers()
        opf.save_meta()
        return opf


    def get_base_id(self):
        image_group_names = self.csv_df["image_group_id"].unique()
        if len(image_group_names)>1:
            raise Exception("More than one image group names with single work id")
        return image_group_names[0]

def get_csvFiles(dir):
    searched_works = set()
    files = {}
    for path in Path(dir).iterdir():
        x = re.match("(.*)-(.*)",path.stem)
        work = x.group(1)
        file = []
        if work in searched_works:
            continue
        for path_in in Path(dir).iterdir():
            if work in path_in.stem:
                file.append(path_in.as_posix())
        files[work] = file
        searched_works.add(work)

    return files

def update_csv_hearders(csv_file):
    df = pd.read_csv(csv_file)
    correct_df = df.rename(columns={
        "text_ID":"work_id",
        "volume_ID":"image_group_id",
        "page_ID":"image_name",
        "row_number":"line_number"
    })
    mod_csv_path = f"{config.PECHAS_PATH}/{Path(csv_file).stem}.csv"
    correct_df.to_csv(mod_csv_path, index=False,header=True)
    return mod_csv_path

def publish_repo(pecha_path, asset_paths=None,private=False):
    repo = github_utils.github_publish(
        pecha_path,
        message="initial commit",
        not_includes=[],
        layers=[],
        org=os.environ.get("OPENPECHA_DATA_GITHUB_ORG"),
        token=os.environ.get("GITHUB_TOKEN"),
        private=private
       )
    if asset_paths:
        zipped_dir = create_zip_dir(asset_paths)
        repo_name = pecha_path.stem
        github_utils.create_release(
            repo_name,
            prerelease=False,
            asset_paths=[zipped_dir], 
            org=os.environ.get("OPENPECHA_DATA_GITHUB_ORG"),
            token=os.environ.get("GITHUB_TOKEN"),repo = repo
        )
        os.remove("source.zip")

def create_zip_dir(paths:Path):
    os.makedirs("./source_files")
    for path in paths:
        shutil.copy(path.as_posix(),"./source_files")
    shutil.make_archive("source",'zip',"./source_files")
    shutil.rmtree("./source_files")
    return Path("source.zip")



def set_up_logger(logger_name):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter("%(message)s")
    fileHandler = logging.FileHandler(f"{logger_name}.log")
    fileHandler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(fileHandler)
    return logger


def create_opfs(csv_files,col_priority):
    obj = csvFormatter()
    pechas_catalog = set_up_logger("pechas_catalog")
    err_log = set_up_logger("err")
    for work_id in csv_files.keys():
        opf = obj.create_opf(csv_files=csv_files[work_id],col_priority_order=col_priority)
        assets = [Path(path) for path in csv_files[work_id]]
        if opf.is_private:
            print("repo is private")
            publish_repo(pecha_path=opf.opf_path.parent,private=True,asset_paths=assets)
        else:
            print("repo is public")
            publish_repo(pecha_path=opf.opf_path.parent,private=False,asset_paths=assets)
        pechas_catalog.info(f"{opf.pecha_id},{obj.title},{work_id}")
        """ try:
            opf = obj.create_opf(csv_files=csv_files[work_id],col_priority_order=col_priority)
            assets = [Path(path) for path in csv_files[work_id]]
            if opf.is_private:
                print("repo is private")
                publish_repo(pecha_path=opf.opf_path.parent,private=True,asset_paths=assets)
            else:
                print("repo is public")
                publish_repo(pecha_path=opf.opf_path.parent,private=False,asset_paths=assets)
            pechas_catalog.info(f"{opf.pecha_id},{obj.title},{work_id}")
        except Exception as e:
            err_log.info(f"{e},{work_id}") """


def main():
    csv_files = get_csvFiles("NorbuKetaka2")
    col_priority = ["image_name","line_number"]
    create_opfs(csv_files,col_priority)
    
if __name__ == "__main__":
    main()
    
