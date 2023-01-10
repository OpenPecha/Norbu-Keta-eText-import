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
import re
from pydantic import parse_obj_as,AnyHttpUrl



class csvFormatter(BaseFormatter):
    col_headers = ["work_id","image_group_id","text","image_name","line_number"]

    def __init__(self,output_path=None,metadata=None,csv_file:str=None):
        self.buda_il = {}
        super().__init__(output_path,metadata)
        if csv_file:
            self.csv_df = self.read_csv()

    def read_csv(self,path):
        splitted_dfs = []
        df = pd.read_csv(path)
        dfs = df.groupby("work_id")
        keys = list(dfs.groups)
        for key in keys:
            df = dfs.get_group(key)
            cleaned_df = df.dropna(how="all")
            splitted_dfs.append(cleaned_df)
        return splitted_dfs

    def get_base_text(self):
        base_text = ""
        for index,row in self.csv_df.iterrows():
            base_text+=row["text"]+"\n"
        return base_text

    def order_df(self,col_priority_order):
        self.csv_df = self.csv_df.sort_values(col_priority_order)


    def get_pagination_layer(self):
        page_annotations = {}
        char_walker=0
        for _,row in self.csv_df.iterrows():
            page_annotation,char_walker= self.get_page_annotation(row,char_walker)
            page_annotations.update(page_annotation)
        segment_layer = Layer(annotation_type=LayerEnum.pagination,annotations=page_annotations)
        return segment_layer
    
    def get_page_annotation(self,row,char_walker):
        start = char_walker
        res = self.get_image_meta(row)
        image_number,image_filename = res
        end = char_walker + len(row["text"])
        page_annotation = {uuid4().hex:Page(span=Span(start=start,end=end),imgnum=image_number,reference=image_filename)}
        return page_annotation,end+1

    def get_image_meta(self,row):
        work_id = row["work_id"]
        image_group_id = row["image_group_id"]
        if (work_id,image_group_id) not in self.buda_il.keys():
            res = get_image_list(work_id, image_group_id)
            print("getting image meta")
            self.buda_il.update({(work_id,image_group_id):res})
            buda_il = res
        else:
            buda_il = self.buda_il[(work_id,image_group_id)]

        for image_number, image_filename in enumerate(map(lambda ii: ii["filename"], buda_il)):
            ex = re.match("(.*)\..*",image_filename)
            if ex.group(1) == row["image_name"]:
                return image_number,image_filename 
        return 

    def get_work_metadata(self,work_id):
        res = get_buda_scan_info(work_id)
        return res

    def get_meta(self,pecha_id,base_id):
        index,row = list(self.csv_df.iterrows())[0]
        work_id = row["work_id"]
        parser = """https://github.com/OpenPecha/Toolkit/blob/7f57883d84bc10351527a49ee6ce970ace404e50/openpecha/formatters/google_ocr.py"""
        res = self.get_work_metadata(work_id)
        title = res["source_metadata"]["title"]
        langs = res["source_metadata"]["languages"]
        if "author" in  res["source_metadata"].keys():
            author= res["source_metadata"]["author"]
        else:
            author=None
        meta = InitialPechaMetadata(
            id = pecha_id,
            ocr_import_info={
                "bdrc_scan_id":work_id,
                "source":"bdrc",
                "ocr_info":{
                    "timestamp":"2022-08-25T00:00:00Z"
                },
                "batch_id":"batch001",
                "software_id":"norbuketatka",
                "expected_default_language":"bo",
                "op_import_options":None,
                "op_import_versio":"1.0.0"
            },
            default_language="bo",
            source = "https://library.bdrc.io",
            source_file=None,
            initial_creation_type=InitialCreationType.ocr,
            imported="2022-08-31T09:26:20.614553+00:00",
            last_modified="2022-08-25T00:00:00Z",
            parser= parser,
            source_metadata={
                "id":f"http://purl.bdrc.io/resource/{work_id}",
                "status": "http://purl.bdrc.io/admindata/StatusReleased",
                "access": "http://purl.bdrc.io/admindata/AccessOpen",
                "reproduction_of": "http://purl.bdrc.io/resource/MW3PD1002",
                "copyright_status": "http://purl.bdrc.io/resource/CopyrightClaimed",
                "title":title,
                "author":author,
            },
            quality=None,
            bases = {
                base_id:{
                    "source_metadata":{
                        "id":f"http://purl.bdrc.io/resource/{work_id}",
                        "total_pages":res["image_groups"][base_id]["total_pages"],
                        "volume_number":res["image_groups"][base_id]["volume_number"],
                        "volume_pages_bdrc_intro":res["image_groups"][base_id]["volume_pages_bdrc_intro"]
                    },
                    "base_file":f"{base_id}.txt",
                    "order":1
                }
            },
            copyright={
                "status":"Public domain",
                "notice":"""The proofread texts are donated by the Norbu Ketaka project led by Queenie Luo (Harvard University), Zhiying Li (Sichuan University) and Leonard van der Kuijp (Harvard University). Correspondence should be directed to  queenieluo@g.harvard.edu"""
            },
            license="CC0"
        )
        return meta

    
    def create_opf(self,csv_file:str,col_priority_order:list=None):
        """
        Paramneters:
        csv_file: str
            csv file path to format
        col_priority_order: list
            list of col to priotize in descending order priority
        """
        self.csv_dfs = self.read_csv(csv_file)
        
        for self.csv_df in self.csv_dfs:
            if col_priority_order:
                self.order_df(col_priority_order)
            base_text = self.get_base_text()
            pagination_layer = self.get_pagination_layer()
            pecha_id = get_initial_pecha_id()
            opf_path = f"opfs/{pecha_id}/{pecha_id}.opf"
            opf = OpenPechaFS(path=opf_path)
            base_id = self.get_base_id()
            opf.bases = {base_id:base_text}
            opf.layers = {base_id:{LayerEnum.pagination:pagination_layer}}
            opf._meta = self.get_meta(pecha_id,base_id)
            opf.save_base()
            opf.save_layers()
            opf.save_meta()


    def get_base_id(self):
        image_group_names = self.csv_df["image_group_id"].unique()
        if len(image_group_names)>1:
            raise Exception("More than one image group names with single work id")
        return image_group_names[0]

def get_csvFiles(dir):
    csv_files = [path for path in Path(dir).iterdir()]
    return csv_files

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

if __name__ == "__main__":
    obj = csvFormatter()
    csv_files = get_csvFiles("08152022_queenieluo")
    #csv_files = ["08152022_queenieluo/W3PD1002-I1KG81129.csv"]
    col_priority = ["line_number","image_name"]
    for csv_file in csv_files:
        mod_csv_path = update_csv_hearders(csv_file)  
        obj.create_opf(csv_file=mod_csv_path,col_priority_order=col_priority)
        break
    