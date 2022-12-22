from openpecha.formatters import BaseFormatter
import pandas as pd
from openpecha.buda.api import get_buda_scan_info
from openpecha.core.pecha import OpenPechaFS
from openpecha.core.layer import Layer, LayerEnum
from openpecha.core.annotation import AnnBase, Span
from uuid import uuid4
from openpecha.core.ids import get_initial_pecha_id,get_base_id
from openpecha.core.metadata import InitialPechaMetadata,InitialCreationType

from pathlib import Path
from openpecha import config



class csvFormatter(BaseFormatter):
    col_headers = ["work_id","image_group_id","text","image_name","line_number"]

    def __init__(self,output_path=None,metadata=None,csv_file:str=None):
        super().__init__(output_path,metadata)
        if csv_file:
            self.csv_df = self.read_csv()

    def read_csv(self,path):
        df = pd.read_csv(path)
        return df

    def get_base_text(self):
        base_text = ""
        for index,row in self.csv_df.iterrows():
            base_text+=row["text"]+"\n"
        return base_text

    def order_df(self,col_priority_order):
        self.csv_df = self.csv_df.sort_values(col_priority_order)


    def get_segment_layer(self):
        segment_annotations = {}
        char_walker=0
        for index,row in self.csv_df.iterrows():
            segment_annotation,char_walker= self.get_segment_annotation(row,char_walker)
            segment_annotations.update(segment_annotation)
        segment_layer = Layer(annotation_type=LayerEnum.segment,annotations=segment_annotations)
        return segment_layer
    
    def get_segment_annotation(self,row,char_walker):
        start = char_walker
        end = char_walker + len(row["text"])
        meta_data = {"work_id":row["work_id"],"image_group_id":row["image_group_id"],"image_name":row["image_name"]}
        segment_annotation = {uuid4().hex:AnnBase(span=Span(start=start,end=end),metadata=meta_data)}
        return segment_annotation,end+1

    def get_work_metadata(self,work_id):
        res = get_buda_scan_info(work_id)
        return res

    def get_meta(self,pecha_id,base_id):
        index,row = list(self.csv_df.iterrows())[0]
        work_id = row["work_id"]
        res = self.get_work_metadata(work_id)
        title = res["source_metadata"]["title"]
        langs = res["source_metadata"]["languages"]
        author= res["source_metadata"]["author"]
        meta = InitialPechaMetadata(
            id = pecha_id,
            source = "Norbu Ketaka etexts",
            initial_creation_type=InitialCreationType.ocr,
            bases = {
                base_id:{
                    "title":title,
                    "languages":langs,
                    "author":author,
                    "base_file":f"{base_id}.txt",
                    "order":1
                }
            }
        )
        return meta

    
    def create_opf(self,col_priority_order:list=None,csv_file:str=None):
        """
        Paramneters:
        csv_file: str
            csv file path to format
        col_priority_order: list
            list of col to priotize in descending order priority
        """
        if csv_file:
            self.csv_df = self.read_csv(csv_file)
        if col_priority_order:
            self.order_df(col_priority_order)

        base_text = self.get_base_text()
        segment_layer = self.get_segment_layer()
        pecha_id = get_initial_pecha_id()
        opf_path = f"opfs/{pecha_id}/{pecha_id}.opf"
        opf = OpenPechaFS(path=opf_path)
        base_id = get_base_id()
        opf.bases = {base_id:base_text}
        opf.layers = {base_id:{LayerEnum.segment:segment_layer}}
        opf._meta = self.get_meta(pecha_id,base_id)
        opf.save_base()
        opf.save_layers()
        opf.save_meta()
        return opf.opf_path

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
    col_priority = ["line_number","image_name"]
    for csv_file in csv_files:
        mod_csv_path = update_csv_hearders(csv_file)  
        obj.create_opf(csv_file=mod_csv_path,col_priority_order=col_priority)
        break
    







   
