from openpecha.formatters import BaseFormatter
import pandas as pd
from openpecha.core.pecha import OpenPechaFS, OpenPechaGitRepo
from openpecha.core.layer import Layer, LayerEnum, PechaMetadata, SpanINFO
from openpecha.core.annotation import AnnBase, Span,Page
from uuid import uuid4
from openpecha.core.ids import get_initial_pecha_id
from openpecha.core.metadata import InitialPechaMetadata,InitialCreationType
from openpecha.buda.api import get_buda_scan_info,get_image_list
from pathlib import Path
from openpecha import config
from openpecha import github_utils
# from pydantic import parse_obj_as, AnyHttpUrl
import os
import re
import logging
import datetime
import shutil



class csvFormatter(BaseFormatter):
    col_headers = ["work_id","image_group_id","text","image_name","line_number"]

    def __init__(self,output_path=None,metadata=None):
        self.buda_il = {}
        super().__init__(output_path,metadata)

    def read_csv(self,path):
        df = pd.read_csv(path, dtype={'row_number': int, 'volume_ID': str, 'page_ID': str, 'text_ID': str, 'text': str}, na_filter=False)
        # fix for https://github.com/OpenPecha/Toolkit/issues/275
        for index, row in df.iterrows():
            volume_id = str(row['volume_ID'])
            page_id = str(row['page_ID']).replace(" ", "").replace("I1K1", "I1KG1")
            if page_id.endswith(".0"):
                page_id = page_id[:-2]

            pre, rest = volume_id[0], volume_id[1:]
            if pre == 'I' and rest.isdigit() and len(rest) == 4:
                volume_id = rest
            
            # Check if page_ID starts with volume_ID
            if not page_id.startswith(volume_id):
                logging.error(f"Page ID '{page_id}' does not start with Volume ID '{volume_id}' at row {index}.")
                continue

            # Remove the volume_ID from the start of page_ID to get the numeric part
            numeric_part = page_id[len(volume_id):]
            
            # Check if the remaining part can be converted to an integer
            try:
                numeric_part_int = int(numeric_part)
            except ValueError:
                #logging.warn(f"Page ID '"+page_id+"' has a non-integer part")
                continue

            # Format the page_ID to be volume_ID + numeric part padded to 4 digits
            updated_page_id = f"{volume_id}{numeric_part_int:04d}"
            
            # Update the dataframe with the new page_ID
            df.at[index, 'page_ID'] = updated_page_id

        df.sort_values(["page_ID","row_number"], inplace=True)
        return df

    def get_base_text(self):
        base_text = ""
        prev_image_name = self.csv_df["page_ID"].iloc[0]
        for index,row in self.csv_df.iterrows():
            cur_image_name = row["page_ID"]
            if prev_image_name == cur_image_name:
                line_text = "" if str(row["text"]) == "nan" else str(row["text"])
                base_text+=line_text+"\n"
            else:
                line_text = "" if str(row["text"]) == "nan" else str(row["text"])
                base_text+=f"\n{line_text}\n"
            prev_image_name = cur_image_name
        return base_text

    def get_pagination_layer(self, page_annotations, wlname, ilname):
        spantoannoid = {}
        for annid, ann in page_annotations.items():
            spantoannoid[str(ann["span"]["start"])+":"+str(ann["span"]["end"])] = annid
        char_walker=0
        grouped = self.csv_df.groupby("page_ID")
        for name,df in grouped:
            page_annotation,char_walker= self.get_page_annotation(df,wlname, ilname, char_walker)
            span = str(page_annotation.span.start)+":"+str(page_annotation.span.end)
            annid = spantoannoid[span] if span in spantoannoid else uuid4().hex
            page_annotations[annid] = page_annotation
        segment_layer = Layer(annotation_type=LayerEnum.pagination,annotations=page_annotations)
        return segment_layer
    
    
    def get_page_annotation(self,df,wlname, ilname, char_walker):
        start = char_walker
        image_number,image_filename = self.get_image_meta(df, wlname, ilname)
        base_text_len = self.convert_text_list_to_string_len(df)
        end = char_walker + base_text_len
        p = Page(span=Span(start=start,end=end),imgnum=image_number,reference=image_filename)
        return p,end+2


    def convert_text_list_to_string_len(self,df):
        res = 0
        for _,row in df.iterrows():
            res += 1 if str(row["text"]) == "nan" else len(row["text"])+1
        return res-1

    def get_image_meta(self,df, wlname, ilname):
        row = df.iloc[0]
        image_name = str(row["page_ID"])
        if (wlname,ilname) not in self.buda_il.keys():
            res = get_image_list(wlname, ilname)
            self.buda_il.update({(wlname,ilname):res})
            buda_il = res
        else:
            buda_il = self.buda_il[(wlname,ilname)]

        if not buda_il:
            logging.error("cannot find il for %s" % ilname)
            return None, None

        pre, rest = ilname[0], ilname[1:]
        imgprefix = ilname
        if pre == 'I' and rest.isdigit() and len(rest) == 4:
            imgprefix = rest
        

        # Remove the volume_ID from the start of page_ID to get the numeric part
        numeric_part = image_name[len(imgprefix):]

        updated_image_name = image_name
        
        # Check if the remaining part can be converted to an integer
        try:
            numeric_part_int = int(numeric_part)
            # Format the page_ID to be volume_ID + numeric part padded to 4 digits
            updated_image_name = f"{imgprefix}{numeric_part_int:04d}"
        except ValueError:
            logging.warn(f"Page ID '{image_name}' has a non-integer part")

        for image_number, image_filename in enumerate(map(lambda ii: ii["filename"], buda_il)):
            ex = re.match(r"(.*)\..*",image_filename)
            if ex.group(1) in [image_name, updated_image_name]:
                return image_number+1,image_filename
        return None, None


    def get_work_metadata(self,work_id):
        res = get_buda_scan_info(work_id)
        return res

    def get_meta(self,pecha_id,base_ids, wlname, batch_min):
        bases = {}
        order = 1
        source_metadata = ""
        self.title = ""
        parser = "https://github.com/OpenPecha/Norbu-Keta-eText-import/blob/main/norbu_ketaka_parser.py"
        res = get_buda_scan_info(wlname)
        if res != None:  
            source_metadata = res["source_metadata"] 
            self.title = res["source_metadata"]["title"]
            sorted_base_ids = sorted(base_ids, key=lambda base_id: res["image_groups"][base_id]["volume_number"])
            for base_id in sorted_base_ids:
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
                "bdrc_scan_id":wlname,
                "source":"bdrc",
                "ocr_info":{
                    "timestamp":"2023-02-01T00:00:00Z"
                },
                "batch_id":"batch-000"+str(batch_min),
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

    
    def update_opf(self, opf, csv_to_iglname, wlname, batch_min, op_id):
        base_ids = []
        for csv_file in csv_to_iglname:
            iglname = csv_to_iglname[csv_file]
            self.csv_df = self.read_csv(csv_file)
            base_text = self.get_base_text()
            layer = opf.get_layer(iglname, LayerEnum.pagination)
            pagination_layer = self.get_pagination_layer(layer.annotations if layer is not None else {}, wlname, iglname) 
            opf.bases.update({iglname:base_text})
            opf.layers.update({iglname:{LayerEnum.pagination:pagination_layer}})
            base_ids.append(iglname)
        opf._meta = self.get_meta(op_id, base_ids, wlname, batch_min)

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

def publish_repo(pecha_path, asset_paths=None,private=False):
    remote_repo = github_utils.github_publish(
        pecha_path,
        message="initial commit",
        not_includes=[],
        layers=[],
        org=os.environ.get("OPENPECHA_DATA_GITHUB_ORG"),
        token=os.environ.get("GITHUB_TOKEN"),
        private=private
       )

def create_opfs(csv_files):
    obj = csvFormatter()
    pechas_catalog = set_up_logger("pechas_catalog")
    err_log = set_up_logger("err")
    for work_id in csv_files.keys():
        opf = obj.create_opf(csv_files=csv_files[work_id])
        assets = [Path(path) for path in csv_files[work_id]]
        opf_git = OpenPechaGitRepo(opf.pecha_id, opf.opf_path)
        opf_git.publish()
        publish_repo(pecha_path=opf.opf_path.parent,private=True,asset_paths=assets)

def create_opf(work_dir, csv_formatter):
    csv_file_paths = list(Path(work_dir).iterdir())
    csv_file_paths.sort()
    opf = csv_formatter.create_opf(csv_files=csv_file_paths)
    # assets = Path(work_dir)
    # opf_git = OpenPechaGitRepo(opf.pecha_id, opf.opf_path)
    # opf_git.publish(asset_path=assets, asset_name='v0.1')
    return opf

    
if __name__ == "__main__":
    csv_formatter = csvFormatter()
    pechas_catalog = set_up_logger("pechas_catalog")
    err_log = set_up_logger("err")
    work_dirs = list(Path("./works").iterdir())
    work_dirs.sort()
    for work_dir in work_dirs:
        work_id = work_dir.stem
        try:
            if work_id == "W1KG14505":
                opf = create_opf(work_dir, csv_formatter)
                pechas_catalog.info(f"{opf.pecha_id},{csv_formatter.title},{work_dir.stem}")
        except:
            err_log.info(f"Couldn't create {work_dir.stem}")
    
