import csv
from norbu_ketaka_parser import csvFormatter
from cached_git_repo import OpenpechaCachedGit
import os
from pathlib import Path
from tqdm import tqdm
import logging
from openpecha.core.pecha import OpenPechaGitRepo, OpenPechaFS
from concurrent.futures import ProcessPoolExecutor, as_completed

TOKEN = os.getenv("GITHUB_TOKEN")
GIT_CACHE_FOLDER = Path("./cache/git_cache")
GIT_PULL = False
ORG_NAME = "Openpecha-data"

FORMATTER = csvFormatter()

def import_w(wlname, csv_to_iglname, batch_num, opf_id):
    # download opf and read it:
    cached_op_git = OpenpechaCachedGit(opf_id, github_org=ORG_NAME, github_token=TOKEN, bare=False, cache_dir_path=GIT_CACHE_FOLDER)
    op = None
    try:
        git_rev = cached_op_git.get_local_latest_commit(dst_sync=GIT_PULL)
        op = cached_op_git.get_openpecha(git_rev)
        cached_op_git.release()
    except KeyboardInterrupt:
        sys.exit()
        pass
    except:
        logging.info("create local repo for "+wlname)
        op = OpenPechaFS(opf_id, GIT_CACHE_FOLDER)
    try:
        FORMATTER.update_opf(op, csv_to_iglname, wlname, batch_num, opf_id)
        op.save_base()
        op.save_layers()
        op.save_meta()
    except KeyboardInterrupt:
        sys.exit()
        pass
    except:
        logging.exception("exception in "+wlname)


def import_db(db_path):
    with open(db_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        cur_w = None
        cur_op = None
        csv_to_iglname = None
        seen_iglnames = []
        min_batch_num = 99
        rows = []
        # Collect all rows first
        for row in reader:
            rows.append(row)
        
        # Use ProcessPoolExecutor with max 4 processes
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = []
            for row in tqdm(rows):
                if cur_w != row[1]:
                    if cur_w is not None:
                        # Submit the import_w function to the executor
                        futures.append(
                            executor.submit(import_w, cur_w, csv_to_iglname, min_batch_num, cur_op)
                        )
                    cur_w = row[1]
                    cur_op = row[4]
                    min_batch_num = min(min_batch_num, int(row[3]))
                    csv_to_iglname = {}
                    seen_iglnames = []
                if row[2] in seen_iglnames:
                    logging.error(row[2]+" appears multiple times")
                else:
                    csv_to_iglname["batch"+row[3]+"/"+row[0]] = row[2]
            
            # Submit the last batch for processing
            futures.append(
                executor.submit(import_w, cur_w, csv_to_iglname, min_batch_num, cur_op)
            )

            # Ensure all tasks are completed
            for future in as_completed(futures):
                try:
                    future.result()  # Retrieve any exceptions raised in the parallel tasks
                except Exception as e:
                    logging.error(f"Error in import_w: {e}")


if __name__ == "__main__":
    import_db("db.csv")