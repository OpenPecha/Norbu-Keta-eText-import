from pathlib import Path
from openpecha.core.pecha import OpenPechaFS, OpenPechaGitRepo


def publish_pecha(pecha_id, pecha_path, work_dir):
    assets = Path(work_dir)
    opf_git = OpenPechaGitRepo(pecha_id, pecha_path)
    opf_git.publish(asset_path=assets, asset_name='v0.1')

if __name__ == "__main__":
    pecha_infos = Path('pecha_with_issue.txt').read_text().split('\n')
    for pecha_info in pecha_infos:
        pecha_id, work_id = pecha_info.split(',')
        pecha_path = Path(f'./opfs/{pecha_id}')
        work_dir = Path(f'./works/{work_id}')
        publish_pecha(pecha_id, pecha_path, work_dir)
        print(f'INFO: {pecha_id} published')