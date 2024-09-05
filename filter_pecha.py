from pathlib import Path
import shutil


def copy_folder(src, dst):
    src_path = Path(src)
    dst_path = Path(dst)

    # Ensure the source directory exists
    if not src_path.is_dir():
        raise ValueError(f"Source directory {src} does not exist")

    # Create the destination directory if it does not exist
    dst_path.mkdir(parents=True, exist_ok=True)

    # Copy each item in the directory
    for item in src_path.iterdir():
        dest_item = dst_path / item.name
        if item.is_dir():
            shutil.copytree(item, dest_item)
        else:
            shutil.copy2(item, dest_item)


if __name__ == "__main__":
    pecha_ids = Path('./pecha_with_issue.txt').read_text().splitlines()
    for pecha_id in pecha_ids:
        pecha_path = Path(f'./opfs/{pecha_id}')
        if pecha_path.exists():
            copy_folder(pecha_path, Path('./unpublished_pecha/'))
