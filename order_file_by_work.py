import os
import shutil
from norbu_ketaka_parser import get_csvFiles

def copy_files_to_directories(file_dict):
    """
    This function takes a dictionary as input where the keys are directory names
    and the values are lists of file paths. It creates a directory for each key
    and copies the files from the corresponding list into the created directory.
    
    :param file_dict: Dictionary containing directory names as keys and lists of file paths as values
    :type file_dict: dict[str, List[str]]
    """
    for dir_name, file_paths in file_dict.items():
        dir_name = f'./works/{dir_name}'
        if not os.path.exists(dir_name):  # Check if the directory already exists
            os.makedirs(dir_name)  # If not, create the directory
        for file_path in file_paths:
            if os.path.isfile(file_path):  # Check if the source file exists
                shutil.copy(file_path, dir_name)  # If yes, copy the file to the destination directory
            else:
                print(f"Warning: {file_path} does not exist and will not be copied.")

if __name__ == "__main__":
    csv_files_path = "BDRC_cleaned"
    csv_files = get_csvFiles(csv_files_path)
    copy_files_to_directories(csv_files)
