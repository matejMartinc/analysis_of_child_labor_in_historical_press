import os
import zipfile


def unzip_recursively(zip_path, extract_to):
    """
    Unzips a zip file, recursively unzips any nested zips,
    and removes each zip file after its successful extraction.

    Args:
        zip_path (str): The path to the zip file to be extracted.
        extract_to (str): The directory where the contents should be extracted.
    """
    try:
        # Create the extraction directory if it doesn't already exist
        if not os.path.exists(extract_to):
            os.makedirs(extract_to)

        print(f"Extracting '{os.path.basename(zip_path)}'...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)

            # After successful extraction, scan the contents for more zip files
            for item_name in zip_ref.namelist():
                item_path = os.path.join(extract_to, item_name)

                # If an extracted item is a zip file, call this function on it
                if item_name.lower().endswith('.zip') and os.path.isfile(item_path):
                    # Define a new directory for the nested zip's contents
                    nested_extract_path = os.path.splitext(item_path)[0]
                    unzip_recursively(item_path, nested_extract_path)

        # After the 'with' block is safely closed and all nested zips are handled,
        # we can remove the original zip file for this function call.
        print(f"Removing processed zip file: '{os.path.basename(zip_path)}'")
        os.remove(zip_path)

    except zipfile.BadZipFile:
        print(f"Error: '{os.path.basename(zip_path)}' is not a valid zip file or is corrupted. Skipping.")
    except Exception as e:
        print(f"An unexpected error occurred while processing '{zip_path}': {e}")

# --- Usage Example ---
# Replace 'your_project.zip' with the path to your main zipped file.
# Replace 'extracted_project' with the name of the folder where you want to extract the files.

folder = "data"
unzip_folder = "data_unziped"

files = os.listdir(folder)

for f in files:
    main_zip_file = os.path.join(folder, f)
    extraction_directory = os.path.join(unzip_folder, f)


    #Create the extraction directory if it doesn't exist
    if not os.path.exists(extraction_directory):
        os.makedirs(extraction_directory)

    unzip_recursively(main_zip_file, extraction_directory)

    print(f"All files have been extracted to '{extraction_directory}'")