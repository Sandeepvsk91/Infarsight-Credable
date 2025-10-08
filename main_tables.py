import os
import time
import requests
import logging
import shutil
from datetime import datetime
from dotenv import load_dotenv

from cibil_pdf_extract import extract_pdf_tables
from cibil_file_import import process_local_files
#from config import LOCAL_ROOT_FOLDER,LOCAL_FILES_TO_PROCESS, LOCAL_OUTPUT_FILES, LOCAL_PROCESSED_FILES, LOCAL_LOG_FILES
from onedrive_utils import (
    get_headers,
    upload_log_file,
    list_folder_files,
    download_file,
    move_file_to_folder
)

import importlib.util
import sys
import os
from pathlib import Path

# Dynamically load config.py
BASE_DIR = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent
config_path = BASE_DIR / "config.py"

if not config_path.exists():
    raise FileNotFoundError(f"config.py not found at {config_path}")

spec = importlib.util.spec_from_file_location("config", str(config_path))
config = importlib.util.module_from_spec(spec)
sys.modules["config"] = config
spec.loader.exec_module(config)

# Now access variables like this:
LOCAL_ROOT_FOLDER = config.LOCAL_ROOT_FOLDER
LOCAL_FILES_TO_PROCESS = config.LOCAL_FILES_TO_PROCESS
LOCAL_OUTPUT_FILES = config.LOCAL_OUTPUT_FILES
LOCAL_PROCESSED_FILES = config.LOCAL_PROCESSED_FILES
LOCAL_LOG_FILES = config.LOCAL_LOG_FILES
# Load environment variables
load_dotenv()
USER_ID = os.getenv("USER_ID")

# === OneDrive Folder Structure ===
ROOT_FOLDER = "CREDABLE_REPORTS"
TARGET_FOLDER_PATH = f"{ROOT_FOLDER}/Files to Process"
ONEDRIVE_EXPORT_FOLDER = f"{ROOT_FOLDER}/Output Files"
ONEDRIVE_PROCESSED_FOLDER = f"{ROOT_FOLDER}/Processed Files"
ONEDRIVE_LOG_FOLDER = f"{ROOT_FOLDER}/Log Files"

# === Local Folder Structure ===

# === Working Directories ===
ONEDRIVE_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "OneDrive_PDFs")
CSV_OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "OneDrive_PDFs_Extracted")

# Active references
LOCAL_PDF_INPUT_DIR = LOCAL_FILES_TO_PROCESS
LOCAL_EXPORT_FOLDER = LOCAL_OUTPUT_FILES
LOG_DIR = LOCAL_LOG_FILES

UPLOAD_INTERVAL = 300  # 5 minutes

# === Directory Setup ===
def clear_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)

# Create local directories
for path in [
    LOCAL_FILES_TO_PROCESS,
    LOCAL_OUTPUT_FILES,
    LOCAL_PROCESSED_FILES,
    LOCAL_LOG_FILES,
    ONEDRIVE_DOWNLOAD_DIR,
    CSV_OUTPUT_DIR
]:
    os.makedirs(path, exist_ok=True)

def setup_logging():
    if len(logging.getLogger().handlers) > 1:
        return
    log_file = os.path.join(LOG_DIR, f"monitor_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(console)
    return log_file

def get_drive_id(headers, user_email):
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('id')

def monitor():
    log_file = setup_logging()
    headers = None
    drive_id = None
    try:
        headers = get_headers()
        drive_id = get_drive_id(headers, USER_ID)
        logging.info(" OneDrive mode enabled")
    except Exception:
        logging.warning(" OneDrive not available – falling back to local-only mode")

    last_log_upload_time = time.time()
    logging.info(" Starting hybrid PDF monitor")

    try:
        while True:
            processed_any = False

            # === 1. Process files from OneDrive if available ===
            if headers and drive_id:
                files = list_folder_files(headers, drive_id, TARGET_FOLDER_PATH)
                pdf_files = [f for f in files if f['name'].lower().endswith('.pdf')]
                if pdf_files:
                    for file in pdf_files:
                        file_name = file['name']
                        logging.info(f"[OneDrive] Processing PDF: {file_name}")
                        local_pdf = os.path.join(ONEDRIVE_DOWNLOAD_DIR, file_name)
                        download_file(headers, drive_id, file['id'], local_pdf)
                        logging.info(f"[OneDrive] Downloaded {file_name}")

                        csv_name = os.path.splitext(file_name)[0] + ".csv"
                        csv_output = os.path.join(CSV_OUTPUT_DIR, csv_name)
                        extract_pdf_tables(local_pdf, csv_output)
                        logging.info(f"[OneDrive] Extracted CSV: {csv_name}")

                        process_local_files(
                            headers=headers,
                            user_email=USER_ID,
                            local_input_dir=CSV_OUTPUT_DIR,
                            local_export_dir=LOCAL_EXPORT_FOLDER,
                            onedrive_export_folder=ONEDRIVE_EXPORT_FOLDER,
                            only_file=csv_name
                        )

                        try:
                            move_file_to_folder(headers, drive_id, file['id'], ONEDRIVE_PROCESSED_FOLDER)
                            logging.info(f"[OneDrive] Moved to: {ONEDRIVE_PROCESSED_FOLDER}")
                        except Exception as e:
                            logging.error(f"[OneDrive] Failed to move file: {e}", exc_info=True)

                        processed_any = True

            # === 2. Process files from local directory if no OneDrive PDFs ===
            if not processed_any:
                local_pdfs = [f for f in os.listdir(LOCAL_PDF_INPUT_DIR) if f.lower().endswith('.pdf')]
                if local_pdfs:
                    for file_name in local_pdfs:
                        logging.info(f"[Local] Processing PDF: {file_name}")
                        local_pdf = os.path.join(LOCAL_PDF_INPUT_DIR, file_name)

                        csv_name = os.path.splitext(file_name)[0] + ".csv"
                        csv_output = os.path.join(CSV_OUTPUT_DIR, csv_name)
                        extract_pdf_tables(local_pdf, csv_output)
                        logging.info(f"[Local] Extracted CSV: {csv_name}")

                        process_local_files(
                            headers=None,
                            user_email=None,
                            local_input_dir=CSV_OUTPUT_DIR,
                            local_export_dir=LOCAL_EXPORT_FOLDER,
                            onedrive_export_folder=None,
                            only_file=csv_name
                        )

                        # Move local PDF to "Processed Files"
                        try:
                            dest_path = os.path.join(LOCAL_PROCESSED_FILES, file_name)
                            shutil.move(local_pdf, dest_path)
                            logging.info(f"[Local] Moved to Processed: {dest_path}")
                        except Exception as move_err:
                            logging.error(f"[Local] Failed to move file: {move_err}", exc_info=True)

                        processed_any = True
                else:
                    logging.info(" No PDFs found in OneDrive or local folder.")

            # === 3. Periodic log upload to OneDrive ===
            now = time.time()
            if headers and drive_id and (now - last_log_upload_time > UPLOAD_INTERVAL):
                logging.info(" Uploading log to OneDrive...")
                try:
                    upload_log_file(headers, drive_id, log_file, ONEDRIVE_LOG_FOLDER)
                    last_log_upload_time = now
                except Exception as e:
                    logging.error(f" Log upload failed: {e}", exc_info=True)

            logging.info(" Sleeping for 30 seconds...\n")
            time.sleep(30)

    except KeyboardInterrupt:
        logging.info(" Monitoring stopped by user.")
    except Exception as e:
        logging.error(f" Unexpected error: {e}", exc_info=True)
    finally:
        # Final log upload attempt
        if headers and drive_id:
            try:
                upload_log_file(headers, drive_id, log_file, ONEDRIVE_LOG_FOLDER)
            except Exception:
                pass

if __name__ == "__main__":
    monitor()
