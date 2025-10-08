import os
import time
import shutil
import logging
from datetime import datetime
from dotenv import load_dotenv
#from config import LOCAL_ROOT_FOLDER,LOCAL_FILES_TO_PROCESS, LOCAL_OUTPUT_FILES, LOCAL_PROCESSED_FILES, LOCAL_LOG_FILES

import text_extract
import text_import
from onedrive_utils import (
    get_headers,
    get_text_drive_id,
    upload_log_file,
    list_folder_files,
    download_file,
    move_file_to_folder,
    upload_file_to_onedrive
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

# Access config variables
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


# === Working Folders ===
ONEDRIVE_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "OneDrive_PDFs")
EXTRACTED_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "OneDrive_PDFs_Extracted")

# Constants
UPLOAD_INTERVAL = 300  # 5 minutes

# === Directory Setup ===
def clear_and_create_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)

def setup_directories():
    for path in [
        LOCAL_FILES_TO_PROCESS,
        LOCAL_OUTPUT_FILES,
        LOCAL_PROCESSED_FILES,
        LOCAL_LOG_FILES,
        ONEDRIVE_DOWNLOAD_DIR,
        EXTRACTED_DIR,
    ]:
        os.makedirs(path, exist_ok=True)

def setup_logging():
    if len(logging.getLogger().handlers) > 1:
        return
    log_file = os.path.join(LOCAL_LOG_FILES, f"text_monitor_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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

# === Main Monitor Loop ===
def monitor():
    setup_directories()
    log_file = setup_logging()

    headers = None
    drive_id = None

    try:
        headers = get_headers()
        drive_id = get_text_drive_id(headers)
        logging.info(" OneDrive mode enabled")
    except Exception:
        logging.warning(" OneDrive unavailable – switching to local-only mode")

    last_log_upload_time = time.time()

    try:
        while True:
            processed_any = False

            # === 1. OneDrive Processing ===
            if headers and drive_id:
                files = list_folder_files(headers, drive_id, TARGET_FOLDER_PATH)
                pdf_files = [f for f in files if f['name'].lower().endswith('.pdf')]

                if pdf_files:
                    for file in pdf_files:
                        file_name = file['name']
                        item_id = file['id']
                        local_pdf_path = os.path.join(ONEDRIVE_DOWNLOAD_DIR, file_name)

                        download_file(headers, drive_id, item_id, local_pdf_path)
                        logging.info(f"[OneDrive] Downloaded → {file_name}")

                        extracted_files = text_extract.extract_pdf_folder(
                            ONEDRIVE_DOWNLOAD_DIR, output_folder=EXTRACTED_DIR, output_format="ods"
                        )
                        logging.info(f"[OneDrive] Extracted: {extracted_files}")

                        for extracted_file in extracted_files:
                            processed_path = text_import.main(extracted_file, output_dir=LOCAL_OUTPUT_FILES)
                            logging.info(f"[OneDrive] Processed → {processed_path}")
                            upload_file_to_onedrive(processed_path, ONEDRIVE_EXPORT_FOLDER)

                        move_file_to_folder(headers, drive_id, item_id, ONEDRIVE_PROCESSED_FOLDER)
                        logging.info(f"[OneDrive] Moved to → {ONEDRIVE_PROCESSED_FOLDER}")
                        processed_any = True

            # === 2. Local Processing Fallback ===
            if not processed_any:
                local_pdfs = [f for f in os.listdir(LOCAL_FILES_TO_PROCESS) if f.lower().endswith('.pdf')]
                if local_pdfs:
                    for file_name in local_pdfs:
                        local_pdf_path = os.path.join(LOCAL_FILES_TO_PROCESS, file_name)
                        logging.info(f"[Local] Processing → {file_name}")

                        extracted_files = text_extract.extract_pdf_folder(
                            LOCAL_FILES_TO_PROCESS, output_folder=EXTRACTED_DIR, output_format="ods"
                        )
                        logging.info(f"[Local] Extracted: {extracted_files}")

                        for extracted_file in extracted_files:
                            processed_path = text_import.main(extracted_file, output_dir=LOCAL_OUTPUT_FILES)
                            logging.info(f"[Local] Processed → {processed_path}")

                        # Move local PDF to processed
                        dest_path = os.path.join(LOCAL_PROCESSED_FILES, file_name)
                        shutil.move(local_pdf_path, dest_path)
                        logging.info(f"[Local] Moved to → {dest_path}")
                        processed_any = True
                else:
                    logging.info(" No PDFs found in OneDrive or local input.")

            # === 3. Periodic log upload to OneDrive ===
            now = time.time()
            if headers and drive_id and (now - last_log_upload_time > UPLOAD_INTERVAL):
                logging.info(" Uploading log to OneDrive...")
                try:
                    upload_log_file(headers, drive_id, log_file, ONEDRIVE_LOG_FOLDER)
                    last_log_upload_time = now
                except Exception as e:
                    logging.error(f" Log upload failed: {e}")

            logging.info(" Sleeping for 30 seconds...\n")
            time.sleep(30)

    except KeyboardInterrupt:
        logging.info(" Monitoring stopped by user.")
    except Exception as e:
        logging.error(f" Unexpected error: {e}", exc_info=True)
    finally:
        if headers and drive_id:
            try:
                upload_log_file(headers, drive_id, log_file, ONEDRIVE_LOG_FOLDER)
            except Exception:
                pass

if __name__ == "__main__":
    monitor()
