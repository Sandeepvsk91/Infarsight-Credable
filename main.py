import os
import time
import logging
import pdfplumber
import importlib.util
from datetime import datetime
from dotenv import load_dotenv
import main_tables
import main_text

import importlib.util
import sys
from pathlib import Path

# Dynamically load external onedrive_utils.py
BASE_DIR = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent

# === Load .env from external file ===
dotenv_path = BASE_DIR / ".env"
if not dotenv_path.exists():
    raise FileNotFoundError(f".env not found at: {dotenv_path}")
load_dotenv(dotenv_path)
USER_ID = os.getenv("USER_ID")

# === Load onedrive_utils.py dynamically from external file ===
utils_path = BASE_DIR / "onedrive_utils.py"
if not utils_path.exists():
    raise FileNotFoundError(f"onedrive_utils.py not found at: {utils_path}")

spec = importlib.util.spec_from_file_location("onedrive_utils", str(utils_path))
onedrive_utils = importlib.util.module_from_spec(spec)
sys.modules["onedrive_utils"] = onedrive_utils
spec.loader.exec_module(onedrive_utils)

# === Import functions and constants from onedrive_utils ===
get_headers = onedrive_utils.get_headers
get_drive_id = onedrive_utils.get_drive_id
get_text_drive_id = onedrive_utils.get_text_drive_id
list_folder_files = onedrive_utils.list_folder_files
download_file = onedrive_utils.download_file
move_file_to_folder = onedrive_utils.move_file_to_folder
upload_file_to_onedrive = onedrive_utils.upload_file_to_onedrive
upload_log_file = onedrive_utils.upload_log_file

TARGET_FOLDER_PATH = onedrive_utils.TARGET_FOLDER_PATH
ONEDRIVE_EXPORT_FOLDER = onedrive_utils.ONEDRIVE_EXPORT_FOLDER
ONEDRIVE_PROCESSED_FOLDER = onedrive_utils.ONEDRIVE_PROCESSED_FOLDER
ONEDRIVE_LOG_FOLDER = onedrive_utils.ONEDRIVE_LOG_FOLDER

config_path = BASE_DIR / "config.py"
if not config_path.exists():
    raise FileNotFoundError(f"config.py not found at: {config_path}")

config_spec = importlib.util.spec_from_file_location("config", str(config_path))
config = importlib.util.module_from_spec(config_spec)
sys.modules["config"] = config
config_spec.loader.exec_module(config)

# Now use config.LOCAL_FILES_TO_PROCESS, etc.
LOCAL_INPUT_PDF_DIR = config.LOCAL_FILES_TO_PROCESS
LOCAL_OUTPUT_DIR = config.LOCAL_OUTPUT_FILES
LOCAL_PROCESSED_DIR = config.LOCAL_PROCESSED_FILES
LOCAL_LOG_DIR = config.LOCAL_LOG_FILES


UPLOAD_INTERVAL = 300  # 5 minutes

os.makedirs(LOCAL_PROCESSED_DIR, exist_ok=True)

# Setup logging
def setup_logging():
    if len(logging.getLogger().handlers) > 1:
        return
    log_file = os.path.join(LOCAL_LOG_DIR, f"monitor_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)
    return log_file

# Classify PDF type
def classify_pdf(file_path):
    table_keywords = ["Borrower Profile"]
    text_keywords = ["CONSUMER CIR"]

    try:
        with pdfplumber.open(file_path) as pdf:
            content = ""
            for page in pdf.pages[:2]:
                text = page.extract_text()
                if text:
                    content += text.lower()

        table_score = sum(1 for kw in table_keywords if kw.lower() in content)
        text_score = sum(1 for kw in text_keywords if kw.lower() in content)

        if table_score > text_score:
            return "table"
        elif text_score > table_score:
            return "text"
        return "unknown"
    except Exception as e:
        logging.error(f"Failed to classify PDF {file_path}: {e}")
        return "unknown"

def process_pdf(file_path, pdf_type, headers=None, drive_id=None, file_id=None):
    try:
        if pdf_type == "table":
            csv_name = os.path.splitext(os.path.basename(file_path))[0] + ".csv"
            csv_output = os.path.join(LOCAL_OUTPUT_DIR, csv_name)
            main_tables.extract_pdf_tables(file_path, csv_output)
            logging.info(f"Extracted table CSV: {csv_output}")

            main_tables.process_local_files(
                headers=headers,
                user_email=USER_ID,
                local_input_dir=LOCAL_OUTPUT_DIR,
                local_export_dir=LOCAL_OUTPUT_DIR,
                onedrive_export_folder=ONEDRIVE_EXPORT_FOLDER,
                only_file=csv_name
            )
            logging.info(f"Processed table CSV: {csv_name}")
            os.remove(csv_output)

        elif pdf_type == "text":
            extracted_files = main_text.text_extract.extract_pdf_folder(
                os.path.dirname(file_path), output_folder=LOCAL_OUTPUT_DIR, output_format="ods"
            )
            logging.info(f"Extracted text files: {extracted_files}")
            for extracted in extracted_files:
                processed = main_text.text_import.main(extracted, output_dir=LOCAL_OUTPUT_DIR)
                logging.info(f"Processed text file: {processed}")
                if headers and drive_id:
                    upload_file_to_onedrive(processed, ONEDRIVE_EXPORT_FOLDER)
                os.remove(extracted)

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")

def monitor():
    log_file = setup_logging()
    headers = None
    drive_id = None

    try:
        headers = get_headers()
        drive_id = get_text_drive_id(headers)
        logging.info("Connected to OneDrive.")
    except Exception as e:
        logging.warning(" Failed to connect to OneDrive. Running in local mode.")

    last_log_upload_time = time.time()

    try:
        while True:
            processed_any = False

            # === OneDrive Processing ===
            if headers and drive_id:
                try:
                    files = list_folder_files(headers, drive_id, TARGET_FOLDER_PATH)
                    pdf_files = [f for f in files if f['name'].lower().endswith('.pdf')]

                    for file in pdf_files:
                        file_name = file['name']
                        file_id = file['id']
                        local_path = os.path.join(LOCAL_INPUT_PDF_DIR, file_name)

                        logging.info(f"[OneDrive] Processing {file_name}")
                        download_file(headers, drive_id, file_id, local_path)
                        pdf_type = classify_pdf(local_path)
                        logging.info(f"[OneDrive] PDF type: {pdf_type}")
                        process_pdf(local_path, pdf_type, headers, drive_id, file_id)
                        move_file_to_folder(headers, drive_id, file_id, ONEDRIVE_PROCESSED_FOLDER)
                        os.remove(local_path)
                        processed_any = True
                except Exception as e:
                    logging.error(f"Failed to process from OneDrive: {e}")

            # === Local Processing ===
            local_files = [f for f in os.listdir(LOCAL_INPUT_PDF_DIR) if f.lower().endswith('.pdf')]
            for fname in local_files:
                local_path = os.path.join(LOCAL_INPUT_PDF_DIR, fname)
                pdf_type = classify_pdf(local_path)
                logging.info(f"[Local] {fname} classified as: {pdf_type}")
                process_pdf(local_path, pdf_type)
                try:
                    dest_path = os.path.join(LOCAL_PROCESSED_DIR, fname)
                    os.rename(local_path, dest_path)
                    logging.info(f"Moved processed file to {LOCAL_PROCESSED_DIR}: {fname}")
                    logging.info("")
                except Exception as e:
                    logging.error(f"Failed to move processed file: {fname} -> {e}")
                processed_any = True

            if not processed_any:
                logging.info(" No files to process.")

            # === Upload Logs Periodically ===
            now = time.time()
            if headers and drive_id and now - last_log_upload_time > UPLOAD_INTERVAL:
                try:
                    upload_log_file(headers, drive_id, log_file, ONEDRIVE_LOG_FOLDER)
                    logging.info("Uploaded log file to OneDrive.")
                    last_log_upload_time = now
                except Exception as e:
                    logging.error(f"Failed to upload log file: {e}")

            logging.info("Sleeping for 30 seconds...\n")
            time.sleep(30)

    except KeyboardInterrupt:
        logging.info(" Monitor stopped by user.")
    except Exception as e:
        logging.error(f"Monitor crashed: {e}", exc_info=True)

if __name__ == "__main__":
    monitor()
