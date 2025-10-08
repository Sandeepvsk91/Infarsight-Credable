import os
import requests
import logging
from urllib.parse import quote
from dotenv import load_dotenv
from config import LOCAL_ROOT_FOLDER,LOCAL_FILES_TO_PROCESS, LOCAL_OUTPUT_FILES, LOCAL_PROCESSED_FILES, LOCAL_LOG_FILES

load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_ID = os.getenv("USER_ID")
SCOPE = ["https://graph.microsoft.com/.default"]

ROOT_FOLDER = "CREDABLE_REPORTS"
TARGET_FOLDER_PATH = f"{ROOT_FOLDER}/Files to Process"
ONEDRIVE_EXPORT_FOLDER = f"{ROOT_FOLDER}/Output Files"
ONEDRIVE_PROCESSED_FOLDER = f"{ROOT_FOLDER}/Processed Files"
ONEDRIVE_LOG_FOLDER = f"{ROOT_FOLDER}/Log Files"



def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "scope": " ".join(SCOPE),
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json().get("access_token")

def get_headers():
    token = get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def get_drive_id(headers, user_email):
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('id')

def get_text_drive_id(headers):
    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('id')

def list_folder_files(headers, drive_id, folder_path):
    encoded_path = quote(folder_path)
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_path}:/children"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])

def download_file(headers, drive_id, item_id, dest_path):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def move_file_to_folder(headers, drive_id, item_id, target_folder_path):
    target_folder_encoded = quote(target_folder_path)
    folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{target_folder_encoded}"
    folder_resp = requests.get(folder_url, headers=headers)
    folder_resp.raise_for_status()
    folder_id = folder_resp.json()["id"]

    move_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}"
    move_data = {"parentReference": {"id": folder_id}}
    move_resp = requests.patch(move_url, headers=headers, json=move_data)
    move_resp.raise_for_status()
    logging.info(f"Moved OneDrive file to → {target_folder_path}")
    logging.info("\n")

def upload_file_to_onedrive(local_path, onedrive_folder):
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    file_name = os.path.basename(local_path)
    encoded_path = quote(f"{onedrive_folder}/{file_name}")
    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/root:/{encoded_path}:/content"
    with open(local_path, "rb") as f:
        resp = requests.put(url, headers=headers, data=f)
    if resp.ok:
        logging.info(f"Uploaded to OneDrive → {file_name}")
        return True
    else:
        logging.error(f" Upload failed for {file_name}: {resp.status_code} - {resp.text}")
        return False

def upload_log_file(headers, drive_id, local_log_path, target_onedrive_folder):
    file_name = os.path.basename(local_log_path)
    target_path = f"{target_onedrive_folder}/{file_name}"
    encoded = quote(target_path)
    with open(local_log_path, 'rb') as f:
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded}:/content"
        response = requests.put(url, headers=headers, data=f)
    if response.ok:
        logging.info(" Log uploaded to OneDrive")
    else:
        logging.error(f" Log upload failed: {response.status_code} - {response.text}")
