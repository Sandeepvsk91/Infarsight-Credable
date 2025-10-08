import os

HOME = os.path.expanduser("~")
DOWNLOADS = os.path.join(HOME, "Downloads")

FOLDER_NAME = "CREDABLE_FILES"
LOCAL_ROOT_FOLDER = os.path.join(DOWNLOADS, FOLDER_NAME)

# Define the root folder once (choose one or make both if you need separate outputs)
#LOCAL_ROOT_FOLDER = os.path.expanduser("~/Downloads/CREDABLE_LOCAL")
LOCAL_FILES_TO_PROCESS = os.path.join(LOCAL_ROOT_FOLDER, "Files to Process")
LOCAL_OUTPUT_FILES = os.path.join(LOCAL_ROOT_FOLDER, "Output Files")
LOCAL_PROCESSED_FILES = os.path.join(LOCAL_ROOT_FOLDER, "Processed Files")
LOCAL_LOG_FILES = os.path.join(LOCAL_ROOT_FOLDER, "Log Files")

# Ensure the directories exist
#for folder in [LOCAL_FILES_TO_PROCESS, LOCAL_OUTPUT_FILES, LOCAL_PROCESSED_FILES, LOCAL_LOG_FILES]:
 #   os.makedirs(folder, exist_ok=True)
