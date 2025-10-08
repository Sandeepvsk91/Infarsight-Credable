import ezodf
import pandas as pd
import numpy as np
import os
import sys
import requests


# OneDrive Upload functions


def get_auth_headers():                                  #---   ( modified for local)
    # TODO: Replace with your actual auth implementation (OAuth2 token retrieval)
    raise NotImplementedError("Implement get_auth_headers() to return auth headers")

def upload_file_to_onedrive(headers, user_email, local_file_path, remote_folder):     #----     (Modified for local)
    """
    Uploads a local XLSX file to a user's OneDrive folder (Excel Online).
    """
    file_name = os.path.basename(local_file_path)
    upload_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/root:/{remote_folder}/{file_name}:/content"

    with open(local_file_path, "rb") as f:
        data = f.read()

    print(f"Uploading {file_name} to OneDrive folder '{remote_folder}' ...")
    response = requests.put(
        upload_url,
        headers={**headers, "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
        data=data
    )

    if response.status_code in (200, 201):
        print(f"Upload successful: {file_name}")
    else:
        print(f"Upload failed: {response.status_code} - {response.text}")
        response.raise_for_status()


# Helper functions

def load_input_file(input_path):
    ext = os.path.splitext(input_path)[1].lower()

    if ext == ".ods":
        print("Reading .ods file...")
        doc = ezodf.opendoc(input_path)
        sheet = doc.sheets[0]
        data = []
        for row in sheet.rows():
            data.append([cell.value for cell in row])
        return data

    elif ext == ".xlsx":
        print("Reading .xlsx file...")
        df = pd.read_excel(input_path, header=None)
        return df.values.tolist()

    elif ext == ".txt":
        print("Reading .txt file...")
        with open(input_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        data = [line.strip().split("\t") for line in lines]
        return data

    else:
        raise ValueError(f"Unsupported file format: {ext}")

def get_output_path(input_path):
    base, ext = os.path.splitext(input_path)
    filename = os.path.basename(base)

    if "_input" in filename:
        output_filename = filename.replace("_input", "_output") + ".xlsx"
    else:
        output_filename = filename + "_output.xlsx"

    output_dir = os.path.dirname(input_path)
    return os.path.join(output_dir, output_filename)

def clean_str(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    return str(val).strip()

def save_to_xlsx(df, out_path):
    df.to_excel(out_path, index=False, engine='openpyxl')
    print(f"Saved output to {out_path}")


# Main 

def main(input_path, output_dir=None, headers=None, user_email=None, remote_folder=None): 
#def main(input_path, output_dir=None):  # (modified for local)
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path = get_output_path(input_path)

    if output_dir:
        filename = os.path.basename(output_path)
        output_path = os.path.join(output_dir, filename)

    # Read input ODS file
    doc = ezodf.opendoc(input_path)
    sheet = doc.sheets[0]

    last_seen = {}
    last_field_per_page = {}
    data_rows = []

    for row in sheet.rows():
        values = [cell.value for cell in row]
        if len(values) < 6:
            continue
        page, pan, name, score, field, value = values[:6]

        if str(page).strip().upper() in ["NAME", "PAGE"] or str(pan).strip().upper() == "PAN":
            continue

        page = clean_str(page)
        try:
            page = int(float(page))
        except:
            continue

        pan = clean_str(pan)
        name = clean_str(name)
        score = clean_str(score)
        field = clean_str(field).upper().strip().rstrip(":")
        value = clean_str(value)

        if page not in last_seen:
            last_seen[page] = {
                "PAN Number": "",
                "Entity Name/ Director Name": "",
                "CMR Rank/Credit Score": "",
                "Page": page
            }
        if pan:
            last_seen[page]["PAN Number"] = pan
        if name:
            last_seen[page]["Entity Name/ Director Name"] = name
        if score:
            last_seen[page]["CMR Rank/Credit Score"] = score

        current = last_seen[page]

        if not field:
            if page in last_field_per_page:
                last_field = last_field_per_page[page]
                appended = False
                for r in reversed(data_rows):
                    if r["Page"] == page and r["Field"] == last_field:
                        r["Value"] = str(r["Value"]) + " " + value
                        appended = True
                        break
                if not appended:
                    continue
            else:
                continue
        else:
            last_field_per_page[page] = field
            data_rows.append({
                "Page": page,
                "PAN": current["PAN Number"],
                "Name": current["Entity Name/ Director Name"],
                "Score": current["CMR Rank/Credit Score"],
                "Field": field,
                "Value": value
            })

    facilities = []
    current_facility = None

    for row in data_rows:
        field = row["Field"]
        value = row["Value"]
        pan = row["PAN"]
        name = row["Name"]
        score = row["Score"]
        page = row["Page"]

        if field == "TYPE":
            if current_facility:
                facilities.append(current_facility)
            current_facility = {
                "Entity Name/ Director Name": name,
                "PAN Number": pan,
                "CMR Rank/Credit Score": score,
                "Facility type": value,
                "Page": page,
                "Guarantor/Borrower/Individual/Joint": "",
                "Sanction limit": "",
                "O/s Amount": "",
                "DPDs": "",
                "Overdue": ""
            }
        elif current_facility:
            if field == "SANCTIONED":
                current_facility["Sanction limit"] = value
            elif field == "HIGH CREDIT" and not current_facility["Sanction limit"]:
                current_facility["Sanction limit"] = value
            elif field == "CURRENT BALANCE":
                current_facility["O/s Amount"] = value
            elif field == "DPD":
                cleaned = value
                for prefix in [
                    "DAYS PAST DUE/ASSET CLASSIFICATION (UP TO 36 MONTHS; LEFT TO RIGHT)",
                    "DAYS PAST DUE/ASSET CLASSIFICATION",
                    "DPD:"
                ]:
                    if cleaned.upper().startswith(prefix.upper()):
                        cleaned = cleaned[len(prefix):].strip()
                        break
                current_facility["DPDs"] = cleaned
            elif field == "OWNERSHIP":
                current_facility["Guarantor/Borrower/Individual/Joint"] = value
            elif field == "OVERDUE":
                current_facility["Overdue"] = value

    if current_facility:
        facilities.append(current_facility)

    all_pages = sorted(last_seen.keys())
    pages_with_data = {f["Page"] for f in facilities}

    for page in all_pages:
        if page not in pages_with_data:
            meta = last_seen[page]
            facilities.append({
                "Entity Name/ Director Name": meta["Entity Name/ Director Name"],
                "PAN Number": meta["PAN Number"],
                "CMR Rank/Credit Score": meta["CMR Rank/Credit Score"],
                "Facility type": "",
                "Page": meta["Page"],
                "Guarantor/Borrower/Individual/Joint": "",
                "Sanction limit": "",
                "O/s Amount": "",
                "DPDs": "",
                "Overdue": ""
            })

    final_df = pd.DataFrame(facilities)[[
        "Entity Name/ Director Name", "PAN Number", "CMR Rank/Credit Score",
        "Facility type", "Page", "Guarantor/Borrower/Individual/Joint",
        "Sanction limit", "O/s Amount", "DPDs", "Overdue"
    ]]

    final_df.sort_values(by="Page", inplace=True)
    final_df.fillna("No Data", inplace=True)

    save_to_xlsx(final_df, output_path)

    # Upload to OneDrive if params provided
    if headers and user_email and remote_folder:                     # (modified for local)
        upload_file_to_onedrive(headers, user_email, output_path, remote_folder)

    return output_path

# CLI Execution

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python your_script.py <input_file> [output_dir]")
        sys.exit(1)

    input_file_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        # Implement your auth logic here
        headers = get_auth_headers()           #(modified for local and next 2 lines)
        user_email = "user@example.com"  # Replace with actual user email
        remote_folder = "YourRemoteFolder"  # Replace with actual OneDrive folder name

        main(input_file_path, output_dir, 
              headers, user_email, remote_folder   #(modified for local)
             )
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
