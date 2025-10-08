import os
import re
import csv
import requests
import pandas as pd
from urllib.parse import quote
#from main import get_auth_headers, USER_ID  


def get_user_drive_id(headers, user_email):
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('id')

def upload_file_to_onedrive(headers, user_email, local_file_path, remote_folder): 
    drive_id = get_user_drive_id(headers, user_email)
    filename = os.path.basename(local_file_path)
    remote_path_encoded = quote(f"{remote_folder}/{filename}")
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{remote_path_encoded}:/content"

    with open(local_file_path, 'rb') as f:
        resp = requests.put(url, headers=headers, data=f)
        resp.raise_for_status()
    print(f"Uploaded {filename} to OneDrive folder '{remote_folder}'")
    #print("\n")
def process_local_files(headers=None, user_email=None, local_input_dir=None, local_export_dir=None, onedrive_export_folder=None,only_file=None):
#def process_local_files(local_input_dir, local_export_dir,only_file=None):  # (modified for local)
    files =[only_file] if only_file else [f for f in os.listdir(local_input_dir) if f.lower().endswith('.csv')]
    if not files:
        print(f"No CSV files found in {local_input_dir}")
        return

    for filename in files:
        source_file = os.path.join(local_input_dir, filename)
        try:
            print(f"Processing file: {source_file}")
            extracted_data, max_len = extract_data_from_csv(source_file)

            base_name = os.path.splitext(filename)[0]
            output_file = os.path.join(local_export_dir, f"{base_name}.xlsx")

            if not os.path.exists(output_file):
                df = pd.DataFrame(columns=FIELD_MAPPING.values())
                df.to_excel(output_file, index=False, engine='openpyxl')

            append_data_to_ods(extracted_data, max_len, output_file)


            if headers and user_email and onedrive_export_folder: 
                upload_file_to_onedrive(headers, user_email, output_file, onedrive_export_folder) # for local path
            else:
                print(f"Processed file saved locally: {output_file}")    

        except Exception as e:
            print(f"Error processing {source_file}: {e}")

# Mapping keys between source and destination files
FIELD_MAPPING = {
    'Name': 'Entity Name/ Director Name',
    'PAN:': 'PAN Number',
    'Rank': 'CMR Rank/Credit Score',
    'Type': 'Facility type',
    'Credit Facility/Page': 'Facility No./ Page No.',
    'Asset Classification / DPD': 'DPDs',
    'DPD period': 'DPD period',
    'Credit Facility Details': 'Guarantor/Borrower/Individual/Joint',
    'Sanctioned Limit': 'Sanction limit',  # Included all countries currency
    'Outstanding Balance': 'O/s Amount',
    'Overdue': 'Overdue',
    'Settled': 'Settled/Written Off / any other instance'
}



# Extraction dictionary for Written Off and Settled values
def extract_data_from_csv(source_file):
    extracted_data = {key: [] for key in FIELD_MAPPING.keys()}
    extracted_data['Written Off'] = []
    extracted_data['Settled'] = []
    lines = []
    rows = []
    
    # Read source CSV into lines and rows
    with open(source_file, 'r', encoding='utf-8-sig', errors='ignore') as f:  
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
            for val in row:
                if val:
                    lines.append(val.strip())
    # Extract Rank value separately
    for row in rows:
        for i, cell in enumerate(row):
            if re.match(r'^rank[:]?$', cell.strip(), re.IGNORECASE):
                if i + 1 < len(row):
                    extracted_data['Rank'].append(row[i + 1].strip())
                else:
                    extracted_data['Rank'].append('')
    # Extract Name and PAN once from the top
    for line in lines[:10]:
        name_match = re.search(r'Name\s*[:\-]?\s*(.+)', line, re.IGNORECASE)
        if name_match:
            extracted_data['Name'].append(name_match.group(1).strip())
        pan_match = re.search(r'PAN\s*[:\-]?\s*([A-Z]{5}\d{4}[A-Z])', line, re.IGNORECASE)
        if pan_match:
            extracted_data['PAN:'].append(pan_match.group(1).strip())

    inside_borrower_profile = False
    # Extract other fields
    for val in lines:
        if 'Borrower Profile' in val or 'As Borrower' in val:
            inside_borrower_profile = True
        elif re.search(r'^Credit Facility\s*\d+', val, re.IGNORECASE):
            inside_borrower_profile = False

        for key in FIELD_MAPPING.keys():
            if key in ['Rank', 'DPD period', 'Name', 'PAN:', 'Sanctioned Limit']:  # Sanctioned Limit handled separately below as this include different currencies
                continue
            if key == 'Type' and inside_borrower_profile:
                continue

            pattern = re.compile(rf"{re.escape(key)}\s*[:\-/_]?\s*(.+)", re.IGNORECASE)
            match = pattern.search(val)
            if match:
                value = match.group(1).strip()
                if key == 'Asset Classification / DPD':
                    text_part = ' '.join(re.findall(r'[A-Za-z]+', value)).strip()
                    num_part = ' '.join(re.findall(r'\d+', value)).strip()
                    extracted_data[key].append(text_part)
                    extracted_data.setdefault('DPD period numeric', []).append(num_part if num_part else '')
                    continue
                extracted_data[key].append(value)

    # Extract Written Off and Settled values
    written_off_temp = []
    settled_temp = []
    for val in lines:
        written_off_match = re.search(r'Written Off\s*[:\-]?\s*([-\d,\.]+)', val, re.IGNORECASE)
        settled_match = re.search(r'Settled\s*[:\-]?\s*([-\d,\.]+)', val, re.IGNORECASE)
        wo_val = written_off_match.group(1).strip() if written_off_match else ''
        set_val = settled_match.group(1).strip() if settled_match else ''
        if wo_val or set_val:  # Append only when either value is found to keep row alignment
            written_off_temp.append(wo_val)
            settled_temp.append(set_val)
    
    # Pad Written Off and Settled lists to same length
    max_wo_set_len = max(len(written_off_temp), len(settled_temp))
    while len(written_off_temp) < max_wo_set_len:
        written_off_temp.append('')
    while len(settled_temp) < max_wo_set_len:
        settled_temp.append('')

    extracted_data['Written Off'] = written_off_temp
    extracted_data['Settled'] = settled_temp

    extracted_data['Credit Facility/Page'] = []  # Extract Credit Facility + Page
    for i in range(len(lines) - 2):
        line = lines[i]
        next1 = lines[i + 1]
        next2 = lines[i + 2]

        page_match = re.search(r'Page\s*(\d+)', line, re.IGNORECASE)
        if page_match:
            page_no = page_match.group(1)
            # Check for Credit Facility number
            cf_match = re.search(r'Credit Facility\s*(\d+)', next1, re.IGNORECASE) or \
                       re.search(r'Credit Facility\s*(\d+)', next2, re.IGNORECASE)
            # Check for Credit Facility Guaranteed number
            cf_g_match = re.search(r'Credit Facility Guaranteed\s*(\d+)', next1, re.IGNORECASE) or \
                         re.search(r'Credit Facility Guaranteed\s*(\d+)', next2, re.IGNORECASE)
            if cf_match:
                extracted_data['Credit Facility/Page'].append(f"{cf_match.group(1)}/{page_no}")
            elif cf_g_match:
                extracted_data['Credit Facility/Page'].append(f"{cf_g_match.group(1)}/{page_no}")
    
    # Extract all sanctioned amounts with currencies dynamically
    sanctioned_amounts = []
    currency_pattern = re.compile(r'Sanctioned\s+([A-Z]{3})\s*[:\-]?\s*([\d,\.]+)', re.IGNORECASE)
    for val in lines:
        matches = currency_pattern.findall(val)
        if matches:
            parts = []
            for currency, amount in matches:
                parts.append(f"{currency.upper()} {amount.strip()}")
            combined = " / ".join(parts)
            sanctioned_amounts.append(combined)
    
    # Determine max length based on major extracted lists
    max_len = max(
        len(extracted_data.get('Type', [])),
        len(extracted_data.get('Credit Facility Details', [])),
        len(extracted_data.get('Written Off', [])),
        len(extracted_data.get('Settled', [])),
        len(sanctioned_amounts) if sanctioned_amounts else 0
    )

    for key in extracted_data:
        while len(extracted_data[key]) < max_len:
            extracted_data[key].append('')
    # Define Name, PAN, Rank to max_len
    for key in ['Name', 'PAN:', 'Rank']:
        values = extracted_data.get(key, [])
        if values:
            first_val = next((v for v in values if v), '')
            extracted_data[key] = [first_val] * max_len
        else:
            extracted_data[key] = [''] * max_len

    if sanctioned_amounts:
        while len(sanctioned_amounts) < max_len:
            sanctioned_amounts.append('')
        extracted_data['Sanctioned Limit'] = sanctioned_amounts
    else:
        extracted_data['Sanctioned Limit'] = [''] * max_len

    return extracted_data, max_len
# Spreadsheet loader
def read_spreadsheet(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        return pd.read_csv(file_path)
    elif ext == '.xlsx':
        return pd.read_excel(file_path, engine='openpyxl')
    elif ext == '.ods':
        return pd.read_excel(file_path, engine='odf')
    else:
        raise ValueError("Unsupported file format: " + ext)

def normalize_val(v):
    if v == '-' or v == '':
        return v
    return v

def append_data_to_ods(extracted_data, max_len, destination_file):
    df = read_spreadsheet(destination_file)
    df.columns = [str(col).strip().lstrip('\ufeff') for col in df.columns]
    # Build output rows combining Written Off and Settled into one field
    append_rows = []
    for i in range(max_len):
        row = {col: '' for col in df.columns}
        for src_key, dest_col in FIELD_MAPPING.items():
            if dest_col not in df.columns:
                continue
            if dest_col == 'DPD period':
                dpd_numeric_list = extracted_data.get('DPD period numeric', [])
                row[dest_col] = dpd_numeric_list[i] if i < len(dpd_numeric_list) else ''
            elif dest_col == 'Settled/Written Off / any other instance':
                written_off_val = extracted_data.get('Written Off', [''])[i].strip()
                settled_val = extracted_data.get('Settled', [''])[i].strip()

                wo_val = normalize_val(written_off_val)
                set_val = normalize_val(settled_val)

                if wo_val and set_val:
                    combined = f"{set_val} / {wo_val}"
                else:
                    combined = set_val or wo_val or ''
                row[dest_col] = combined
            else:
                row[dest_col] = extracted_data[src_key][i]
        append_rows.append(row)
    # Create DataFrame and reorder columns to match destination
    append_df = pd.DataFrame(append_rows)
    append_df = append_df[df.columns]
    
    # Save based on file extension
    ext = os.path.splitext(destination_file)[1].lower()
    if ext == '.csv':
        append_df.to_csv(destination_file, index=False)
    elif ext == '.xlsx':
        append_df.to_excel(destination_file, index=False, engine='openpyxl')
    elif ext == '.ods':
        append_df.to_excel(destination_file, index=False, engine='odf')
    else:
        raise ValueError("Unsupported export file format: " + ext)

    print(f"Appended {max_len} rows to {destination_file}")

