import fitz  # PyMuPDF
import pandas as pd
import re
import os
import glob

def extract_pdf_folder(folder_path, output_folder=None, output_format='ods'):
   
    ext_map = {
        'ods': 'ods',
        'xlsx': 'xlsx',
        'txt': 'txt'
    }

    pdf_files = glob.glob(os.path.join(folder_path, '*.pdf'))
    if not pdf_files:
        raise FileNotFoundError(f" No PDF files found in folder: {folder_path}")

    # Set default output folder if not provided
    if output_folder is None:
        output_folder = folder_path
    os.makedirs(output_folder, exist_ok=True)

    footer_patterns = [
        r"©.*TransUnion CIBIL.*", r"Formerly: Credit Information Bureau.*",
        r"all rights reserved\.?", r"CIN\s*:\s*[A-Z0-9\-]+",
        r"MEMBER\s+ID\s*:\s*.*", r"CONTROL\s+NUMBER\s*:\s*.*",
        r"DATE\s*:\s*\d{2}-\d{2}-\d{4}", r"PAGE\s*\d+\s*OF\s*\d+.*"
    ]
    compiled_footers = [re.compile(p, re.IGNORECASE) for p in footer_patterns]
    footer_fragments = ["TransUnion CIBIL"]

    def clean_line(line: str) -> str:
        for pat in compiled_footers:
            line = pat.sub('', line)
        for frag in footer_fragments:
            line = line.replace(frag, '')
        return re.sub(r'\s{2,}', ' ', line).strip()

    pan_regex = re.compile(r'\b([A-Z]{5}[0-9]{4}[A-Z])\b')
    name_keywords = ['CONSUMER NAME', 'NAME']
    ordered_fields = ['Type', 'Ownership', 'Sanctioned', 'Current Balance', 'DPD']
    dpd_header = "DAYS PAST DUE/ASSET CLASSIFICATION (UP TO 36 MONTHS; LEFT TO RIGHT)"

    extracted_files = []

    for pdf_path in pdf_files:
        print(f"\nProcessing file: {pdf_path}")
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_name = f"{base_name}_extract.{ext_map.get(output_format, 'ods')}"
        output_path = os.path.join(output_folder, output_name)

        if not os.path.exists(pdf_path):
            print(f"File not found: {pdf_path}")
            continue

        rows = []

        with fitz.open(pdf_path) as doc:
            all_lines = []
            for pno, page in enumerate(doc, 1):
                text = page.get_text("text") or ""
                for ln in text.split('\n'):
                    cl = clean_line(ln.strip())
                    if cl:
                        all_lines.append((pno, cl))

        pages_seen = {p for p, _ in all_lines}

        pan = score = name = None
        for i in range(len(all_lines)):
            pno, line = all_lines[i]
            upper = line.upper()

            if not pan and (m := pan_regex.search(line)):
                pan = m.group(1)
            if not score and 'SCORE' in upper:
                if m2 := re.search(r'\d{3}', line):
                    score = m2.group()
                elif i + 1 < len(all_lines) and re.match(r'^\d{3}$', all_lines[i + 1][1]):
                    score = all_lines[i + 1][1]
            if not name:
                for nk in name_keywords:
                    if nk in upper:
                        parts = line.split(':', maxsplit=1)
                        name = parts[1].strip() if len(parts) > 1 else (all_lines[i + 1][1] if i + 1 < len(all_lines) else "")
                        break

        accounts = []
        current_account = {}
        current_page = None
        last_field_page = None
        gap_count = 0

        for i in range(len(all_lines)):
            pno, line = all_lines[i]
            upper = line.upper()

            field = None
            value = None

            if upper.startswith("TYPE:"):
                field = "Type"
                parts = line.split(":", 1)
                value = parts[1].strip() if len(parts) > 1 else ""
                if i + 1 < len(all_lines):
                    next_line = all_lines[i + 1][1].strip()
                    if next_line and not any(next_line.upper().startswith(x.upper()) for x in ordered_fields):
                        if value:
                            value = f"{value} {next_line}".strip()
                        else:
                            value = next_line

            elif upper.startswith("OWNERSHIP:"):
                field = "Ownership"
                value = line.split(":", 1)[1].strip()

            elif upper.startswith("SANCTIONED:"):
                field = "Sanctioned"
                value = line.split(":", 1)[1].strip()

            elif upper.startswith("HIGH CREDIT:"):
                field = "Sanctioned"
                if "Sanctioned" not in current_account or not current_account["Sanctioned"]:
                    value = line.split(":", 1)[1].strip()
                else:
                    continue

            elif upper.startswith("CURRENT BALANCE:"):
                field = "Current Balance"
                value = line.split(":", 1)[1].strip()

            elif dpd_header in upper:
                field = "DPD"
                value = ""
                for j in range(1, 6):
                    if i + j >= len(all_lines):
                        break
                    next_line = all_lines[i + j][1].strip().upper()
                    if any(next_line.startswith(x.upper()) for x in ordered_fields):
                        break
                    if any(next_line.startswith(bad) for bad in ["CONSUMER CIR", "DATE:", "PAGE", "CONTROL NUMBER", "MEMBER ID"]):
                        continue
                    if re.fullmatch(r'[A-Z0-9\s]+', next_line):
                        value = next_line
                        break
                if not value:
                    value = line.strip()

            if field:
                if not current_account:
                    current_page = pno
                current_account[field] = value
                last_field_page = pno
                gap_count = 0
            elif current_account:
                if pno > (last_field_page or current_page):
                    gap_count += pno - (last_field_page or current_page)
                    last_field_page = pno
                if gap_count > 2 or len(current_account) == len(ordered_fields):
                    accounts.append((current_page, current_account.copy()))
                    current_account = {}
                    gap_count = 0

        if current_account:
            accounts.append((current_page, current_account.copy()))

        for page, field_map in accounts:
            if all(field_map.get(fld, 'No Data') == 'No Data' for fld in ordered_fields):
                continue
            meta = {"Page": page, "PAN": pan, "Name": name, "Score": score}
            for fld in ordered_fields:
                val = field_map.get(fld, 'No Data')
                rows.append({**meta, "Field": fld, "Value": val})

        used_pages = {r['Page'] for r in rows}
        for p in sorted(pages_seen - used_pages):
            rows.append({"Page": p, "PAN": pan, "Name": name, "Score": score, "Field": "No Data", "Value": "No Data"})

        df = pd.DataFrame(rows, columns=["Page", "PAN", "Name", "Score", "Field", "Value"])
        #df['Value'].replace('', 'No Data', inplace=True)
        df.replace({'Value': {'': 'No Data'}}, inplace=True)
        #df.method({col: value}, inplace=True)

        

        try:
            if output_format == 'xlsx':
                df.to_excel(output_path, index=False)
            elif output_format == 'ods':
                df.to_excel(output_path, engine='odf', index=False)
            elif output_format == 'txt':
                df.to_csv(output_path, sep='\t', index=False)
            else:
                raise ValueError(f" Unsupported format: {output_format}")

            print(f"Saved extracted data to {output_path}")
            extracted_files.append(output_path)
        except Exception as e:
            print(f" Failed to save {output_path}: {e}")

   # print("\nAll files processed.")
    return extracted_files
