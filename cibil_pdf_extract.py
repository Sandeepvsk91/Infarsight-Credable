import os
import time
import requests
import pdfplumber
import csv
from urllib.parse import quote

# Keywords and extraction logic
keywords_to_capture = {
    "Credit Facility Details": 15,
    "Borrower Profile": 15,
    "TransUnion CIBIL Rank": 15
}
global_keywords = {"Borrower Profile", "TransUnion CIBIL Rank"}

def clean_cell(cell):
    return cell.strip() if cell else ''

#Extracting data from pdf tables to csv format
def extract_pdf_tables(pdf_path, csv_output_path):
    keywords_captured = set()
    with pdfplumber.open(pdf_path) as pdf, open(csv_output_path, 'a', newline='', encoding='utf-8') as f_csv:
        csv_writer = csv.writer(f_csv)
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if not tables:
                continue
            for table_idx, table in enumerate(tables, start=1):
                capturing = False
                rows_captured = 0
                current_keyword = None
                i = 0
                while i < len(table):
                    row = table[i]
                    if not any(row):
                        i += 1
                        continue
                    row_cleaned = [clean_cell(cell) for cell in row]
                    row_joined = ' '.join(row_cleaned).strip().lower()
                    if not capturing:
                        for keyword, count in keywords_to_capture.items():
                            if keyword.lower() in row_joined:
                                if keyword in global_keywords and keyword in keywords_captured:
                                    continue
                                capturing = True
                                rows_captured = 0
                                current_keyword = keyword
                                if keyword in global_keywords:
                                    keywords_captured.add(keyword)
                                csv_writer.writerow([f'Page {page_num} - Table {table_idx} - Keyword: {keyword}'])
                                csv_writer.writerow(row_cleaned)
                                break
                        i += 1
                        continue
                    if capturing:
                        if 'asset classification / dpd' in row_joined and i + 1 < len(table):
                            next_row = table[i + 1]
                            next_row_cleaned = [clean_cell(cell) for cell in next_row]
                            merged_row = [row_cleaned[0] + ' ' + next_row_cleaned[0]] + row_cleaned[1:]
                            csv_writer.writerow(merged_row)
                            rows_captured += 1
                            i += 2
                            continue
                        if current_keyword == "TransUnion CIBIL Rank" and any(cell.lower() == 'rank' for cell in row_cleaned):
                            i += 1
                            if i < len(table):
                                next_row = [clean_cell(cell) for cell in table[i]]
                                rank_value = next_row[1] if len(next_row) > 1 else 'NA'
                                csv_writer.writerow(['Rank', rank_value])
                                rows_captured += 1
                                i += 1
                            continue
                        if rows_captured < keywords_to_capture[current_keyword]:
                            csv_writer.writerow(row_cleaned)
                            rows_captured += 1
                            i += 1
                        else:
                            capturing = False
                            current_keyword = None
                            csv_writer.writerow([])
                            i += 1
    print(f"Extracted: {os.path.basename(pdf_path)} → {os.path.basename(csv_output_path)}")
