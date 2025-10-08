# pdf_classifier.py
import pdfplumber
import fitz

def classify_pdf(file_path, max_pages=3):
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages[:max_pages]:
                tables = page.extract_tables()
                if tables and any(any(row) for table in tables for row in table):
                    return 'table'

        doc = fitz.open(file_path)
        for page in doc[:max_pages]:
            text = page.get_text().strip()
            if len(text) > 100:
                return 'text'

        return 'unknown'

    except Exception as e:
        print(f"[Classifier] Error: {e}")
        return 'unknown'
