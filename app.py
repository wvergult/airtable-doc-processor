import os
import io
import time
import requests
from docx import Document
import pdfplumber

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("BASE_ID")
TABLE_NAME = os.environ.get("TABLE_NAME")

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

def extract_docx(file_bytes):
    document = Document(io.BytesIO(file_bytes))
    return "\n".join([para.text for para in document.paragraphs])

def extract_pdf(file_bytes):
    text = ""
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
    return text

def process_records():
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"

    formula = "AND({Application File} != '', {Raw Extracted Text} = '')"

    response = requests.get(url, headers=HEADERS, params={"filterByFormula": formula})
    records = response.json().get("records", [])

    for record in records:
        record_id = record["id"]
        file_url = record["fields"]["Application File"][0]["url"]

        file_response = requests.get(file_url)
        file_bytes = file_response.content

        if file_url.endswith(".docx"):
            text = extract_docx(file_bytes)
        elif file_url.endswith(".pdf"):
            text = extract_pdf(file_bytes)
        else:
            continue

        update_url = f"{url}/{record_id}"

        requests.patch(update_url, headers=HEADERS, json={
            "fields": {
                "Raw Extracted Text": text
            }
        })

        print(f"Processed record {record_id}")

if __name__ == "__main__":
    while True:
        process_records()
        time.sleep(60)
