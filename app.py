import os
import io
import requests
from flask import Flask, request, jsonify
from docx import Document
import pdfplumber

app = Flask(__name__)

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("BASE_ID")
TABLE_NAME = os.environ.get("TABLE_NAME")

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

@app.route("/process", methods=["POST"])
def process():
    data = request.json
    record_id = data["record_id"]
    file_url = data["file_url"]

    file_response = requests.get(file_url)
    file_bytes = file_response.content

    if file_url.endswith(".docx"):
        text = extract_docx(file_bytes)
    elif file_url.endswith(".pdf"):
        text = extract_pdf(file_bytes)
    else:
        return jsonify({"error": "Unsupported file type"}), 400

    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_id}"

    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

    requests.patch(url, headers=headers, json={
        "fields": {
            "Raw Extracted Text": text
        }
    })

    return jsonify({"status": "success"})

if __name__ == "__main__":
    app.run()
