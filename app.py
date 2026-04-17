import os
import time
import requests
from io import BytesIO
from pdfminer.high_level import extract_text

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
BASE_ID = os.environ.get("BASE_ID")
TABLE_NAME = os.environ.get("TABLE_NAME")

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}


def process_records():
    try:
        print("Checking Airtable for new records...")

        formula = "AND({Upload Application}, {Raw Extracted Text} = '')"

        url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
        params = {
            "filterByFormula": formula
        }

        response = requests.get(url, headers=HEADERS, params=params)

        print("GET status:", response.status_code)
        print("GET response:", response.text)

        if response.status_code != 200:
            print("Failed to fetch records")
            return

        records = response.json().get("records", [])

        print(f"Found {len(records)} matching records")

        for record in records:
            record_id = record["id"]
            fields = record.get("fields", {})

            attachments = fields.get("Upload Application")

            if not attachments:
                print(f"No attachment found for {record_id}")
                continue

            file_url = attachments[0]["url"]

            print(f"Downloading PDF for record {record_id}")

            pdf_response = requests.get(file_url)

            if pdf_response.status_code != 200:
                print(f"Failed to download PDF for {record_id}")
                continue

            pdf_file = BytesIO(pdf_response.content)

            try:
                text = extract_text(pdf_file)
            except Exception as e:
                print(f"PDF extraction failed for {record_id}: {e}")
                continue

            update_url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}/{record_id}"

            update_response = requests.patch(
                update_url,
                headers=HEADERS,
                json={
                    "fields": {
                        "Raw Extracted Text": text
                    }
                }
            )

            print(f"PATCH status for {record_id}:", update_response.status_code)
            print(f"PATCH response for {record_id}:", update_response.text)

            if update_response.status_code == 200:
                print(f"✅ Successfully processed {record_id}")
            else:
                print(f"❌ Failed to update {record_id}")

    except Exception as e:
        print("FATAL ERROR:", str(e))


if __name__ == "__main__":
    print("Worker started...")
    while True:
        process_records()
        time.sleep(60)
