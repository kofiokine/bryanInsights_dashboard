import os
import json
import requests
import pandas as pd
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

with open("service_account.json", "w") as f:
    f.write(os.environ["GDRIVE_CREDENTIALS"])

creds = service_account.Credentials.from_service_account_file("service_account.json")
drive_service = build("drive", "v3", credentials=creds)


API_KEY = os.environ["BRILLIANT_API_KEY"]
HEADERS = {
    "APIKey": API_KEY,
    "Content-Type": "application/json"
}

url = "https://api.brilliantassessments.com/api/assessmentresponse/getassessmentresponses/CDI"
response = requests.get(url, headers=HEADERS)

if response.status_code != 200:
    print(f" Failed to fetch response IDs: {response.status_code} {response.text}")
    exit()

response_ids = response.json().get("ResponseIds", [])
print(f" Found {len(response_ids)} response IDs")

records = []
for rid in response_ids:
    detail_url = f"https://api.brilliantassessments.com/api/assessmentresponse/getassessmentresponse/{rid}"
    res = requests.get(detail_url, headers=HEADERS)

    if res.status_code != 200:
        print(f" Skipped {rid} — {res.status_code}")
        continue

    data = res.json()
    row = {
        "ResponseId": rid,
        "Email": data.get("Email"),
        "First Name": data.get("FirstName"),
        "Last Name": data.get("LastName"),
        "Completion Date": data.get("CompletionDate"),
        "Business Name": data.get("BusinessName"),
        "Status": data.get("Status"),
        "Organizational Performance Rating": data.get("Rating", {}).get("Score")
    }

    for seg in data.get("SegmentationRatings", []):
        name = seg.get("SegmentationName")
        score = seg.get("Score")
        if name:
            row[name] = score

    records.append(row)

df = pd.DataFrame(records)
df.columns = [re.sub(r'[^\w\s\)\]]+$', '', col.strip()) for col in df.columns]
df.to_csv("assessment_data.csv", index=False)
print(" Saved to assessment_data.csv")


FILE_NAME = "assessment_data.csv"
FOLDER_ID = "1zQ_jetfKyZlfXGE2pbzVUqLzpotfPenr"

# Search for existing file with the same name in the folder
query = f"'{FOLDER_ID}' in parents and name='{FILE_NAME}' and trashed=false"
response = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
files = response.get("files", [])

media = MediaFileUpload(FILE_NAME, mimetype="text/csv")

if files:
    # File exists – overwrite it
    file_id = files[0]["id"]
    drive_service.files().update(fileId=file_id, media_body=media).execute()
    print(f" Overwrote existing file with ID: {file_id}")
else:
    # File does not exist – create it
    file_metadata = {
        "name": FILE_NAME,
        "parents": [FOLDER_ID]
    }
    file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f" Uploaded new file with ID: {file.get('id')}")
