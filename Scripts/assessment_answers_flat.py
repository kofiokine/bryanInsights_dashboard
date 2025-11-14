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
        "Status": data.get("Status")
    }

    for ans in data.get("Answers", []):
        question = ans.get("QuestionText", "")
        answer = ans.get("AnswerText", "")
        if question:
            row[question.strip()] = answer.strip() if answer else ""

    records.append(row)


df = pd.DataFrame(records)
df.columns = [re.sub(r'[^\w\s\)\]]+$', '', col.strip()) for col in df.columns]
csv_filename = "assessment_answers_flat.csv"
df.to_csv(csv_filename, index=False)
print(f" Saved to {csv_filename}")


FOLDER_ID = "1zQ_jetfKyZlfXGE2pbzVUqLzpotfPenr"


results = drive_service.files().list(
    q=f"name='{csv_filename}' and '{FOLDER_ID}' in parents and trashed=false",
    spaces="drive",
    fields="files(id, name)"
).execute()
items = results.get("files", [])

if items:
    file_id = items[0]["id"]
    print(f" File exists. Overwriting ID: {file_id}")
    media = MediaFileUpload(csv_filename, mimetype="text/csv")
    updated_file = drive_service.files().update(
        fileId=file_id,
        media_body=media
    ).execute()
    print(f" File updated: {updated_file.get('id')}")
else:
    print(" File not found. Creating new.")
    file_metadata = {
        "name": csv_filename,
        "parents": [FOLDER_ID]
    }
    media = MediaFileUpload(csv_filename, mimetype="text/csv")
    new_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    print(f" File created: {new_file.get('id')}")
