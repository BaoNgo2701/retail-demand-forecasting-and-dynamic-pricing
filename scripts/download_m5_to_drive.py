import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
import io
import os

# ================================
# CONFIG
# ================================

# Kaggle credentials (lấy trong kaggle.json)
KAGGLE_USER = os.getenv("KAGGLE_USERNAME", "your_kaggle_username")
KAGGLE_KEY = os.getenv("KAGGLE_KEY", "your_kaggle_api_key")

# Kaggle competition
COMPETITION = "m5-forecasting-accuracy"

# Google Drive API credentials (service account)
SERVICE_ACCOUNT_FILE = "credentials/service_account.json"

# Drive folder ID nơi muốn lưu file
DRIVE_FOLDER_ID = "YOUR_GOOGLE_DRIVE_FOLDER_ID"  

# ================================
# DOWNLOAD FROM KAGGLE
# ================================
def download_from_kaggle(competition: str) -> bytes:
    """Download dataset zip từ Kaggle (dạng bytes)."""
    url = f"https://www.kaggle.com/api/v1/competitions/data/download-all/{competition}"
    print(f"⬇️  Downloading {competition} from Kaggle...")
    resp = requests.get(url, auth=(KAGGLE_USER, KAGGLE_KEY), stream=True)

    if resp.status_code == 200:
        print("✅ Downloaded successfully!")
        return resp.content
    else:
        raise Exception(f"❌ Kaggle API error {resp.status_code}: {resp.text}")


# ================================
# UPLOAD TO GOOGLE DRIVE
# ================================
def upload_to_drive(file_bytes: bytes, filename: str, folder_id: str):
    """Upload dataset trực tiếp lên Google Drive."""
    print("📤 Uploading to Google Drive...")

    # Auth Google Drive
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=scopes
    )
    drive_service = build("drive", "v3", credentials=creds)

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="application/zip", resumable=True)

    uploaded_file = drive_service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    print(f"✅ Uploaded to Drive! File ID: {uploaded_file['id']}")
    return uploaded_file["id"]


# ================================
# MAIN PIPELINE
# ================================
def main():
    try:
        # 1. Download from Kaggle
        data_bytes = download_from_kaggle(COMPETITION)

        # 2. Upload to Drive
        filename = f"{COMPETITION}.zip"
        upload_to_drive(data_bytes, filename, DRIVE_FOLDER_ID)

        print("🎉 Done! Dataset is now in Google Drive.")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
