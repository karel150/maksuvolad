import pandas as pd
import requests
import io
import os
import json
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

URL = "https://ncfailid.emta.ee/s/EXNA4wtJWmX54bp/download/maksuvolglaste_nimekiri.xlsx"
LOGI_FAIL = "maksuvola_ajalugu.csv"

def uplaodi_google_drive(faili_nimi):
    FOLDER_ID = "1LS_EVrXSKKxxK7BE-YnWmo2-72UQmbLO"

    # Fetch OAuth credentials from GitHub Secrets
    creds = Credentials(
        None,  # Muutsime: 'token=None' asemel on nüüd lihtsalt 'None'
        refresh_token=os.environ["GDRIVE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GDRIVE_CLIENT_ID"],
        client_secret=os.environ["GDRIVE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    
    service = build("drive", "v3", credentials=creds)

    # Search for existing file in the folder
    results = service.files().list(
        q=f"name='{faili_nimi}' and '{FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])

    media = MediaFileUpload(faili_nimi, mimetype="text/csv", resumable=True)

    if files:
        service.files().update(
            fileId=files[0]["id"],
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print("Fail uuendatud Google Drive'is.")
    else:
        service.files().create(
            body={"name": faili_nimi, "parents": [FOLDER_ID]},
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print("Fail loodud Google Drive'is.")

def uuenda_statistikat():
    try:
        print(f"Päringu käivitamine: {datetime.now().strftime('%H:%M:%S')}...")
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        df = pd.read_excel(io.BytesIO(response.content))
        df.columns = df.columns.str.strip()
        volglaste_arv = df['Registrikood'].nunique()
        kogu_vola_summa = int(round(df['Maksuvõlg'].sum()))
        uued_andmed = pd.DataFrame([{
            'Kuupäev': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Unikaalseid_võlglasi': volglaste_arv,
            'Kogu_summa_EUR': kogu_vola_summa
        }])
        fail_eksisteerib = os.path.isfile(LOGI_FAIL)
        uued_andmed.to_csv(
            LOGI_FAIL, mode='a', index=False,
            header=not fail_eksisteerib, sep=';', encoding='utf-8-sig'
        )
        print(f"Leitud {volglaste_arv} võlglast, {kogu_vola_summa} €.")
        uplaodi_google_drive(LOGI_FAIL)
    except Exception as e:
        print(f"Viga: {e}")
        raise

if __name__ == "__main__":
    uuenda_statistikat()
