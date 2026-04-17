import pandas as pd
import requests
import io
import os
import json
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

URL = "https://ncfailid.emta.ee/s/EXNA4wtJWmX54bp/download/maksuvolglaste_nimekiri.xlsx"
LOGI_FAIL = "maksuvola_ajalugu.csv"

def uplaodi_google_drive(faili_nimi):
    FOLDER_ID = "1LS_EVrXSKKxxK7BE-YnWmo2-72UQmbLO"

    # OAuth sisselogimine GitHub Secrets abil
    creds = Credentials(
        None,
        refresh_token=os.environ["GDRIVE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GDRIVE_CLIENT_ID"],
        client_secret=os.environ["GDRIVE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    
    service = build("drive", "v3", credentials=creds)

    # Otsime, kas fail on juba kaustas olemas
    results = service.files().list(
        q=f"name='{faili_nimi}' and '{FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])

    if files:
        file_id = files[0]["id"]
        
        # 1. Laeme olemasoleva sisu Drive'ist alla
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        old_content = fh.getvalue().decode('utf-8')

        # 2. Loeme kohaliku faili read ja jätame päise vahele
        with open(faili_nimi, "r", encoding="utf-8") as f:
            lines = f.readlines()
            # Võtame andmed alates teisest reast (index 1), et vältida päise duleerimist
            new_data_rows = "".join(lines[1:]) if len(lines) > 1 else ""

        # 3. Paneme vana sisu ja uued andmeread kokku
        if old_content and not old_content.endswith('\n'):
            old_content += '\n'
        
        combined_content = old_content + new_data_rows

        # Kirjutame liidetud sisu ajutiselt kohalikku faili tagasi üleslaadimiseks
        with open(faili_nimi, "w", encoding="utf-8") as f:
            f.write(combined_content)

        # 4. Uuendame faili Drive'is
        media = MediaFileUpload(faili_nimi, mimetype="text/csv", resumable=True)
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print(f"Faili sisu täiendatud (päist ei korratud).")

    else:
        # Kui faili veel pole, loome uue faili koos päisega
        media = MediaFileUpload(faili_nimi, mimetype="text/csv", resumable=True)
        service.files().create(
            body={"name": faili_nimi, "parents": [FOLDER_ID]},
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print(f"Uus fail loodud Google Drive'is (koos päisega).")

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
