import os, json, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 1. 전일(D-1) 날짜 계산
target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

def get_drive_service():
    info = json.loads(os.environ['GDRIVE_CREDENTIALS'])
    creds = service_account.Credentials.from_service_account_info(info)
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(service, file_path, folder_id):
    file_metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}
    media = MediaFileUpload(file_path, mimetype='text/csv')
    
    # 기존 파일이 있으면 업데이트, 없으면 생성
    query = f"name = '{os.path.basename(file_path)}' and '{folder_id}' in parents"
    results = service.files().list(q=query).execute()
    items = results.get('files', [])
    
    if items:
        service.files().update(fileId=items[0]['id'], media_body=media).execute()
    else:
        service.files().create(body=file_metadata, media_body=media).execute()

def run():
    # CSV A~E 생성 (여기에 실제 수집 로직을 확장하세요)
    pd.DataFrame({'status': ['Success'], 'date': [target_date]}).to_csv('CSV_A_ETF.csv', index=False)
    # 일단 테스트용으로 빈 파일들을 생성합니다.
    for name in ['CSV_B_Supply.csv', 'CSV_C_Global.csv', 'CSV_D_Constituents.csv', 'CSV_E_Momentum.csv']:
        pd.DataFrame().to_csv(name)

    # 구글 드라이브 업로드
    service = get_drive_service()
    f_id = os.environ['GDRIVE_FOLDER_ID']
    for f in ['CSV_A_ETF.csv', 'CSV_B_Supply.csv', 'CSV_C_Global.csv', 'CSV_D_Constituents.csv', 'CSV_E_Momentum.csv']:
        upload_to_drive(service, f, f_id)
        print(f"Uploaded {f}")

if __name__ == "__main__":
    run()
