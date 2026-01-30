import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock

# 1. 날짜 설정 (최근 영업일 데이터 수집을 위해 D-1)
target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

def run():
    print(f"[{target_date}] 데이터 수집 시작...")
    
    # 예시: CSV A (ETF 마스터) 생성
    try:
        tickers = stock.get_etf_ticker_list(target_date)
        data_a = []
        for t in tickers:
            name = stock.get_etf_ticker_name(t)
            data_a.append({'ticker': t, 'name': name})
        
        df_a = pd.DataFrame(data_a)
        # 파일을 현재 폴더에 저장 (GitHub이 이를 감지해서 업데이트함)
        df_a.to_csv('CSV_A_ETF.csv', index=False)
        print("CSV_A_ETF.csv 생성 완료")
        
    except Exception as e:
        print(f"데이터 수집 중 오류 발생: {e}")

    # 테스트용 나머지 파일 생성
    for name in ['CSV_B_Supply.csv', 'CSV_C_Global.csv', 'CSV_D_Constituents.csv', 'CSV_E_Momentum.csv']:
        pd.DataFrame({'info': ['Coming soon'], 'date': [target_date]}).to_csv(name, index=False)

if __name__ == "__main__":
    run()
