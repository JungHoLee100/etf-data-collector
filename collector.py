import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
yf.pdr_override() # 필요한 경우
import requests_cache
# 캐시를 사용하지 않도록 설정
session = None
import time

def run():
    print("🚀 [거시 지표 전용] CSV A, C, E 수집 엔진 가동...")
    now = datetime.now()
    # 최근 30거래일 영업일 확보
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    
    # 1. 이름표 사전 확보 (CSV_A용)
    print("🏷️ ETF 이름표 확보 중...")
    try:
        etf_tickers = stock.get_etf_ticker_list(b_days[-1])
        etf_name_map = {t: stock.get_etf_ticker_name(t) for t in etf_tickers}
    except:
        etf_name_map = {}

    # 데이터 저장 그릇
    data_a = {}
    adr_results = {'metric': 'Market_ADR'}

    # 2. 메인 루프 (날짜별 일괄 수집)
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...", end="\r")

        # [CSV_A] ETF 시세 및 거래량
        try:
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a: data_a[t] = {'ticker': t, 'name': etf_name_map.get(t, t)}
                data_a[t][f"{date_key}_P"] = row['종가']
                data_a[t][f"{date_key}_V"] = row['거래량']
        except: pass

        # [CSV_E] ADR (코스피 상승/하락 비율)
        try:
            df_p = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            # 종가 > 시가인 종목이 상승 종목
            ups = len(df_p[df_p['종가'] > df_p['시가']])
            downs = len(df_p[df_p['종가'] < df_p['시가']])
            adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100
        except: pass
        
        time.sleep(0.2) # 서버 보호

    # 3. 저장 및 글로벌 지수 수집
    print("\n💾 파일 저장 중...")
    
    # CSV_A 저장
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    
    # CSV_E 저장
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
    
    # CSV_C 저장 (글로벌 지수)
    try:
        df_c = yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close']
        df_c.T.to_csv('CSV_C.csv', encoding='utf-8-sig')
        print("✅ CSV_C 완료")
    except:
        print("❌ CSV_C 실패")

    print(f"🏁 모든 거시 데이터 세트(A, C, E)가 준비되었습니다.")

if __name__ == "__main__":
    run()
