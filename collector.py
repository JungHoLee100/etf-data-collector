import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def run():
    print("🚀 [최적화 엔진] 초고속 수집 모드 가동...")
    now = datetime.now()
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    start_date, end_date = b_days[0], b_days[-1]

    # 1. 이름표 미리 다 받아오기 (Batch Name Fetch) - 속도의 핵심!
    print("🏷️ 종목 이름표 일괄 확보 중...")
    try:
        # 전종목 이름/업종 정보를 한 번에 가져와서 지도로 만듭니다.
        df_names = stock.get_market_sector_by_ticker(end_date, market="ALL")
        name_map = df_names['종목명'].to_dict()
        
        # ETF 이름표 확보
        etf_tickers = stock.get_etf_ticker_list(end_date)
        etf_name_map = {t: stock.get_etf_ticker_name(t) for t in etf_tickers}
    except:
        name_map, etf_name_map = {}, {}

    # 데이터 저장 그릇
    data_a, data_d = {}, {}
    adr_results = {'metric': 'Market_ADR'}

    # 2. 메인 루프: 날짜별 일괄 수집 (D, E, A 처리)
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...")

        # [A] ETF
        df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
        for t, row in df_etf.iterrows():
            if t not in data_a: data_a[t] = {'ticker': t, 'name': etf_name_map.get(t, t)}
            data_a[t][f"{date_key}_P"] = row['종가']

        # [D & E] 주식 전종목 & ADR
        df_p = stock.get_market_ohlcv_by_ticker(d_str, market="ALL")
        df_v = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="ALL")
        
        # E: ADR 산출
        ups = len(df_p[df_p['대비'] > 0])
        downs = len(df_p[df_p['대비'] < 0])
        adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

        # D: 시계열 매핑 (이름표 조회는 메모리에서 수행하여 초고속)
        for t, row in df_p.iterrows():
            if t not in data_d: data_d[t] = {'ticker': t, 'name': name_map.get(t, t)}
            data_d[t][f"{date_key}_P"] = row['종가']
            if t in df_v.index:
                vol = row['거래량']
                if vol > 0:
                    data_d[t][f"{date_key}_For%"] = round(df_v.loc[t, '외국인']/vol*100, 2)

    # 3. [B] 파생상품 (별도 정밀 타격)
    print("📂 CSV_B 수집 중...")
    list_b = []
    for code, name in {"101SC": "Futures", "201SC": "Call", "301SC": "Put"}.items():
        try:
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, code)
            row = {'ticker': code, 'name': name}
            for date, p_data in df_p.iterrows():
                d_str = date.strftime('%Y-%m-%d')
                row[f"{d_str}_P"] = p_data['종가']
                if date in df_v.index: row[f"{d_str}_ForNet"] = df_v.loc[date, '외국인']
            list_b.append(row)
        except: pass

    # 4. 파일 저장 (최종)
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
    
    # 5. [C] 글로벌
    yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close'].T.to_csv('CSV_C.csv')
    
    print("🏁 모든 파일이 완벽하게 생성되었습니다!")

if __name__ == "__main__": run()
