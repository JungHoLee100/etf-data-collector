import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def run():
    print("🚀 [통합 엔진] 모든 데이터 일괄 수집 모드 가동...")
    
    # 1. 영업일 확인 (최근 30거래일)
    now = datetime.now()
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    print(f"📅 대상 기간: {b_days[0]} ~ {b_days[-1]} ({len(b_days)}일)")

    # 데이터 저장을 위한 그릇들
    data_a, data_b, data_d = {}, {}, {}
    adr_results = {'metric': 'Market_ADR'}
    
    # --- 메인 루프: 날짜별로 한 번에 수집 ---
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 패킹 중...")

        # [CSV A] ETF 일괄 수집
        df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
        for t, row in df_etf.iterrows():
            if t not in data_a: data_a[t] = {'ticker': t, 'name': stock.get_etf_ticker_name(t)}
            data_a[t][f"{date_key}_P"] = row['종가']
            data_a[t][f"{date_key}_V"] = row['거래량']

        # [CSV B] 파生的(선물/옵션) 수급 일괄 수집
        # KOSPI200 선물(101SC), 콜(201SC), 풋(301SC)
        df_deriv = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, "ALL")
        for t in ["101SC", "201SC", "301SC"]:
            name = "Futures" if "101" in t else "Call" if "201" in t else "Put"
            if t not in data_b: data_b[t] = {'ticker': t, 'name': name}
            if t in df_deriv.index:
                data_b[t][f"{date_key}_ForNet"] = df_deriv.loc[t, '외국인']
                data_b[t][f"{date_key}_InstNet"] = df_deriv.loc[t, '기관합계']

        # [CSV D & E] 전 종목 시세/수급 및 ADR 계산
        df_stock_p = stock.get_market_ohlcv_by_ticker(d_str, market="ALL")
        df_stock_v = df_deriv # 위에서 받은 수급 데이터 재활용
        
        # E: ADR 계산
        # $$ADR = \frac{\text{상승 종목 수}}{\text{하락 종목 수}} \times 100$$
        ups = len(df_stock_p[df_stock_p['대비'] > 0])
        downs = len(df_stock_p[df_stock_p['대비'] < 0])
        adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

        # D: 전 종목 데이터 매핑
        for t, row in df_stock_p.iterrows():
            if t not in data_d: data_d[t] = {'ticker': t, 'name': stock.get_market_ticker_name(t)}
            data_d[t][f"{date_key}_P"] = row['종가']
            data_d[t][f"{date_key}_V"] = row['거래량']
            if t in df_stock_v.index:
                vol = row['거래량']
                if vol > 0:
                    data_d[t][f"{date_key}_For%"] = round(df_stock_v.loc[t, '외국인']/vol*100, 2)
                    data_d[t][f"{date_key}_Inst%"] = round(df_stock_v.loc[t, '기관합계']/vol*100, 2)
        
        time.sleep(0.5) # 서버 보호를 위한 휴식

    # --- 파일 저장 (Overwrite) ---
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_b.values())).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')

    # --- CSV C (Global) ---
    yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close'].T.to_csv('CSV_C.csv')
    
    print("🏁 모든 파일(A, B, C, D, E)이 일괄 업데이트되었습니다.")

if __name__ == "__main__": run()
