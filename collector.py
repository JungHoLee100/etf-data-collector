import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def run():
    print("🚀 [최종 정밀 수집] P(종가)와 V(거래량) 모두 포함한 수집 시작...")
    now = datetime.now()
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    start_date, end_date = b_days[0], b_days[-1]

    # 1. 이름표 지도(Map) 생성
    print("🏷️ 이름표 데이터 확보 중...")
    name_map = {}
    try:
        for mkt in ["KOSPI", "KOSDAQ"]:
            tickers = stock.get_market_ticker_list(market=mkt)
            for t in tickers:
                name_map[t] = stock.get_market_ticker_name(t)
        
        etf_tickers = stock.get_etf_ticker_list(end_date)
        etf_name_map = {t: stock.get_etf_ticker_name(t) for t in etf_tickers}
        print(f"✅ 이름표 확보 완료 (총 {len(name_map) + len(etf_name_map)}개)")
    except:
        print("⚠️ 이름표 확보 중 오류 발생 (진행은 계속합니다)")

    # 데이터 바구니
    data_a, data_d = {}, {}
    adr_results = {'metric': 'Market_ADR'}

    # 2. 메인 루프 (날짜별 시계열 수집)
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...")

        # [CSV A] ETF 시세 및 거래량
        try:
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a: data_a[t] = {'ticker': t, 'name': etf_name_map.get(t, t)}
                data_a[t][f"{date_key}_P"] = row['종가']
                data_a[t][f"{date_key}_V"] = row['거래량'] # 거래량 추가
        except: pass

        # [CSV D & E] 주식 시세, 거래량, 수급
        try:
            # 시장별로 확실히 긁어와서 합침
            df_p = pd.concat([stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI"), 
                              stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")])
            df_v = pd.concat([stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSPI"),
                              stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSDAQ")])
            
            # E: ADR 산출
            ups = len(df_p[df_p['종가'] > df_p['시가']])
            downs = len(df_p[df_p['종가'] < df_p['시가']])
            adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

            # D: 전 종목 시계열 매핑
            for t, row in df_p.iterrows():
                if t not in data_d: data_d[t] = {'ticker': t, 'name': name_map.get(t, t)}
                data_d[t][f"{date_key}_P"] = row['종가']
                data_d[t][f"{date_key}_V"] = row['거래량'] # 거래량 추가
                if t in df_v.index:
                    v = row['거래량']
                    if v > 0:
                        data_d[t][f"{date_key}_For%"] = round(df_v.loc[t, '외국인']/v*100, 2)
        except: pass
        time.sleep(0.2)

    # 3. [CSV B] 파생상품 (선물/옵션)
    print("📂 CSV_B 수집 중...")
    list_b = []
    # 파생상품 코드를 표준형으로 수정
    for code, name in {"101": "Futures", "201": "Call", "301": "Put"}.items():
        try:
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, code)
            row = {'ticker': code, 'name': name}
            for d in tmp_days:
                ds = d.strftime('%Y-%m-%d')
                if d in df_p.index: row[f"{ds}_P"] = df_p.loc[d, '종가']
                if d in df_v.index: row[f"{ds}_ForNet"] = df_v.loc[d, '외국인']
            list_b.append(row)
        except: pass

    # 4. [CSV C] 글로벌 지수
    print("📂 CSV_C 수집 중...")
    try:
        df_c = yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close']
        csv_c_data = df_c.T
    except: csv_c_data = pd.DataFrame()

    # 5. 모든 파일 저장
    print("💾 모든 파일 저장 중...")
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
    csv_c_data.to_csv('CSV_C.csv', encoding='utf-8-sig')
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')

    print(f"🏁 작업 완료! (CSV_D 종목 수: {len(data_d)})")

if __name__ == "__main__":
    run()
