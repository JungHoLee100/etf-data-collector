import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def run():
    print("🚀 [브릿지 모드] 지수 구성 종목 필터링 수집 엔진 가동...")
    now = datetime.now()
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    start_date, end_date = b_days[0], b_days[-1]

    # 1. 브릿지 단계: K200, Q150 리스트 확보 (최근 영업일 기준 검색)
    print("🌉 브릿지 가동: 지수 구성 종목(350개) 리스트 확보 중...")
    target_tickers = set()
    for i in range(1, 10): # 최근 10일 중 리스트가 있는 날을 찾음
        check_date = b_days[-i]
        k200 = stock.get_index_portfolio_deposit_file(check_date, "1028")
        q150 = stock.get_index_portfolio_deposit_file(check_date, "2034")
        if k200 and q150:
            target_tickers = set(k200 + q150)
            print(f"✅ {check_date} 기준 350개 종목 리스트 확보 완료")
            break
    
    # 2. 이름표 사전 확보
    name_map = {}
    for t in target_tickers:
        try: name_map[t] = stock.get_market_ticker_name(t)
        except: name_map[t] = t

    # 데이터 저장 그릇
    data_a, data_d = {}, {}
    adr_results = {'metric': 'Market_ADR'}

    # 3. 메인 루프 (날짜별 일괄 수집 후 필터링)
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...")

        try:
            # [A] ETF
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a: data_a[t] = {'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                data_a[t][f"{date_key}_P"], data_a[t][f"{date_key}_V"] = row['종가'], row['거래량']

            # [D & E] 주식 및 ADR (일괄 수집 후 필터링)
            p_k = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            p_q = stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")
            v_k = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSPI")
            v_q = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSDAQ")
            
            df_p = pd.concat([p_k, p_q])
            df_v = pd.concat([v_k, v_q])

            # E: ADR 산출 (KOSPI 상승/하락 기반)
            diff = df_p['종가'] - df_p['시가']
            ups, downs = len(diff[diff > 0]), len(diff[diff < 0])
            adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

            # D: 필터링 - target_tickers에 있는 것만 저장
            for t in target_tickers:
                if t in df_p.index:
                    if t not in data_d: data_d[t] = {'ticker': t, 'name': name_map.get(t, t)}
                    row_p = df_p.loc[t]
                    data_d[t][f"{date_key}_P"], data_d[t][f"{date_key}_V"] = row_p['종가'], row_p['거래량']
                    if t in df_v.index:
                        v = row_p['거래량']
                        if v > 0:
                            data_d[t][f"{date_key}_For%"] = round(df_v.loc[t, '외국인']/v*100, 2)
                            data_d[t][f"{date_key}_Inst%"] = round(df_v.loc[t, '기관합계']/v*100, 2)
        except Exception as e:
            print(f"⚠️ {d_str} 오류: {e}")
        time.sleep(0.3)

    # 4. [B] 파생상품 (선물/옵션 - 개별 정밀 수집)
    print("📂 CSV_B 수집 중...")
    list_b = []
    for code, name in {"101": "Futures", "201": "Call", "301": "Put"}.items():
        try:
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, code)
            row = {'ticker': code, 'name': name}
            for d in tmp_days:
                ds = d.strftime('%Y-%m-%d')
                if d in df_p.index: row[f"{ds}_P"], row[f"{ds}_V"] = df_p.loc[d, '종가'], df_p.loc[d, '거래량']
                if d in df_v.index: row[f"{ds}_ForNet"], row[f"{ds}_InstNet"] = df_v.loc[d, '외국인'], df_v.loc[d, '기관합계']
            list_b.append(row)
        except: pass

    # 5. 저장 단계
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
    yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close'].T.to_csv('CSV_C.csv')
    
    print(f"🏁 작업 완료! (CSV_D 필터링 결과: {len(data_d)} 종목)")

if __name__ == "__main__": run()
