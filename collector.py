import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def run():
    print("🚀 [최종 해결 모드] 선물·옵션 및 전 종목 수집 엔진 가동...")
    now = datetime.now()
    # 최근 30거래일 확보
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    start_date, end_date = b_days[0], b_days[-1]

    # 1. 이름표 지도 사전 확보 (안전장치 강화)
    name_map = {}
    print("🏷️ 종목 이름표 확보 중...")
    for mkt in ["KOSPI", "KOSDAQ"]:
        tickers = stock.get_market_ticker_list(market=mkt)
        for t in tickers:
            name_map[t] = stock.get_market_ticker_name(t)

    # 2. [CSV_B] 선물/옵션 데이터 (파생상품 전용 로직)
    print("📂 CSV_B (선물/옵션) 수집 시작...")
    list_b = []
    # 101FM: KOSPI200 선물, 201FM: 콜, 301FM: 풋 (표준 파생 코드)
    # pykrx의 get_market_net_purchases_of_equities_by_ticker는 선물 티커도 지원함
    for code, name in {"101SC": "K200_Futures", "201SC": "K200_Call", "301SC": "K200_Put"}.items():
        try:
            # 선물 시세와 수급을 가져오기 위해 기간 조회 함수 사용
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, code)
            
            row = {'ticker': code, 'name': name}
            for d in tmp_days:
                ds = d.strftime('%Y-%m-%d')
                if d in df_p.index:
                    row[f"{ds}_P"] = df_p.loc[d, '종가']
                    row[f"{ds}_V"] = df_p.loc[d, '거래량']
                if d in df_v.index:
                    row[f"{ds}_ForNet"] = df_v.loc[d, '외국인']
                    row[f"{ds}_InstNet"] = df_v.loc[d, '기관합계']
            list_b.append(row)
        except Exception as e:
            print(f"⚠️ {name} 수집 실패: {e}")
    pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')

    # 3. [CSV_D & E & A] 주식 및 ETF 데이터 (일자별 배치)
    data_a, data_d = {}, {}
    adr_results = {'metric': 'Market_ADR'}

    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...")

        try:
            # ETF
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a: data_a[t] = {'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                data_a[t][f"{date_key}_P"], data_a[t][f"{date_key}_V"] = row['종가'], row['거래량']

            # 전 종목 시세 및 수급 (KOSPI/KOSDAQ 분리 수집 후 합침)
            p_k = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            p_q = stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")
            v_k = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSPI")
            v_q = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSDAQ")
            
            df_p = pd.concat([p_k, p_q])
            df_v = pd.concat([v_k, v_q])

            # ADR 계산 (CSV_E)
            ups = len(df_p[df_p['종가'] > df_p['시가']])
            downs = len(df_p[df_p['종가'] < df_p['시가']])
            adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

            # CSV_D 시계열 매핑
            for t, row in df_p.iterrows():
                if t not in data_d: data_d[t] = {'ticker': t, 'name': name_map.get(t, t)}
                data_d[t][f"{date_key}_P"], data_d[t][f"{date_key}_V"] = row['종가'], row['거래량']
                if t in df_v.index:
                    vol = row['거래량']
                    if vol > 0:
                        data_d[t][f"{date_key}_For%"] = round(df_v.loc[t, '외국인']/vol*100, 2)
                        data_d[t][f"{date_key}_Inst%"] = round(df_v.loc[t, '기관합계']/vol*100, 2)
        except Exception as e:
            print(f"⚠️ {d_str} 처리 중 오류: {e}")
        time.sleep(0.5)

    # 4. 저장 (A, D, E)
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')

    # 5. [CSV_C] 글로벌
    yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close'].T.to_csv('CSV_C.csv')
    
    print(f"🏁 모든 작업 완료! (CSV_D 종목 수: {len(data_d)})")

if __name__ == "__main__":
    run()
