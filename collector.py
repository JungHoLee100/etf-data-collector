import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

def run():
    print("🚀 [1/5] 수집 엔진 가동 - 영업일 확인 중...")
    now = datetime.now()
    tmp_start = (now - timedelta(days=60)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    
    try:
        b_days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index[-30:]
        start_date, end_date = b_days[0].strftime("%Y%m%d"), b_days[-1].strftime("%Y%m%d")
        print(f"📅 기간 확정: {start_date} ~ {end_date}")

        # --- CSV_A (ETF) ---
        print("📂 [2/5] CSV_A 생성 시작...")
        etfs = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etfs:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                row = {'market': 'ETF', 'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                for date, data in df.iterrows():
                    row[f"{date.strftime('%Y-%m-%d')}_P"] = data['종가']
                    row[f"{date.strftime('%Y-%m-%d')}_V"] = data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
        print(f"✅ CSV_A 생성 완료: {os.path.exists('CSV_A.csv')}")

        # --- CSV_B (파생상품: 선물/옵션) ---
        print("📂 [3/5] CSV_B 생성 시작...")
        # 101: 선물, 201: 콜, 301: 풋 (표준 코드)
        deriv_map = {"101": "Futures", "201": "Call", "301": "Put"}
        list_b = []
        for code, name in deriv_map.items():
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, code)
            row = {'market': 'Deriv', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_p.index:
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = df_p.loc[date, '종가'], df_p.loc[date, '거래량']
                if date in df_v.index:
                    row[f"{d_str}_ForNet"] = df_v.loc[date, '외국인']
                    row[f"{d_str}_InstNet"] = df_v.loc[date, '기관합계']
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
        print(f"✅ CSV_B 생성 완료: {os.path.exists('CSV_B.csv')}")

        # --- CSV_C (글로벌) ---
        yf.download(['^IXIC', 'KRW=X', '^SOX'], start=pd.to_datetime(start_date))['Close'].T.to_csv('CSV_C.csv')
        print("✅ CSV_C 생성 완료")

        # --- CSV_D (주요 주식 시계열 - 초고속 모드) ---
        print("📂 [4/5] CSV_D 생성 시작 (K200, Q150)...")
        # 지수 구성종목 리스트 확보
        k200_tickers = stock.get_index_portfolio_deposit_file("1028", end_date)
        q150_tickers = stock.get_index_portfolio_deposit_file("2034", end_date)
        target_list = set(k200_tickers + q150_tickers)

        d_rows = {}
        for d in b_days:
            d_str = d.strftime("%Y%m%d")
            # 날짜별 전 종목 데이터를 한 번에 가져와서 필터링 (가장 빠름)
            day_data = pd.concat([stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI"), 
                                  stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")])
            day_v = pd.concat([stock.get_market_net_purchase_of_equities_by_ticker(d_str, market="KOSPI"),
                               stock.get_market_net_purchase_of_equities_by_ticker(d_str, market="KOSDAQ")])
            
            # 타겟 종목만 추출하여 저장
            for t in target_list:
                if t not in d_rows: d_rows[t] = {'ticker': t, 'name': stock.get_market_ticker_name(t)}
                if t in day_data.index:
                    d_rows[t][f"{d.strftime('%Y-%m-%d')}_P"] = day_data.loc[t, '종가']
                    v = day_data.loc[t, '거래량']
                    d_rows[t][f"{d.strftime('%Y-%m-%d')}_V"] = v
                    if v > 0 and t in day_v.index:
                        d_rows[t][f"{d.strftime('%Y-%m-%d')}_For%"] = round(day_v.loc[t, '외국인']/v*100, 2)
                        d_rows[t][f"{d.strftime('%Y-%m-%d')}_Inst%"] = round(day_v.loc[t, '기관합계']/v*100, 2)
        
        pd.DataFrame(list(d_rows.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
        print(f"✅ CSV_D 생성 완료: {os.path.exists('CSV_D.csv')}")

        # --- CSV_E (ADR) ---
        print("📂 [5/5] CSV_E 생성 시작...")
        adr_data = {'metric': 'Market_ADR'}
        for d in b_days:
            m_df = stock.get_market_ohlcv_by_ticker(d.strftime("%Y%m%d"), market="KOSPI")
            ups, downs = len(m_df[m_df['대비']>0]), len(m_df[m_df['대비']<0])
            adr_data[d.strftime('%Y-%m-%d')] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([adr_data]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
        print(f"✅ CSV_E 생성 완료: {os.path.exists('CSV_E.csv')}")

        print("🏁 모든 로컬 파일 생성 프로세스 종료.")

    except Exception as e:
        print(f"❌ 치명적 오류: {e}")
        raise e

if __name__ == "__main__":
    run()
