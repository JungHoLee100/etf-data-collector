import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def get_trading_days():
    now = datetime.now()
    tmp_start = (now - timedelta(days=60)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index
    return days[-30:]

def run():
    print("🚀 전 종목 대응 수집 엔진 가동...")
    b_days = get_trading_days()
    start_date, end_date = b_days[0].strftime("%Y%m%d"), b_days[-1].strftime("%Y%m%d")
    
    # --- CSV_A (ETF 전체) ---
    try:
        etfs = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etfs:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                row = {'market': 'ETF', 'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_A 완료")
    except Exception as e: print(f"❌ A 오류: {e}")

    # --- CSV_B (파생상품 수급) ---
    try:
        # 티커 앞에 시장 구분자를 붙여 더 정확하게 호출 (KOSPI200 선물 등)
        deriv_map = {"101SC": "Futures", "201SC": "Call", "301SC": "Put"}
        list_b = []
        for code, name in deriv_map.items():
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, code)
            row = {'market': 'Deriv', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_p.index: row[f"{d_str}_P"], row[f"{d_str}_V"] = df_p.loc[date, '종가'], df_p.loc[date, '거래량']
                if date in df_v.index: row[f"{d_str}_ForNet"], row[f"{d_str}_InstNet"] = df_v.loc[date, '외국인'], df_v.loc[date, '기관합계']
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_B 완료")
    except Exception as e: print(f"❌ B 오류: {e}")

    # --- CSV_C (글로벌 매크로) ---
    try:
        # 데이터 누락 방지를 위해 시작일을 조금 더 앞당겨 호출
        yf_start = (pd.to_datetime(start_date) - timedelta(days=7)).strftime("%Y-%m-%d")
        df_c = yf.download(['^IXIC', 'KRW=X', '^SOX'], start=yf_start, progress=False)['Close']
        df_c.T.to_csv('CSV_C.csv', encoding='utf-8-sig')
        print("✅ CSV_C 완료")
    except Exception as e: print(f"❌ C 오류: {e}")

    # --- CSV_D (코스피/코스닥 전 종목 상세) ---
    try:
        print("📂 CSV_D 수집 중 (전 종목 대상, 시간이 소요됩니다)...")
        # 시장별 티커 리스트 합치기
        all_tickers = stock.get_market_ticker_list(end_date, market="KOSPI") + \
                      stock.get_market_ticker_list(end_date, market="KOSDAQ")
        
        d_rows = {}
        # 속도를 위해 날짜별로 한 번에 가져와서 메모리에서 재구성
        for d in b_days:
            d_str = d.strftime("%Y%m%d")
            # 당일 전체 시세 및 수급 가져오기
            m_p = pd.concat([stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI"), stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")])
            m_v = pd.concat([stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSPI"), stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="KOSDAQ")])
            
            for t in all_tickers:
                if t not in d_rows: 
                    d_rows[t] = {'ticker': t, 'name': stock.get_market_ticker_name(t)}
                
                if t in m_p.index:
                    d_rows[t][f"{d_str}_P"], d_rows[t][f"{d_str}_V"] = m_p.loc[t, '종가'], m_p.loc[t, '거래량']
                    vol = m_p.loc[t, '거래량']
                    if vol > 0 and t in m_v.index:
                        d_rows[t][f"{d_str}_For%"] = round(m_v.loc[t, '외국인']/vol*100, 2)
                        d_rows[t][f"{d_str}_Inst%"] = round(m_v.loc[t, '기관합계']/vol*100, 2)
            time.sleep(0.1) # 서버 부하 방지
            
        pd.DataFrame(list(d_rows.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_D 완료")
    except Exception as e: print(f"❌ D 오류: {e}")

    # --- CSV_E (ADR 모멘텀) ---
    try:
        row_e = {'metric': 'Market_ADR'}
        for d in b_days:
            m_df = stock.get_market_ohlcv_by_ticker(d.strftime("%Y%m%d"), market="KOSPI")
            ups, downs = len(m_df[m_df['대비']>0]), len(m_df[m_df['대비']<0])
            row_e[d.strftime('%Y-%m-%d')] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([row_e]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_E 완료")
    except Exception as e: print(f"❌ E 오류: {e}")

if __name__ == "__main__": run()
