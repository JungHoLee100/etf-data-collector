import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

def get_trading_days():
    now = datetime.now()
    tmp_start = (now - timedelta(days=60)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    # 실제 장이 열린 날짜 확보
    days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index
    return days[-30:]

def run():
    print("🚀 수집 엔진 가동 (최종 수정본 - 함수명 및 파일명 고정)...")
    b_days = get_trading_days()
    start_date, end_date = b_days[0].strftime("%Y%m%d"), b_days[-1].strftime("%Y%m%d")
    print(f"📅 수집 기간: {start_date} ~ {end_date}")

    # --- CSV_A: ETF 전 종목 (종가, 거래량) ---
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

    # --- CSV_B: 파생상품 (선물/옵션 상세 수급) ---
    try:
        # 101: 선물, 201: 콜, 301: 풋
        deriv_map = {"101": "Futures", "201": "Call", "301": "Put"}
        list_b = []
        for code, name in deriv_map.items():
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            # 함수명 수정: _by_date -> _by_ticker (최신 pykrx 반영)
            df_v = stock.get_market_net_purchase_of_equities_by_ticker(start_date, end_date, code)
            row = {'market': 'Deriv', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_p.index:
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = df_p.loc[date, '종가'], df_p.loc[date, '거래량']
                if date in df_v.index:
                    row[f"{d_str}_ForNet"], row[f"{d_str}_InstNet"] = df_v.loc[date, '외국인'], df_v.loc[date, '기관합계']
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_B 완료")
    except Exception as e: print(f"❌ B 오류: {e}")

    # --- CSV_C: 글로벌 매크로 ---
    try:
        yf.download(['^IXIC', 'KRW=X', '^SOX'], start=pd.to_datetime(start_date))['Close'].T.to_csv('CSV_C.csv')
        print("✅ CSV_C 완료")
    except Exception as e: print(f"❌ C 오류: {e}")

    # --- CSV_D: K200/Q150 종목 상세 ---
    try:
        k200 = stock.get_index_portfolio_deposit_file(end_date, "1028")
        q150 = stock.get_index_portfolio_deposit_file(end_date, "2034")
        targets = {t: 'KOSPI200' for t in k200}; targets.update({t: 'KOSDAQ150' for t in q150})
        list_d = []
        for t, mkt in targets.items():
            df_s = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            df_v = stock.get_market_net_purchase_of_equities_by_ticker(start_date, end_date, t)
            if not df_s.empty:
                row = {'market': mkt, 'ticker': t, 'name': stock.get_market_ticker_name(t)}
                for date in b_days:
                    d_str = date.strftime('%Y-%m-%d')
                    if date in df_s.index and date in df_v.index:
                        v = df_s.loc[date, '거래량']
                        row[f"{d_str}_P"], row[f"{d_str}_V"] = df_s.loc[date, '종가'], v
                        if v > 0:
                            row[f"{d_str}_For%"] = round(df_v.loc[date, '외국인']/v*100, 2)
                            row[f"{d_str}_Inst%"] = round(df_v.loc[date, '기관합계']/v*100, 2)
                list_d.append(row)
        pd.DataFrame(list_d).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_D 완료")
    except Exception as e: print(f"❌ D 오류: {e}")

    # --- CSV_E: 시장 모멘텀 (ADR) ---
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
