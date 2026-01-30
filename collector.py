import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

def get_trading_days():
    now = datetime.now()
    # 최근 30거래일 확보를 위해 60일치 조회
    tmp_start = (now - timedelta(days=60)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index
    return days[-30:]

def run():
    print("🚀 데이터 수집 엔진 가동 (정밀 수정 버전)...")
    b_days = get_trading_days()
    start_date = b_days[0].strftime("%Y%m%d")
    end_date = b_days[-1].strftime("%Y%m%d")
    print(f"📅 분석 기간: {start_date} ~ {end_date}")

    # --- CSV_A: ETF 전체 (종가, 거래량) ---
    try:
        print("📂 CSV_A 생성 중...")
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
    except Exception as e: print(f"❌ CSV_A 오류: {e}")

    # --- CSV_B: 선물/옵션 (시계열 데이터 포함) ---
    try:
        print("📂 CSV_B 생성 중...")
        derivatives = {"101": "K200_Futures", "201": "K200_Call", "301": "K200_Put"}
        list_b = []
        for code, name in derivatives.items():
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, code)
            row = {'market': 'Deriv', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_p.index:
                    row[f"{d_str}_P"] = df_p.loc[date, '종가']
                    row[f"{d_str}_V"] = df_p.loc[date, '거래량']
                if date in df_v.index:
                    row[f"{d_str}_ForNet"] = df_v.loc[date, '외국인']
                    row[f"{d_str}_InstNet"] = df_v.loc[date, '기관합계']
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_B 완료")
    except Exception as e: print(f"❌ CSV_B 오류: {e}")

    # --- CSV_C: 글로벌 매크로 ---
    try:
        print("📂 CSV_C 생성 중...")
        indices = {'^IXIC': 'Nasdaq', 'KRW=X': 'USD_KRW', '^SOX': 'Semicon'}
        df_c = yf.download(list(indices.keys()), start=pd.to_datetime(start_date))['Close']
        df_c.T.to_csv('CSV_C.csv', encoding='utf-8-sig')
        print("✅ CSV_C 완료")
    except Exception as e: print(f"❌ CSV_C 오류: {e}")

    # --- CSV_D: K200/Q150 (섹터 및 수급 비중 포함) ---
    try:
        print("📂 CSV_D 생성 중...")
        k200 = stock.get_index_portfolio_deposit_file("1028", end_date)
        q150 = stock.get_index_portfolio_deposit_file("2034", end_date)
        targets = {t: 'KOSPI200' for t in k200}; targets.update({t: 'KOSDAQ150' for t in q150})
        # 섹터 정보 미리 가져오기
        sectors = stock.get_market_sector_by_ticker(end_date)
        list_d = []
        for t, mkt in targets.items():
            df_s = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, t)
            if not df_s.empty:
                sector_name = sectors.loc[t, '업종명'] if t in sectors.index else 'N/A'
                row = {'market': mkt, 'sector': sector_name, 'ticker': t, 'name': stock.get_market_ticker_name(t)}
                for date in b_days:
                    d_str = date.strftime('%Y-%m-%d')
                    if date in df_s.index:
                        v = df_s.loc[date, '거래량']
                        row[f"{d_str}_P"], row[f"{d_str}_V"] = df_s.loc[date, '종가'], v
                        if v > 0 and date in df_v.index:
                            row[f"{d_str}_For%"] = round(df_v.loc[date, '외국인']/v*100, 2)
                            row[f"{d_str}_Inst%"] = round(df_v.loc[date, '기관합계']/v*100, 2)
                            row[f"{d_str}_Ind%"] = round(df_v.loc[date, '개인']/v*100, 2)
                list_d.append(row)
        pd.DataFrame(list_d).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_D 완료")
    except Exception as e: print(f"❌ CSV_D 오류: {e}")

    # --- CSV_E: 시장 모멘텀 (ADR) ---
    try:
        print("📂 CSV_E 생성 중...")
        row_e = {'metric': 'Market_ADR'}
        for d in b_days:
            d_str = d.strftime("%Y%m%d")
            m_df = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            ups, downs = len(m_df[m_df['대비']>0]), len(m_df[m_df['대비']<0])
            row_e[d.strftime('%Y-%m-%d')] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([row_e]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_E 완료")
    except Exception as e: print(f"❌ CSV_E 오류: {e}")

if __name__ == "__main__":
    run()
