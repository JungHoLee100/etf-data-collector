import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

def run():
    print("🚀 수집 엔진 가동 (정호님 맞춤형 최종본)...")
    
    # 1. 영업일 확인 (최근 30거래일 데이터 확보)
    now = datetime.now()
    tmp_start = (now - timedelta(days=50)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    
    try:
        b_days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index
        if len(b_days) < 30:
            print(f"⚠️ 영업일이 부족합니다. ({len(b_days)}일 확보)")
        
        # 30거래일치로 제한
        b_days = b_days[-30:]
        start_date, end_date = b_days[0].strftime("%Y%m%d"), b_days[-1].strftime("%Y%m%d")
        print(f"✅ 수집 범위 확정: {start_date} ~ {end_date} (총 {len(b_days)}일)")

        # --- CSV A: ETF 전 종목 (시장, 섹터, 가격, 거래량) ---
        print("📂 CSV A: ETF 전 종목 수집 중...")
        etfs = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etfs:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                name = stock.get_etf_ticker_name(t)
                row = {'market': 'ETF', 'sector': 'ETF', 'ticker': t, 'name': name}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A_ETF_ALL_30D.csv', index=False, encoding='utf-8-sig')

        # --- CSV B: 파생상품 (선물/옵션 매수/매도량 상세) ---
        print("📂 CSV B: 선물/옵션 수급 상세 수집 중...")
        derivatives = {"101SC": "Futures", "201SC": "Call", "301SC": "Put"}
        list_b = []
        for code, name in derivatives.items():
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, code)
            row = {'market': 'Deriv', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_p.index and date in df_v.index:
                    row[f"{d_str}_P"] = df_p.loc[date, '종가']
                    row[f"{d_str}_V"] = df_p.loc[date, '거래량']
                    # 외인/기관 매수 및 매도량 (거래량 기준)
                    row[f"{d_str}_ForBuy"] = df_v.loc[date, '외국인매수'] if '외국인매수' in df_v.columns else 0
                    row[f"{d_str}_ForSell"] = df_v.loc[date, '외국인매도'] if '외국인매도' in df_v.columns else 0
                    row[f"{d_str}_InstBuy"] = df_v.loc[date, '기관매수'] if '기관매수' in df_v.columns else 0
                    row[f"{d_str}_InstSell"] = df_v.loc[date, '기관매도'] if '기관매도' in df_v.columns else 0
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B_Derivatives_30D.csv', index=False, encoding='utf-8-sig')

        # --- CSV C: 글로벌 매크로 ---
        print("📂 CSV C: 글로벌 지수 수집 중...")
        indices = {'^IXIC': 'Nasdaq', 'KRW=X': 'USD_KRW', '^SOX': 'Semicon'}
        df_c = yf.download(list(indices.keys()), start=pd.to_datetime(start_date))['Close']
        df_c.T.to_csv('CSV_C_Global_30D.csv', encoding='utf-8-sig')

        # --- CSV D: K200/Q150 (섹터, 점유비 포함) ---
        print("📂 CSV D: 주요 지수 종목 상세 수집 중...")
        k200 = stock.get_index_portfolio_deposit_file("1028", end_date)
        q150 = stock.get_index_portfolio_deposit_file("2034", end_date)
        targets = {t: 'KOSPI' for t in k200}; targets.update({t: 'KOSDAQ' for t in q150})
        list_d = []
        for t, mkt in targets.items():
            df_s = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, t)
            if not df_s.empty:
                sector = stock.get_market_sector_by_ticker(end_date, t)
                row = {'market': mkt, 'sector': sector, 'ticker': t, 'name': stock.get_market_ticker_name(t)}
                for date in b_days:
                    d_str = date.strftime('%Y-%m-%d')
                    if date in df_s.index:
                        v = df_s.loc[date, '거래량']
                        row[f"{d_str}_P"], row[f"{d_str}_V"] = df_s.loc[date, '종가'], v
                        if v > 0 and date in df_v.index:
                            # 수급 비중 (%) - 거래량 대비 순매수 비중
                            row[f"{d_str}_For%"] = round(df_v.loc[date, '외국인']/v*100, 2)
                            row[f"{d_str}_Inst%"] = round(df_v.loc[date, '기관합계']/v*100, 2)
                            row[f"{d_str}_Ind%"] = round(df_v.loc[date, '개인']/v*100, 2)
                list_d.append(row)
        pd.DataFrame(list_d).to_csv('CSV_D_Index_Stocks_30D.csv', index=False, encoding='utf-8-sig')

        # --- CSV E: 시장 모멘텀 (ADR) ---
        print("📂 CSV E: ADR 모멘텀 산출 중...")
        row_e = {'metric': 'Market_ADR'}
        for d in b_days:
            m_df = stock.get_market_ohlcv_by_ticker(d.strftime("%Y%m%d"), market="KOSPI")
            ups, downs = len(m_df[m_df['대비']>0]), len(m_df[m_df['대비']<0])
            row_e[d.strftime('%Y-%m-%d')] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([row_e]).to_csv('CSV_E_Momentum_30D.csv', index=False, encoding='utf-8-sig')

        print("🏁 모든 작업 완료!")
    except Exception as e:
        print(f"❗ 오류 발생: {e}")
        raise e

if __name__ == "__main__":
    run()
