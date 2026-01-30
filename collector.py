import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

# 1. 날짜 설정 (최근 30거래일 확보)
def get_trading_days():
    now = datetime.now()
    tmp_start = (now - timedelta(days=60)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    # 삼성전자 기준으로 실제 장이 열렸던 날짜 리스트 추출
    days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index
    return days[-30:] # 최근 30거래일 확정

def run():
    print("🚀 수집 엔진 가동 (파일명 고정 업데이트 버전)...")
    b_days = get_trading_days()
    start_date, end_date = b_days[0].strftime("%Y%m%d"), b_days[-1].strftime("%Y%m%d")
    print(f"📅 분석 범위: {start_date} ~ {end_date}")

    # --- CSV A: ETF 전체 (가격, 거래량) ---
    try:
        etfs = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etfs:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                name = stock.get_etf_ticker_name(t)
                row = {'market': 'ETF', 'ticker': t, 'name': name}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_A 업데이트 완료")
    except Exception as e: print(f"❌ A 오류: {e}")

    # --- CSV B: 선물/옵션 (매수/매도 상세) ---
    try:
        derivatives = {"101SC": "Futures", "201SC": "Call", "301SC": "Put"}
        list_b = []
        for code, name in derivatives.items():
            df_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            # 투자자별 매수/매도량 상세 데이터
            df_v = stock.get_market_trading_volume_by_date(start_date, end_date, code)
            row = {'market': 'Deriv', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_p.index:
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = df_p.loc[date, '종가'], df_p.loc[date, '거래량']
                    # 외국인/기관 매수 및 매도량 상세 추가
                    row[f"{d_str}_ForBuy"] = df_v.loc[(date, '외국인'), '매수'] if (date, '외국인') in df_v.index else 0
                    row[f"{d_str}_ForSell"] = df_v.loc[(date, '외국인'), '매도'] if (date, '외국인') in df_v.index else 0
                    row[f"{d_str}_InstBuy"] = df_v.loc[(date, '기관합계'), '매수'] if (date, '기관합계') in df_v.index else 0
                    row[f"{d_str}_InstSell"] = df_v.loc[(date, '기관합계'), '매도'] if (date, '기관합계') in df_v.index else 0
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_B 업데이트 완료")
    except Exception as e: print(f"❌ B 오류: {e}")

    # --- CSV C: 글로벌 매크로 ---
    try:
        indices = {'^IXIC': 'Nasdaq', 'KRW=X': 'USD_KRW', '^SOX': 'Semicon'}
        df_c = yf.download(list(indices.keys()), start=pd.to_datetime(start_date))['Close']
        df_c.T.to_csv('CSV_C.csv', encoding='utf-8-sig')
        print("✅ CSV_C 업데이트 완료")
    except Exception as e: print(f"❌ C 오류: {e}")

    # --- CSV D: K200/Q150 (섹터, 점유비 포함) ---
    try:
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
                    if date in df_s.index and date in df_v.index:
                        v = df_s.loc[date, '거래량']
                        row[f"{d_str}_P"], row[f"{d_str}_V"] = df_s.loc[date, '종가'], v
                        if v > 0:
                            row[f"{d_str}_For%"] = round(df_v.loc[date, '외국인']/v*100, 2)
                            row[f"{d_str}_Inst%"] = round(df_v.loc[date, '기관합계']/v*100, 2)
                            row[f"{d_str}_Ind%"] = round(df_v.loc[date, '개인']/v*100, 2)
                list_d.append(row)
        pd.DataFrame(list_d).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_D 업데이트 완료")
    except Exception as e: print(f"❌ D 오류: {e}")

    # --- CSV E: 시장 모멘텀 (ADR) ---
    try:
        row_e = {'metric': 'Market_ADR'}
        for d in b_days:
            m_df = stock.get_market_ohlcv_by_ticker(d.strftime("%Y%m%d"), market="KOSPI")
            ups, downs = len(m_df[m_df['대비']>0]), len(m_df[m_df['대비']<0])
            row_e[d.strftime('%Y-%m-%d')] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([row_e]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
        print("✅ CSV_E 업데이트 완료")
    except Exception as e: print(f"❌ E 오류: {e}")

if __name__ == "__main__":
    run()
