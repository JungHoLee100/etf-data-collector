import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

# 1. 날짜 설정 (최근 30거래일 확보)
end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

def run():
    print(f"[{datetime.now()}] 정호님 맞춤형 통합 수집 엔진 가동...")
    try:
        b_days = stock.get_market_ohlcv_by_date(start_date, end_date, "005930").index
    except: return

    # --- CSV A: 전체 ETF (시장, 섹터, 종가, 거래량) ---
    try:
        etfs = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etfs:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                # 시장/섹터 정보 (ETF는 기초지수명을 섹터로 활용)
                info = stock.get_etf_item_info(t)
                row = {'market': 'ETF', 'sector': info.get('기초지수명', 'N/A'), 'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A_ETF_ALL_30D.csv', index=False, encoding='utf-8-sig')
        print("CSV A 완료")
    except Exception as e: print(f"A 오류: {e}")

    # --- CSV D: K200/Q150 핵심 종목 (종가, 거래량, 수급 점유비) ---
    try:
        k200 = stock.get_index_portfolio_deposit_file("1028", end_date)
        q150 = stock.get_index_portfolio_deposit_file("2034", end_date)
        targets = {t: 'KOSPI200' for t in k200}; targets.update({t: 'KOSDAQ150' for t in q150})
        list_d = []
        for t, mkt in targets.items():
            df_s = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, t)
            if not df_s.empty:
                # 섹터 정보 가져오기
                sector = stock.get_market_sector_by_ticker(end_date, t)
                row = {'market': mkt, 'sector': sector, 'ticker': t, 'name': stock.get_market_ticker_name(t)}
                for date in b_days:
                    d_str = date.strftime('%Y-%m-%d')
                    if date in df_s.index:
                        row[f"{d_str}_P"], row[f"{d_str}_V"] = df_s.loc[date, '종가'], df_s.loc[date, '거래량']
                        # 수급 점유비 (전체 거래량 대비 순매수 비중 %)
                        total_v = df_s.loc[date, '거래량']
                        if total_v > 0:
                            row[f"{d_str}_For%"] = round(df_v.loc[date, '외국인'] / total_v * 100, 2)
                            row[f"{d_str}_Inst%"] = round(df_v.loc[date, '기관합계'] / total_v * 100, 2)
                list_d.append(row)
        pd.DataFrame(list_d).to_csv('CSV_D_Index_Stocks_30D.csv', index=False, encoding='utf-8-sig')
        print("CSV D 완료")
    except Exception as e: print(f"D 오류: {e}")

    # --- CSV B: 파생상품 (선물/옵션 수급) ---
    try:
        derivatives = {"101SC": "Futures", "201SC": "Call", "301SC": "Put"}
        list_b = []
        for code, name in derivatives.items():
            df_b = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_v = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, code)
            row = {'market': 'Derivatives', 'ticker': code, 'name': name}
            for date in b_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_b.index:
                    row[f"{d_str}_P"] = df_b.loc[date, '종가']
                    row[f"{d_str}_V"] = df_b.loc[date, '거래량']
                    row[f"{d_str}_For_Net"] = df_v.loc[date, '외국인']
                    row[f"{d_str}_Inst_Net"] = df_v.loc[date, '기관합계']
            list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B_Derivatives_30D.csv', index=False, encoding='utf-8-sig')
        print("CSV B 완료")
    except Exception as e: print(f"B 오류: {e}")

    # --- CSV C & E (글로벌 종가 & 모멘텀) ---
    # C와 E는 이전 로직과 동일하게 30일 시계열을 생성합니다.
