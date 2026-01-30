import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

# 1. 날짜 설정: 오늘부터 45일 전까지 (30거래일 확보용)
end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

def run():
    print(f"[{start_date} ~ {end_date}] 30거래일 정밀 수집 시작...")

    try:
        # --- CSV A: ETF 마스터 + 30일 가격 시계열 ---
        # 주요 ETF 10개에 대한 30일치 OHLCV 데이터를 하나의 파일로 결합
        target_etfs = stock.get_etf_ticker_list(end_date)[:20] # 상위 20개 종목 예시
        list_a = []
        for t in target_etfs:
            name = stock.get_etf_ticker_name(t)
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            df['ticker'] = t
            df['name'] = name
            list_a.append(df)
        pd.concat(list_a).to_csv('CSV_A_ETF_30D.csv', encoding='utf-8-sig')
        print("CSV_A: 30일 시계열 생성 완료")

        # --- CSV B: 수급 마스터 (30일 누적 포지션) ---
        # 기관/외국인/개인의 30일간 순매수 합계
        df_b = stock.get_etf_net_purchase_of_equities_by_ticker(start_date, end_date, "KOSPI")
        df_b.to_csv('CSV_B_Supply_30D.csv', encoding='utf-8-sig')
        print("CSV_B: 수급 데이터 완료")

        # --- CSV C: 글로벌 매크로 (30일 시계열) ---
        indices = {'^IXIC': 'Nasdaq', 'KRW=X': 'USD_KRW', '^SOX': 'Semicon'}
        df_c = yf.download(list(indices.keys()), start=pd.to_datetime(start_date), end=pd.to_datetime(end_date))['Close']
        df_c.to_csv('CSV_C_Global_30D.csv', encoding='utf-8-sig')
        print("CSV_C: 매크로 데이터 완료")

        # --- CSV D: 주요 구성종목(Constituents) 30일 데이터 ---
        # KODEX 200(069500) 등 대표 ETF의 구성종목 TOP 10의 30일치 시세
        pdf = stock.get_etf_portfolio_deposit_file("069500", end_date)
        top_stocks = list(pdf.keys())[:10]
        list_d = []
        for s in top_stocks:
            s_name = stock.get_market_ticker_name(s)
            df_s = stock.get_market_ohlcv_by_date(start_date, end_date, s)
            df_s['ticker'] = s
            df_s['name'] = s_name
            list_d.append(df_s)
        pd.concat(list_d).to_csv('CSV_D_Constituents_30D.csv', encoding='utf-8-sig')
        print("CSV_D: 구성종목 시세 완료")

        # --- CSV E: 시장 모멘텀 (30일 ADR 추이) ---
        # 시장의 과열/침체를 알 수 있는 30일간의 등락 종목 비율
        adr_list = []
        # 날짜별로 루프를 돌며 ADR 계산 (최근 10일치 예시)
        dates = stock.get_market_ohlcv_by_date(start_date, end_date, "005930").index[-10:]
        for d in dates:
            d_str = d.strftime("%Y%m%d")
            m_df = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            ups = len(m_df[m_df['대비'] > 0])
            downs = len(m_df[m_df['대비'] < 0])
            adr_list.append({'date': d_str, 'adr': (ups/downs*100 if downs!=0 else 100)})
        pd.DataFrame(adr_list).to_csv('CSV_E_Momentum_30D.csv', index=False, encoding='utf-8-sig')
        print("CSV_E: 모멘텀 지표 완료")

    except Exception as e:
        print(f"데이터 생성 중 오류: {e}")

if __name__ == "__main__":
    run()
