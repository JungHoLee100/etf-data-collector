import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

# 1. 날짜 설정 (최근 30거래일 확보를 위해 약 45일 범위 설정)
end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

def run():
    print(f"[{start_date} ~ {end_date}] 전 종목 포함 통합 데이터 수집 시작...")

    try:
        # --- CSV A: ETF 마스터 (종가/거래량 30일 시계열) ---
        etf_list = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etf_list[:50]: # 주요 ETF 50개 대상
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                row = {'market': 'ETF', 'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A_ETF_30D.csv', index=False, encoding='utf-8-sig')

        # --- CSV B: 파생상품 포지션 (선물/옵션 외국인·기관) ---
        derivatives = {"101SC": "K200_Futures", "201SC": "K200_Call", "301SC": "K200_Put"}
        list_b = []
        for code, d_name in derivatives.items():
            df_b = stock.get_market_net_purchase_of_equities_by_date(start_date, end_date, code)
            for inv in ['외국인', '기관합계']:
                row = {'item': d_name, 'investor': inv}
                for d, val in df_b[inv].to_dict().items():
                    row[f"{d.strftime('%Y-%m-%d')}_Vol"] = val
                list_b.append(row)
        pd.DataFrame(list_b).to_csv('CSV_B_Derivatives_30D.csv', index=False, encoding='utf-8-sig')

        # --- CSV C: 글로벌 매크로 ---
        indices = {'^IXIC': 'Nasdaq', 'KRW=X': 'USD_KRW', '^SOX': 'Semicon'}
        df_c = yf.download(list(indices.keys()), start=pd.to_datetime(start_date), end=pd.to_datetime(end_date))['Close']
        df_c.T.to_csv('CSV_C_Global_30D.csv', encoding='utf-8-sig')

        # --- CSV D: [업데이트] 국내 주식 전 종목 30일 종가 ---
        print("CSV D: 전 종목 시계열 데이터 생성 중...")
        # 영업일 리스트 추출
        business_days = stock.get_market_ohlcv_by_date(start_date, end_date, "005930").index
        all_stocks_data = {}

        for d in business_days:
            d_str = d.strftime("%Y%m%d")
            # 해당 날짜의 전 종목 종가 가져오기 (KOSPI + KOSDAQ)
            df_kospi = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")['종가']
            df_kosdaq = stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")['종가']
            all_stocks_data[d.strftime('%Y-%m-%d')] = pd.concat([df_kospi, df_kosdaq])

        df_d = pd.DataFrame(all_stocks_data)
        # 종목명 추가를 위해 티커 리스트 확보
        df_d.index.name = 'ticker'
        df_d.reset_index(inplace=True)
        # 종목명 매핑 (성능을 위해 마지막 날짜 기준 1회만 수행)
        full_names = {t: stock.get_market_ticker_name(t) for t in df_d['ticker']}
        df_d.insert(1, 'name', df_d['ticker'].map(full_names))
        
        df_d.to_csv('CSV_D_All_Stocks_30D.csv', index=False, encoding='utf-8-sig')

        # --- CSV E: 시장 모멘텀 (ADR 30일) ---
        row_e = {'metric': 'Market_ADR'}
        for d in business_days:
            d_str = d.strftime("%Y%m%d")
            m_df = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            ups, downs = len(m_df[m_df['대비'] > 0]), len(m_df[m_df['대비'] < 0])
            row_e[f"{d.strftime('%Y-%m-%d')}"] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([row_e]).to_csv('CSV_E_Momentum_30D.csv', index=False, encoding='utf-8-sig')

        print("모든 시장 데이터가 성공적으로 통합되었습니다.")
    except Exception as e:
        print(f"데이터 수집 중 오류: {e}")

if __name__ == "__main__":
    run()
