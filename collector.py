import os, pandas as pd
import time
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

# 1. 날짜 설정 (최근 30거래일 확보)
end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")

def run():
    print(f"[{datetime.now()}] 전 종목 무제한 수집 엔진 가동...")
    
    # 공통 영업일 리스트 확보
    try:
        b_days = stock.get_market_ohlcv_by_date(start_date, end_date, "005930").index
        print(f"수집 대상 영업일: {len(b_days)}일 확인")
    except:
        print("영업일 데이터를 가져오는데 실패했습니다. 종료합니다.")
        return

    # --- CSV A: 모든 ETF (시장구분/종가/거래량) ---
    try:
        print("CSV A 생성 중 (전체 ETF)...")
        etf_tickers = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etf_tickers:
            name = stock.get_etf_ticker_name(t)
            # 기초지수명을 가져와서 시장 구분(KOSPI200/KOSDAQ150 등) 보조 지표로 활용
            meta = stock.get_etf_item_info(t)
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            
            if not df.empty:
                row = {'ticker': t, 'name': name, 'base_index': meta.get('기초지수명', 'N/A')}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        pd.DataFrame(list_a).to_csv('CSV_A_ETF_ALL_30D.csv', index=False, encoding='utf-8-sig')
        print(f"CSV A 완료: {len(list_a)}개 종목")
    except Exception as e: print(f"CSV A 오류: {e}")

    # --- CSV B: 파생상품 포지션 (선물/옵션) ---
    try:
        print("CSV B 생성 중 (파생 수급)...")
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
        print("CSV B 완료")
    except Exception as e: print(f"CSV B 오류: {e}")

    # --- CSV C: 글로벌 매크로 ---
    try:
        print("CSV C 생성 중 (Global)...")
        indices = {'^IXIC': 'Nasdaq', 'KRW=X': 'USD_KRW', '^SOX': 'Semicon'}
        df_c = yf.download(list(indices.keys()), start=pd.to_datetime(start_date), end=pd.to_datetime(end_date))['Close']
        df_c.T.to_csv('CSV_C_Global_30D.csv', encoding='utf-8-sig')
        print("CSV C 완료")
    except Exception as e: print(f"CSV C 오류: {e}")

    # --- CSV D: 전 종목 (KOSPI/KOSDAQ) 30일 종가 ---
    try:
        print("CSV D 생성 중 (전 종목 시계열)...")
        day_cols = {}
        for d in b_days:
            d_str = d.strftime("%Y%m%d")
            # 시장별 데이터를 한 번에 가져와서 결합 (매우 효율적임)
            k_p = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")['종가']
            q_p = stock.get_market_ohlcv_by_ticker(d_str, market="KOSDAQ")['종가']
            day_cols[d.strftime('%Y-%m-%d')] = pd.concat([k_p, q_p])
        
        df_d = pd.DataFrame(day_cols)
        df_d.index.name = 'ticker'
        df_d.reset_index(inplace=True)
        # 종목명/시장 구분 정보 추가
        all_names = {t: stock.get_market_ticker_name(t) for t in df_d['ticker']}
        df_d.insert(1, 'name', df_d['ticker'].map(all_names))
        df_d.to_csv('CSV_D_All_Stocks_30D.csv', index=False, encoding='utf-8-sig')
        print(f"CSV D 완료: {len(df_d)}개 종목")
    except Exception as e: print(f"CSV D 오류: {e}")

    # --- CSV E: 시장 모멘텀 (ADR) ---
    try:
        print("CSV E 생성 중 (ADR)...")
        row_e = {'metric': 'Market_ADR'}
        for d in b_days[-20:]: # 최근 20일치만 정밀 계산
            d_str = d.strftime("%Y%m%d")
            m_df = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            ups, downs = len(m_df[m_df['대비'] > 0]), len(m_df[m_df['대비'] < 0])
            row_e[f"{d.strftime('%Y-%m-%d')}"] = round(ups/downs*100, 2) if downs != 0 else 100
        pd.DataFrame([row_e]).to_csv('CSV_E_Momentum_30D.csv', index=False, encoding='utf-8-sig')
        print("CSV E 완료")
    except Exception as e: print(f"CSV E 오류: {e}")

if __name__ == "__main__":
    run()
