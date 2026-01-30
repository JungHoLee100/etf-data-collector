import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

# 이름 정보를 안전하게 가져오는 함수
def get_safe_name(ticker, is_etf=False):
    try:
        if is_etf: return stock.get_etf_ticker_name(ticker)
        return stock.get_market_ticker_name(ticker)
    except:
        return f"Unknown_{ticker}"

def run():
    print("🚀 [통합 엔진] 전 종목 예외 방어 모드 가동...")
    
    # 1. 영업일 확인 (최근 30거래일)
    now = datetime.now()
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    print(f"📅 대상 기간: {b_days[0]} ~ {b_days[-1]} ({len(b_days)}일)")

    # 데이터 저장 그릇
    data_a, data_b, data_d = {}, {}, {}
    adr_results = {'metric': 'Market_ADR'}
    
    # --- 메인 루프 (날짜별 일괄 수집) ---
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...", end="\r")

        # [CSV A] ETF 일괄 수집
        try:
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a: data_a[t] = {'ticker': t, 'name': get_safe_name(t, True)}
                data_a[t][f"{date_key}_P"] = row['종가']
                data_a[t][f"{date_key}_V"] = row['거래량']
        except: print(f"\n⚠️ {d_str} ETF 수집 건너뜀")

        # [CSV B] 파생상품 수급 (최신 함수명 반영)
        try:
            df_deriv = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, "ALL")
            for t in ["101SC", "201SC", "301SC"]:
                name = "Futures" if "101" in t else "Call" if "201" in t else "Put"
                if t not in data_b: data_b[t] = {'ticker': t, 'name': name}
                if t in df_deriv.index:
                    data_b[t][f"{date_key}_ForNet"] = df_deriv.loc[t, '외국인']
                    data_b[t][f"{date_key}_InstNet"] = df_deriv.loc[t, '기관합계']
        except: pass

        # [CSV D & E] 전 종목 시세 및 ADR
        try:
            df_stock_p = stock.get_market_ohlcv_by_ticker(d_str, market="ALL")
            
            # ADR 계산 (CSV_E)
            ups = len(df_stock_p[df_stock_p['대비'] > 0])
            downs = len(df_stock_p[df_stock_p['대비'] < 0])
            adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

            # 전 종목 시계열 (CSV_D)
            for t, row in df_stock_p.iterrows():
                if t not in data_d: data_d[t] = {'ticker': t, 'name': get_safe_name(t)}
                data_d[t][f"{date_key}_P"] = row['종가']
                data_d[t][f"{date_key}_V"] = row['거래량']
                if t in df_deriv.index:
                    v = row['거래량']
                    if v > 0:
                        data_d[t][f"{date_key}_For%"] = round(df_deriv.loc[t, '외국인']/v*100, 2)
                        data_d[t][f"{date_key}_Inst%"] = round(df_deriv.loc[t, '기관합계']/v*100, 2)
        except: pass
        
        time.sleep(0.3)

    # --- 파일 최종 저장 ---
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_b.values())).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')

    # CSV_C 글로벌 지수
    try:
        yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close'].T.to_csv('CSV_C.csv')
    except: pass
    
    print(f"\n✅ 모든 데이터({len(data_d)} 종목) 수집 완료!")

if __name__ == "__main__": run()
