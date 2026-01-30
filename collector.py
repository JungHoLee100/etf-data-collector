import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

def run():
    print("🚀 [통합 수집 엔진] 모든 파일 일괄 저장 모드 가동...")
    now = datetime.now()
    tmp_days = stock.get_market_ohlcv_by_date((now - timedelta(days=60)).strftime("%Y%m%d"), 
                                               now.strftime("%Y%m%d"), "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    start_date, end_date = b_days[0], b_days[-1]

    # 1. 이름표 및 섹터 정보 사전 확보 (속도 최적화)
    print("🏷️ 종목 정보 사전 확보 중...")
    try:
        df_info = stock.get_market_sector_by_ticker(end_date, market="ALL")
        name_map = df_info['종목명'].to_dict()
        etf_tickers = stock.get_etf_ticker_list(end_date)
        etf_name_map = {t: stock.get_etf_ticker_name(t) for t in etf_tickers}
    except:
        name_map, etf_name_map = {}, {}

    # 데이터 저장용 그릇
    data_a, data_d = {}, {}
    adr_results = {'metric': 'Market_ADR'}

    # 2. 메인 루프 (날짜별 일괄 수집: A, D, E 처리)
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...")

        # [CSV A] ETF
        try:
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a: data_a[t] = {'ticker': t, 'name': etf_name_map.get(t, t)}
                data_a[t][f"{date_key}_P"] = row['종가']
        except: pass

        # [CSV D & E] 주식 및 ADR
        try:
            df_p = stock.get_market_ohlcv_by_ticker(d_str, market="ALL")
            df_v = stock.get_market_net_purchases_of_equities_by_ticker(d_str, d_str, market="ALL")
            
            # E: ADR 산출 ('대비' 컬럼 부재 시 방어 로직)
            if '대비' in df_p.columns:
                ups = len(df_p[df_p['대비'] > 0])
                downs = len(df_p[df_p['대비'] < 0])
            else:
                # '대비'가 없으면 종가와 시가 차이로 계산
                diff = df_p['종가'] - df_p['시가']
                ups = len(diff[diff > 0])
                downs = len(diff[diff < 0])
            
            adr_results[date_key] = round(ups/downs*100, 2) if downs != 0 else 100

            # D: 전 종목 시계열 매핑
            for t, row in df_p.iterrows():
                if t not in data_d: data_d[t] = {'ticker': t, 'name': name_map.get(t, t)}
                data_d[t][f"{date_key}_P"] = row['종가']
                if t in df_v.index:
                    vol = row['거래량']
                    if vol > 0:
                        data_d[t][f"{date_key}_ForNet%"] = round(df_v.loc[t, '외국인']/vol*100, 2)
        except: pass
        
        time.sleep(0.3)

    # 3. [CSV B] 파생상품 수집 (정밀 개별 호출)
    print("📂 CSV_B 데이터 구성 중...")
    list_b = []
    for code, name in {"101": "Futures", "201": "Call", "301": "Put"}.items():
        try:
            df_deriv_p = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            df_deriv_v = stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, code)
            row = {'ticker': code, 'name': name}
            for date in tmp_days:
                d_str = date.strftime('%Y-%m-%d')
                if date in df_deriv_p.index: row[f"{d_str}_P"] = df_deriv_p.loc[date, '종가']
                if date in df_deriv_v.index: row[f"{d_str}_ForNet"] = df_deriv_v.loc[date, '외국인']
            list_b.append(row)
        except: pass

    # 4. 최종 저장 단계 (A, B, C, D, E 모든 파일 저장)
    print("💾 모든 파일을 저장소에 기록 중...")
    
    # CSV_A 저장
    pd.DataFrame(list(data_a.values())).to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    
    # CSV_B 저장
    pd.DataFrame(list_b).to_csv('CSV_B.csv', index=False, encoding='utf-8-sig')
    
    # CSV_C 저장 (글로벌 지수)
    try:
        df_global = yf.download(['^IXIC', 'KRW=X', '^SOX'], start=tmp_days[0], progress=False)['Close']
        df_global.T.to_csv('CSV_C.csv', encoding='utf-8-sig')
    except Exception as e:
        print(f"⚠️ CSV_C 저장 실패: {e}")

    # CSV_D 저장
    pd.DataFrame(list(data_d.values())).to_csv('CSV_D.csv', index=False, encoding='utf-8-sig')
    
    # CSV_E 저장
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
    
    print("🏁 모든 작업이 완료되었습니다! (A, B, C, D, E 생성 완료)")

if __name__ == "__main__":
    run()
