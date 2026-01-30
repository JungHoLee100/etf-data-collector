import os
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf
import time

# [안정화 패치] GitHub Actions 환경에서 SQLite 에러 방지
try:
    yf.set_tz_cache(False)
except:
    pass

def run():
    print("🚀 [거시 지표 전용] CSV A, C, E 수집 엔진 가동...")
    now = datetime.now()
    
    # 1. 최근 30거래일 영업일 확보 (분석을 위한 충분한 시계열)
    # 삼성전자(005930)를 기준으로 실제 장이 열렸던 날짜 리스트를 가져옵니다.
    start_date_ref = (now - timedelta(days=60)).strftime("%Y%m%d")
    end_date_ref = now.strftime("%Y%m%d")
    tmp_days = stock.get_market_ohlcv_by_date(start_date_ref, end_date_ref, "005930").index[-30:]
    b_days = [d.strftime("%Y%m%d") for d in tmp_days]
    
    print(f"📅 수집 기간: {b_days[0]} ~ {b_days[-1]} ({len(b_days)} 거래일)")

    # 2. ETF 이름표 미리 확보 (매일 호출 방지)
    print("🏷️ ETF 이름표 확보 중...")
    etf_tickers = stock.get_etf_ticker_list(b_days[-1])
    etf_name_map = {t: stock.get_etf_ticker_name(t) for t in etf_tickers}

    # 데이터 보관용 딕셔너리
    data_a = {}
    adr_results = {'metric': 'Market_ADR'}

    # 3. 날짜별 루프 실행 (CSV A 및 E 수집)
    for d_str in b_days:
        date_key = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
        print(f"📦 {date_key} 데이터 처리 중...", end="\r")

        # [CSV_A] ETF 종가 및 거래량
        try:
            df_etf = stock.get_etf_ohlcv_by_ticker(d_str)
            for t, row in df_etf.iterrows():
                if t not in data_a:
                    data_a[t] = {'ticker': t, 'name': etf_name_map.get(t, t)}
                data_a[t][f"{date_key}_P"] = int(row['종가'])
                data_a[t][f"{date_key}_V"] = int(row['거래량'])
        except Exception as e:
            print(f"\n⚠️ {d_str} ETF 수집 건너뜀: {e}")

        # [CSV_E] 시장 ADR (KOSPI 기준)
        try:
            df_p = stock.get_market_ohlcv_by_ticker(d_str, market="KOSPI")
            ups = len(df_p[df_p['종가'] > df_p['시가']])
            downs = len(df_p[df_p['종가'] < df_p['시가']])
            # 분모가 0인 경우 처리
            adr_results[date_key] = round(ups / downs * 100, 2) if downs != 0 else 100.0
        except:
            adr_results[date_key] = 100.0
        
        time.sleep(0.05) # 서버 부하 방지

    print("\n💾 파일 저장 중...")

    # [CSV_A] 저장
    df_raw_a = pd.DataFrame(list(data_a.values()))
    df_raw_a.to_csv('CSV_A.csv', index=False, encoding='utf-8-sig')
    print("✅ CSV_A 완료")

    # [CSV_E] 저장
    pd.DataFrame([adr_results]).to_csv('CSV_E.csv', index=False, encoding='utf-8-sig')
    print("✅ CSV_E 완료")

    # [CSV_C] 글로벌 지표 수집 (나스닥, 환율, 필라델피아 반도체)
    try:
        tickers_c = ['^IXIC', 'KRW=X', '^SOX']
        df_c_raw = yf.download(tickers_c, start=tmp_days[0].strftime("%Y-%m-%d"), progress=False, ignore_tz=True)
        
        if not df_c_raw.empty:
            df_c = df_c_raw['Close'][tickers_c]
            df_c.T.to_csv('CSV_C.csv', encoding='utf-8-sig')
            print("✅ CSV_C 완료")
        else:
            print("⚠️ CSV_C 데이터가 비어있습니다.")
    except Exception as e:
        print(f"❌ CSV_C 수집 실패: {e}")

    print("🏁 모든 거시 데이터 세트(A, C, E)가 준비되었습니다.")

if __name__ == "__main__":
    run()
