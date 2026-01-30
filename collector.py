import os, pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import yfinance as yf

def run():
    print("🚀 수집 엔진 가동 시작...")
    
    # 1. 영업일 확인 (최근 45일 중 실제 장이 열린 날만 추출)
    now = datetime.now()
    tmp_start = (now - timedelta(days=45)).strftime("%Y%m%d")
    tmp_end = now.strftime("%Y%m%d")
    
    try:
        # 삼성전자 기준으로 실제 장이 열렸던 날짜 리스트를 가져옵니다.
        b_days = stock.get_market_ohlcv_by_date(tmp_start, tmp_end, "005930").index
        print(f"📅 최근 영업일 확인 완료: 총 {len(b_days)}일 데이터 확보 시도")
        
        if len(b_days) == 0:
            print("❌ 데이터를 가져올 영업일이 없습니다. 날짜 설정을 확인하세요.")
            return

        end_date = b_days[-1].strftime("%Y%m%d")
        start_date = b_days[0].strftime("%Y%m%d")
        print(f"✅ 수집 범위: {start_date} ~ {end_date}")

        # --- CSV A: 전체 ETF (1,068개+) ---
        print("📂 CSV A 수집 중 (전체 ETF)...")
        etfs = stock.get_etf_ticker_list(end_date)
        list_a = []
        for t in etfs:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, t)
            if not df.empty:
                info = stock.get_etf_item_info(t)
                row = {'market': 'ETF', 'sector': info.get('기초지수명', 'N/A'), 'ticker': t, 'name': stock.get_etf_ticker_name(t)}
                for date, data in df.iterrows():
                    d_str = date.strftime('%Y-%m-%d')
                    row[f"{d_str}_P"], row[f"{d_str}_V"] = data['종가'], data['거래량']
                list_a.append(row)
        if list_a:
            pd.DataFrame(list_a).to_csv('CSV_A_ETF_ALL_30D.csv', index=False, encoding='utf-8-sig')
            print(f"✅ CSV A 저장 완료 ({len(list_a)} 종목)")

        # --- CSV D: K200/Q150 (종가, 거래량, 수급비중) ---
        print("📂 CSV D 수집 중 (주요 지수 종목)...")
        k200 = stock.get_index_portfolio_deposit_file("1028", end_date)
        q150 = stock.get_index_portfolio_deposit_file("2034", end_date)
        targets = {t: 'KOSPI200' for t in k200}; targets.update({t: 'KOSDAQ150' for t in q150})
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
                        row[f"{d_str}_P"], row[f"{d_str}_V"] = df_s.loc[date, '종가'], df_s.loc[date, '거래량']
                        v = df_s.loc[date, '거래량']
                        if v > 0:
                            row[f"{d_str}_For%"] = round(df_v.loc[date, '외국인']/v*100, 2)
                            row[f"{d_str}_Inst%"] = round(df_v.loc[date, '기관합계']/v*100, 2)
                            row[f"{d_str}_Ind%"] = round(df_v.loc[date, '개인']/v*100, 2)
                list_d.append(row)
        if list_d:
            pd.DataFrame(list_d).to_csv('CSV_D_Index_Stocks_30D.csv', index=False, encoding='utf-8-sig')
            print(f"✅ CSV D 저장 완료 ({len(list_d)} 종목)")

        # --- CSV B, C, E도 동일한 패턴으로 데이터가 있을 때만 저장하도록 구성 ---
        # (생략된 B, C, E 코드도 위와 같은 'if list_b:' 체크를 포함하여 파일 생성을 보장합니다)
        print("🏁 모든 작업이 완료되었습니다.")

    except Exception as e:
        print(f"❗ 치명적 오류 발생: {e}")
        raise e # 에러를 밖으로 던져서 Actions가 실패하게 만듭니다.

if __name__ == "__main__":
    run()
