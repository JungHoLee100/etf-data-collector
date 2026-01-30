import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime, timedelta

def calculate_score(alpha, rvol, trend_days, grade):
    # 1. Alpha 점수 (5점 만점)
    s_alpha = min(5, max(1, alpha / 2)) if alpha > 0 else 0
    
    # 2. 거래량 에너지 (3점 만점)
    s_vol = 3 if rvol >= 200 else (2 if rvol >= 120 else (1 if rvol >= 80 else 0))
    
    # 3. 추세 일관성 (2점 만점)
    s_trend = 2 if trend_days >= 4 else (1 if trend_days >= 2 else 0)
    
    # B등급(눌림목) 특수 로직: 조정 시 거래량 적을수록 가점 (역발상)
    if grade == 'B' and rvol < 80:
        s_vol = 3 

    return int(min(10, s_alpha + s_vol + s_trend))

def get_description(grade, score):
    descriptions = {
        'S': {10: "천하무적: 시장의 왕", 5: "추세 유지 중인 대장주", 1: "탄력이 둔화되는 대장주"},
        'A': {10: "괴물 신인: 강력한 수급 유입", 5: "추세 전환 시도 중", 1: "단기 반짝 가능성 주의"},
        'B': {10: "황금 눌림목: 완벽한 매수 타점", 5: "박스권 조정 중", 1: "추세 붕괴 위험 경계"},
        'F': {1: "관심 제외: 시장 소외주"}
    }
    # 점수에 가장 가까운 설명 반환
    closest_score = min(descriptions[grade].keys(), key=lambda x: abs(x - score))
    return descriptions[grade][closest_score]

def run_analysis():
    print("🧠 [분석 엔진 가동] CSV_A를 기반으로 S/A/B/F 10단계 분석을 시작합니다...")
    
    # 데이터 로드
    try:
        df = pd.read_csv('CSV_A.csv')
    except:
        print("❌ CSV_A.csv 파일이 없습니다. collector.py를 먼저 실행하세요.")
        return

    # 지수 데이터 확보 (Alpha 계산용)
    now = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
    k200 = stock.get_index_ohlcv_by_date(start_date, now, "1028")['종가']
    k200_1m_ret = (k200.iloc[-1] / k200.iloc[0] - 1) * 100
    k200_1w_ret = (k200.iloc[-1] / k200.iloc[-6] - 1) * 100

    analysis_results = []
    
    # 컬럼에서 날짜 리스트 추출 (가격_P, 거래량_V 패턴)
    price_cols = sorted([c for c in df.columns if '_P' in c])
    vol_cols = sorted([c for c in df.columns if '_V' in c])

    for _, row in df.iterrows():
        prices = row[price_cols].values.astype(float)
        vols = row[vol_cols].values.astype(float)
        
        # 수익률 계산
        ret_1m = (prices[-1] / prices[0] - 1) * 100
        ret_1w = (prices[-1] / prices[-6] - 1) * 100
        
        # Alpha 계산 (K200 기준)
        alpha_1m = ret_1m - k200_1m_ret
        alpha_1w = ret_1w - k200_1w_ret
        
        # 등급 부여 (Dual-Window)
        grade = 'F'
        if alpha_1m > 0 and alpha_1w > 0: grade = 'S'
        elif alpha_1m <= 0 and alpha_1w > 0: grade = 'A'
        elif alpha_1m > 0 and alpha_1w <= 0: grade = 'B'
        
        if grade == 'F': continue # 소외주는 분석 제외

        # 점수 산출 데이터
        rvol = (vols[-1] / (np.mean(vols) if np.mean(vols) > 0 else 1)) * 100
        up_days = sum(1 for i in range(1, 6) if prices[-i] > prices[-i-1])
        
        score = calculate_score(alpha_1m, rvol, up_days, grade)
        
        analysis_results.append({
            'ticker': row['ticker'],
            'name': row['name'],
            'grade_score': f"{grade}{score}",
            'alpha_1m': round(alpha_1m, 2),
            'rvol': round(rvol, 1),
            'description': get_description(grade, score)
        })

    # 결과 저장
    result_df = pd.DataFrame(analysis_results).sort_values(by='alpha_1m', ascending=False)
    result_df.to_csv('CSV_A_Analysis.csv', index=False, encoding='utf-8-sig')
    print(f"🏁 분석 완료! {len(result_df)}개의 유효 종목이 'CSV_A_Analysis.csv'에 저장되었습니다.")

if __name__ == "__main__":
    run_analysis()
