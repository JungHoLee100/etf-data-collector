import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime, timedelta
import os

def calculate_trend_text(prices):
    if len(prices) < 5: return "데이터부족"
    diffs = np.diff(prices)
    up_days = np.sum(diffs > 0)
    total_ret = (prices[-1] / prices[0] - 1) * 100
    
    if up_days >= len(diffs) * 0.8 and total_ret > 0: return "지속상승"
    elif up_days <= len(diffs) * 0.2 and total_ret < 0: return "지속하락"
    elif total_ret > 0: return "상승>하락"
    else: return "하락>상승"

def get_score_and_grade(alpha_m, alpha_w, rvol, up_days_5d):
    # 1. 등급 판정 (Dual-Window)
    grade = 'F'
    if alpha_m > 0 and alpha_w > 0: grade = 'S'
    elif alpha_m <= 0 and alpha_w > 0: grade = 'A'
    elif alpha_m > 0 and alpha_w <= 0: grade = 'B'
    
    # 2. 세부 점수 계산 (5:3:2 가중치)
    # P (Price Alpha): 5점 만점
    if alpha_m >= 10: s_p = 5
    elif alpha_m >= 5: s_p = 4
    elif alpha_m >= 3: s_p = 3
    elif alpha_m >= 1: s_p = 2
    elif alpha_m > 0: s_p = 1
    else: s_p = 0
    
    # V (Volume Energy): 3점 만점
    if grade == 'B' and rvol < 80: # B등급 특수 로직: 조정 시 거래량 적을수록 황금눌림목
        s_v = 3
    elif rvol >= 200: s_v = 3
    elif rvol >= 120: s_v = 2
    elif rvol >= 80: s_v = 1
    else: s_v = 0
    
    # T (Trend Consistency): 2점 만점 (5일 중 상승 일수)
    s_t = 2 if up_days_5d >= 4 else (1 if up_days_5d >= 2 else 0)
    
    total_score = s_p + s_v + s_t
    return grade, int(total_score), s_p, s_v, s_t

def get_desc(grade, score):
    mapping = {
        'S': {8: "천하무적: 시장의 주도주", 4: "안정적 추세 유지 중", 0: "상승 탄력 둔화 주의"},
        'A': {8: "괴물 신인: 강력 수급 유입", 4: "추세 전환 시도 중", 0: "단기 반짝 가능성 체크"},
        'B': {8: "황금 눌림목: 최적 매수 타점", 4: "박스권 에너지 응축 중", 0: "추세 붕괴 위험 경계"},
        'F': {0: "시장 소외주: 관망 권고"}
    }
    closest = min(mapping[grade].keys(), key=lambda x: abs(x - score))
    return mapping[grade][closest]

def run():
    print("🧠 [S/A/B/F 분석 엔진] 2차 가공을 시작합니다...")
    if not os.path.exists('CSV_A.csv'):
        print("❌ 원천 데이터(CSV_A.csv)가 없습니다.")
        return

    df_raw = pd.read_csv('CSV_A.csv')
    
    # 지수 벤치마크 데이터 준비
    now = datetime.now()
    start_date = (now - timedelta(days=60)).strftime("%Y%m%d")
    end_date = now.strftime("%Y%m%d")
    
    k200 = stock.get_index_ohlcv_by_date(start_date, end_date, "1028")['종가']
    k150 = stock.get_index_ohlcv_by_date(start_date, end_date, "2034")['종가']
    
    b_1m_ret = max((k200.iloc[-1]/k200.iloc[0]-1)*100, (k150.iloc[-1]/k150.iloc[0]-1)*100)
    b_1w_ret = max((k200.iloc[-1]/k200.iloc[-6]-1)*100, (k150.iloc[-1]/k150.iloc[-6]-1)*100)

    price_cols = sorted([c for c in df_raw.columns if '_P' in c])
    vol_cols = sorted([c for c in df_raw.columns if '_V' in c])

    analysis_rows = []

    for _, row in df_raw.iterrows():
        prices = row[price_cols].values.astype(float)
        volumes = row[vol_cols].values.astype(float)
        
        # 기초 지표 계산
        p_curr = prices[-1]
        chg_1d = (prices[-1] / prices[-2] - 1) * 100
        ret_1w = (prices[-1] / prices[-6] - 1) * 100
        ret_1m = (prices[-1] / prices[0] - 1) * 100
        
        alpha_w = ret_1w - b_1w_ret
        alpha_m = ret_1m - b_1m_ret
        
        # 에너지 및 추세
        rvol = (volumes[-1] / np.mean(volumes[-30:])) * 100
        vol_status = "폭발" if rvol >= 200 else ("유입" if rvol >= 120 else "유지")
        up_days_5d = np.sum(np.diff(prices[-6:]) > 0)
        
        # 등급 및 점수 산출
        grade, total_s, s_p, s_v, s_t = get_score_and_grade(alpha_m, alpha_w, rvol, up_days_5d)
        
        if grade == 'F' and total_s < 5: continue # 소외주 중 약세는 제외

        analysis_rows.append({
            'ticker': f"'{row['ticker']}'", # 엑셀 0 누락 방지
            'name': row['name'],
            'price_curr': int(p_curr),
            'change_1d': round(chg_1d, 2),
            'return_1w': round(ret_1w, 2),
            'return_1m': round(ret_1m, 2),
            'alpha_1w': round(alpha_w, 2),
            'alpha_1m': round(alpha_m, 2),
            'rvol': round(rvol, 1),
            'vol_status': vol_status,
            'trend_1w': calculate_trend_text(prices[-6:]),
            'trend_1m': calculate_trend_text(prices),
            'up_days_5d': up_days_5d,
            'score_p': s_p, 'score_v': s_v, 'score_t': s_t,
            'total_score': total_s,
            'grade_score': f"{grade}{total_s}",
            'description': get_desc(grade, total_s)
        })

    result_df = pd.DataFrame(analysis_rows).sort_values(by=['total_score', 'alpha_1m'], ascending=False)
    result_df.to_csv('CSV_A_Analysis.csv', index=False, encoding='utf-8-sig')
    print(f"🏁 분석 완료! {len(result_df)}개 종목이 성적표에 등록되었습니다.")

if __name__ == "__main__":
    run()
