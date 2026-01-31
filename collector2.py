import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime, timedelta
import os
import json

# --- [도움 함수 1] 추세 텍스트 계산 ---
def calculate_trend_text(prices):
    if len(prices) < 5: return "데이터부족"
    diffs = np.diff(prices)
    up_days = np.sum(diffs > 0)
    total_ret = (prices[-1] / prices[0] - 1) * 100
    if up_days >= len(diffs) * 0.8 and total_ret > 0: return "지속상승"
    elif up_days <= len(diffs) * 0.2 and total_ret < 0: return "지속하락"
    elif total_ret > 0: return "상승>하락"
    else: return "하락>상승"

# --- [도움 함수 2] 등급 및 점수 계산 ---
def get_score_and_grade(alpha_m, alpha_w, rvol, up_days_5d):
    grade = 'F'
    if alpha_m > 0 and alpha_w > 0: grade = 'S'
    elif alpha_m <= 0 and alpha_w > 0: grade = 'A'
    elif alpha_m > 0 and alpha_w <= 0: grade = 'B'
    
    s_p = 5 if alpha_m >= 10 else (4 if alpha_m >= 5 else (3 if alpha_m >= 3 else (2 if alpha_m >= 1 else (1 if alpha_m > 0 else 0))))
    s_v = 3 if (grade == 'B' and rvol < 80) or rvol >= 200 else (2 if rvol >= 120 else (1 if rvol >= 80 else 0))
    s_t = 2 if up_days_5d >= 4 else (1 if up_days_5d >= 2 else 0)
    return grade, int(s_p + s_v + s_t), s_p, s_v, s_t

# --- [도움 함수 3] 분석 설명 매핑 ---
def get_desc(grade, score):
    mapping = {
        'S': {8: "천하무적: 시장의 주도주", 4: "안정적 추세 유지 중", 0: "상승 탄력 둔화 주의"},
        'A': {8: "괴물 신인: 강력 수급 유입", 4: "추세 전환 시도 중", 0: "단기 반짝 가능성 체크"},
        'B': {8: "황금 눌림목: 최적 매수 타점", 4: "박스권 에너지 응축 중", 0: "추세 붕괴 위험 경계"},
        'F': {0: "시장 소외주: 관망 권고"}
    }
    target_grade = mapping.get(grade, mapping['F'])
    closest = min(target_grade.keys(), key=lambda x: abs(x - score))
    return target_grade[closest]

# --- [도움 함수 4] 지수 수익률 (이게 빠져서 에러가 났었습니다) ---
def get_safe_index_performance(start_date, end_date):
    indices = {"1028": "KOSPI200", "2034": "KOSDAQ150"}
    best_m, best_w = 0, 0
    for ticker in indices:
        try:
            df = stock.get_index_ohlcv_by_date(start_date, end_date, ticker)
            if not df.empty:
                m_ret = (df['종가'].iloc[-1] / df['종가'].iloc[0] - 1) * 100
                w_ret = (df['종가'].iloc[-1] / df['종가'].iloc[-6] - 1) * 100
                best_m = max(best_m, m_ret); best_w = max(best_w, w_ret)
        except: continue
    return best_m, best_w

# --- [메인 엔진] ---
def run():
    print("🧠 [S/A/B/F 분석 엔진] 가동 시작...")
    if not os.path.exists('CSV_A.csv'):
        print("❌ CSV_A.csv 파일이 없습니다."); return

    df_raw = pd.read_csv('CSV_A.csv')
    now = datetime.now()
    start_date = (now - timedelta(days=60)).strftime("%Y%m%d")
    end_date = now.strftime("%Y%m%d")
    
    # 벤치마크 수익률 확보
    b_1m_ret, b_1w_ret = get_safe_index_performance(start_date, end_date)
    print(f"📊 기준 수익률 설정 완료 (1M: {b_1m_ret:.2f}%)")

    price_cols = sorted([c for c in df_raw.columns if '_P' in c])
    vol_cols = sorted([c for c in df_raw.columns if '_V' in c])
    analysis_rows = []

    for _, row in df_raw.iterrows():
        try:
            prices = row[price_cols].values.astype(float)
            volumes = row[vol_cols].values.astype(float)
            p_curr = prices[-1]
            chg_1d = (prices[-1] / prices[-2] - 1) * 100
            ret_1w = (prices[-1] / prices[-6] - 1) * 100
            ret_1m = (prices[-1] / prices[0] - 1) * 100
            alpha_w, alpha_m = ret_1w - b_1w_ret, ret_1m - b_1m_ret
            rvol = (volumes[-1] / np.mean(volumes[-30:])) * 100
            up_days_5d = np.sum(np.diff(prices[-6:]) > 0)
            
            grade, total_s, s_p, s_v, s_t = get_score_and_grade(alpha_m, alpha_w, rvol, up_days_5d)
            if grade == 'F' and total_s < 5: continue

            analysis_rows.append({
                'ticker': str(row['ticker']).replace("'", "").zfill(6),
                'name': row['name'],
                'price_curr': int(p_curr),
                'change_1d': round(chg_1d, 2),
                'return_1w': round(ret_1w, 2),
                'alpha_1m': round(alpha_m, 2),
                'rvol': round(rvol, 1),
                'grade_score': f"{grade}{total_s}",
                'description': get_desc(grade, total_s)
            })
        except: continue

    # 1. 원본 분석 결과 저장
    result_df = pd.DataFrame(analysis_rows).sort_values(by=['alpha_1m'], ascending=False)
    result_df.to_csv('CSV_A_Analysis.csv', index=False, encoding='utf-8-sig')
    print("✅ CSV_A_Analysis 저장 완료")

    # 2. Final_Insight 통합 보따리 제작 (핵심)
    print("📦 Final_Insight 통합 작업 시작...")
    try:
        df_c = pd.read_csv('CSV_C.csv').tail(1)
        df_e = pd.read_csv('CSV_E.csv').tail(1)
        
        macro_json = json.dumps(df_c.to_dict(orient='records')[0], ensure_ascii=False) if not df_c.empty else "{}"
        sentiment_json = json.dumps(df_e.to_dict(orient='records')[0], ensure_ascii=False) if not df_e.empty else "{}"
        
        insight_df = result_df.copy()
        insight_df['macro_json'] = macro_json
        insight_df['sentiment_json'] = sentiment_json
        insight_df['combined_at'] = datetime.now().strftime("%Y-%m-%d %H:%M")

        insight_df.to_csv('Final_Insight.csv', index=False, encoding='utf-8-sig')
        print(f"🏁 Final_Insight.csv 생성 성공! ({len(insight_df)}개 종목)")
    except Exception as e:
        print(f"⚠️ 통합 중 오류: {e}")

if __name__ == "__main__":
    run()
