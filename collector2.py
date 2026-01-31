import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime, timedelta
import os
import json  # 통합을 위해 추가

# ... (기존 calculate_trend_text, get_score_and_grade, get_desc, get_safe_index_performance 함수는 동일) ...

def run():
    print("🧠 [S/A/B/F 분석 엔진] 가동 시작...")
    if not os.path.exists('CSV_A.csv'):
        print("❌ CSV_A.csv 파일이 없습니다.")
        return

    df_raw = pd.read_csv('CSV_A.csv')
    now = datetime.now()
    start_date = (now - timedelta(days=60)).strftime("%Y%m%d")
    end_date = now.strftime("%Y%m%d")
    
    b_1m_ret, b_1w_ret = get_safe_index_performance(start_date, end_date)
    
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
            
            alpha_w = ret_1w - b_1w_ret
            alpha_m = ret_1m - b_1m_ret
            
            rvol = (volumes[-1] / np.mean(volumes[-30:])) * 100
            vol_status = "폭발" if rvol >= 200 else ("유입" if rvol >= 120 else "유지")
            up_days_5d = np.sum(np.diff(prices[-6:]) > 0)
            
            grade, total_s, s_p, s_v, s_t = get_score_and_grade(alpha_m, alpha_w, rvol, up_days_5d)
            
            if grade == 'F' and total_s < 5: continue

            analysis_rows.append({
                # [표준화 1] 티커에서 따옴표 제거 및 6자리 고정
                'ticker': str(row['ticker']).replace("'", "").zfill(6),
                'name': row['name'],
                'price_curr': int(p_curr),
                'change_1d': round(chg_1d, 2),
                'alpha_1m': round(alpha_m, 2),
                'rvol': round(rvol, 1),
                'grade_score': f"{grade}{total_s}",
                'description': get_desc(grade, total_s)
            })
        except: continue

    # 1. 기존 분석 결과 저장 (리더보드용)
    result_df = pd.DataFrame(analysis_rows).sort_values(by=['alpha_1m'], ascending=False)
    result_df.to_csv('CSV_A_Analysis.csv', index=False, encoding='utf-8-sig')
    print(f"✅ CSV_A_Analysis.csv 저장 완료.")

    # ---------------------------------------------------------
    # 2. [신규] Final_Insight.csv 통합 보따리 제작 (Gemini용)
    # ---------------------------------------------------------
    print("📦 데이터 통합(Final_Insight) 시작...")
    try:
        # 매크로(C)와 심리(E) 파일 읽기
        df_c = pd.read_csv('CSV_C.csv').tail(1) # 가장 최신 매크로 1줄
        df_e = pd.read_csv('CSV_E.csv').tail(1) # 가장 최신 심리 1줄
        
        # 데이터를 텍스트 형태로 변환 (Gemini가 읽기 쉽게)
        macro_context = df_c.to_dict(orient='records')[0] if not df_c.empty else "매크로 데이터 부재"
        sentiment_context = df_e.to_dict(orient='records')[0] if not df_e.empty else "심리 데이터 부재"
        
        # 분석 결과에 매크로/심리 정보를 '컬럼'으로 추가
        # 모든 종목이 동일한 시장 상황(C, E)을 공유하도록 구성합니다.
        insight_df = result_df.copy()
        insight_df['macro_json'] = json.dumps(macro_context, ensure_ascii=False)
        insight_df['sentiment_json'] = json.dumps(sentiment_context, ensure_ascii=False)
        insight_df['combined_at'] = datetime.now().strftime("%Y-%m-%d %H:%M")

        # 최종 통합 파일 저장
        insight_df.to_csv('Final_Insight.csv', index=False, encoding='utf-8-sig')
        print(f"🏁 Final_Insight.csv 생성 완료! 숨바꼭질 끝.")
        
    except Exception as e:
        print(f"⚠️ 통합 과정 중 오류 발생 (하지만 원본은 안전함): {e}")

if __name__ == "__main__":
    run()
