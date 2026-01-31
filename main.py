import pandas as pd
import os, json, requests, datetime
from fastapi import FastAPI, Request
from google import genai
from pykrx import stock
import yfinance as yf

# ... (기존 FastAPI 설정 및 GITHUB_TOKEN 설정 유지) ...

@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    info = await req.json()
    code = str(info.get("code"))
    name = info.get("name")
    
    # --- [데이터 수집 1] CSV A, C, E 통합 로드 ---
    base_url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"
    context_data = {}
    try:
        # CSV_A_Analysis (핵심 모델 지표)
        df_a = pd.read_csv(f"{base_url}/CSV_A_Analysis.csv", encoding='utf-8-sig')
        target_a = df_a[df_a['ticker'] == code].to_dict(orient='records')
        
        # CSV_C (시장/매크로 필터)
        df_c = pd.read_csv(f"{base_url}/CSV_C.csv", encoding='utf-8-sig')
        target_c = df_c.head(5).to_dict(orient='records') # 시장 전반적인 필터링 기준 전달
        
        # CSV_E (상세 가점/감점 요인)
        df_e = pd.read_csv(f"{base_url}/CSV_E.csv", encoding='utf-8-sig')
        target_e = df_e[df_e['ticker'] == code].to_dict(orient='records')
        
        context_data = {"A_Analysis": target_a, "C_Macro": target_c, "E_Details": target_e}
    except Exception as e:
        context_data = {"error": f"CSV 로드 실패: {str(e)}"}

    # --- [데이터 수집 2] 실시간 B (선물/옵션 외국인/기관 수급) ---
    try:
        today = datetime.datetime.now().strftime("%Y%m%d")
        # KOSPI 200 선물/옵션 수급을 대변하는 투자자별 순매수 데이터
        df_b = stock.get_market_net_purchases_of_equities(today, today, "KOSPI")
        # 외인, 기관의 합계 정보만 추출
        b_summary = df_b.loc[['외국인', '기관합계'], ['매수거래대금', '매도거래대금', '순매수거래대금']].to_dict()
    except:
        b_summary = "파생/시장 수급 데이터 조회 불가"

    # --- [데이터 수집 3] 실시간 D (개별 종목 기술적 지표 - 30일) ---
    try:
        ticker_yf = yf.Ticker(f"{code}.KS" if not code.startswith('k') else f"{code}.KQ")
        hist = ticker_yf.history(period="30d")
        # 5일/20일 이평선 및 현재 상태
        ma5 = hist['Close'].rolling(5).mean().iloc[-1]
        ma20 = hist['Close'].rolling(20).mean().iloc[-1]
        curr_price = hist['Close'].iloc[-1]
        
        # 재무 (PER, PBR, PEG)
        yf_info = ticker_yf.info
        d_summary = {
            "current_price": curr_price,
            "MA5": round(ma5, 2),
            "MA20": round(ma20, 2),
            "MA_Status": "Golden Cross" if ma5 > ma20 else "Dead Cross",
            "PER": yf_info.get("trailingPE"),
            "PBR": yf_info.get("priceToBook"),
            "PEG": yf_info.get("pegRatio")
        }
    except:
        d_summary = "개별 주가/재무 데이터 조회 불가"

    # --- [Gemini 종합 추론 실행] ---
    prompt = f"""
    당신은 대한민국 ETF 투자 전략의 최고 권위자입니다. 아래 제공된 [데이터 패키지]를 융합하여 {name}({code})에 대한 심층 분석 보고서를 작성하세요.

    [1. 모델 데이터 (A, C, E)]:
    {json.dumps(context_data, ensure_ascii=False, indent=2)}

    [2. 실시간 시장 수급 (B - 외국인/기관 파생 및 현물 흐름)]:
    {json.dumps(b_summary, ensure_ascii=False, indent=2)}

    [3. 개별 주가 및 재무 지표 (D)]:
    {json.dumps(d_summary, ensure_ascii=False, indent=2)}

    [분석 미션]:
    1. CSV_A의 등급과 CSV_E의 가점 요인을 B(수급) 및 D(기술적 이평선)와 대조하여 현재 시점의 '최종 투자 등급'을 재산정하세요.
    2. 파생 시장에서 외국인/기관의 순매수 기조가 이 종목에 미칠 단기 영향을 분석하세요.
    3. '추가매수/유지/일부매도/관망' 중 하나를 결론으로 제시하고 구체적 수치를 인용해 이유를 적으세요.
    4. CSV_A_Analysis 리스트 중 동일 섹터에서 등급이 더 높은 '대안 종목' 2개를 추천하세요. (종목명(코드) 형식 엄수)

    *주의: "정보가 부족하다"는 답변은 금지하며, 제공된 데이터 간의 '상관관계'를 찾아내는 데 집중하세요.*
    """
    
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    
    # 분석 로그 저장 (GitHub)
    # ... (기존 로그 저장 로직 유지) ...

    return {"analysis": response.text}
