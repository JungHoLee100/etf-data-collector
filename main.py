from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import google.generativeai as genai
import os
import json
import requests
from io import StringIO

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# [설정] Render Environment Variables 기반 로드
# ---------------------------------------------------------
GITHUB_USER = os.getenv("GITHUB_USER")
REPO_NAME = os.getenv("REPO_NAME")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"

def fetch_csv(filename):
    try:
        url = f"{BASE_URL}/{filename}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            df = pd.read_csv(StringIO(res.text), encoding='utf-8-sig')
            df.columns = df.columns.str.strip()
            return df.fillna("")
        return pd.DataFrame()
    except: return pd.DataFrame()

# ---------------------------------------------------------
# [API 1] 초기화: Final_Insight.csv 하나만 사용하여 모든 데이터 로드
# ---------------------------------------------------------
@app.get("/api/init")
async def init():
    # 통합 보따리 로드 (리더보드 데이터 + 매크로/심리 JSON 포함)
    df_final = fetch_csv("Final_Insight.csv")
    
    # 포트폴리오 로드
    try:
        url = f"{BASE_URL}/portfolio.json"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
        res = requests.get(url, headers=headers)
        portfolio = res.json() if res.status_code == 200 else {"holdings": []}
    except: portfolio = {"holdings": []}

    return {
        "static": {
            "A": df_final.to_dict(orient='records'),
            "C": [], "E": [] # Final_Insight 내부에 포함되어 있으므로 비움
        },
        "portfolio": portfolio
    }

# ---------------------------------------------------------
# [API 2] 분석: 선택된 모든 종목(n개)을 개별 분석
# ---------------------------------------------------------
@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    try:
        raw_info = await req.json()
        mode = raw_info.get("type", "SINGLE")
        
        # 티커 추출 및 6자리 표준화
        if mode == "PORTFOLIO":
            target_tickers = [str(h.get("code")).strip().zfill(6) for h in raw_info.get("portfolio", [])]
        else:
            code = str(raw_info.get("code") or "").strip().zfill(6)
            target_tickers = [code] if code != "000000" else []

        if not target_tickers:
            return {"analysis": "❌ 분석할 종목 코드를 확인할 수 없습니다."}

        # 데이터 매칭
        df_insight = fetch_csv("Final_Insight.csv")
        matched_rows = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(target_tickers)]
        
        if matched_rows.empty:
            return {"analysis": "❌ 선택한 종목의 데이터를 Final_Insight.csv에서 찾을 수 없습니다."}

        analysis_data = matched_rows.to_dict(orient='records')
        
        # Gemini 프롬프트: 모든 종목 개별 분석 및 양식 고정
        prompt = f"""
        당신은 정호님의 수석 퀀트 에널리스트입니다. 2026년 시장 상황을 반영하여 아래 모든 종목을 분석하세요.
        리스트에 포함된 **모든 종목({len(analysis_data)}개)**에 대해 각각 구체적인 진단을 내려야 합니다.

        [통합 데이터팩]
        {json.dumps(analysis_data, ensure_ascii=False)}

        [지시사항]
        1. 각 종목의 등급, Alpha 수치를 바탕으로 기세를 진단하고, 데이터 내 'macro_json', 'sentiment_json'을 해석하여 거시적 대응책을 제시하세요.
        2. 분석 대상 종목을 하나도 빠뜨리지 말고 모두 언급하세요.
        3. 답변 마지막에는 반드시 '추천종목:' 섹션을 만들고 중복 없이 '종목명(코드)' 형식으로만 나열하세요.
        """
        
        response = model.generate_content(prompt)
        return {"analysis": response.text}
    except Exception as e:
        return {"analysis": f"🚨 서버 오류: {str(e)}"}

@app.post("/api/portfolio/save")
async def save_portfolio(req: Request): return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
