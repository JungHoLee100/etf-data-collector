from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import google.generativeai as genai
import os
import json
import requests
import base64
from io import StringIO
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 환경 변수 로드
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

@app.get("/api/init")
async def init():
    df_final = fetch_csv("Final_Insight.csv")
    try:
        url = f"{BASE_URL}/portfolio.json"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
        res = requests.get(url, headers=headers)
        portfolio = res.json() if res.status_code == 200 else {"holdings": []}
    except: portfolio = {"holdings": []}
    return {"static": {"A": df_final.to_dict(orient='records')}, "portfolio": portfolio}

@app.post("/api/portfolio/save")
async def save_portfolio(req: Request):
    try:
        data = await req.json()
        holdings = data.get("holdings", [])
        url = f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/portfolio.json"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        res = requests.get(url, headers=headers)
        sha = res.json().get("sha") if res.status_code == 200 else None
        content_json = json.dumps({"holdings": holdings}, indent=2, ensure_ascii=False)
        encoded_content = base64.b64encode(content_json.encode("utf-8")).decode("utf-8")
        payload = {"message": "Update Portfolio", "content": encoded_content, "sha": sha}
        requests.put(url, headers=headers, json=payload)
        return {"status": "success"}
    except Exception as e: return {"status": "error", "message": str(e)}

@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    try:
        raw_info = await req.json()
        p_items = raw_info.get("portfolio_items", [])
        l_items = raw_info.get("leaderboard_items", [])
        all_tickers = [str(i['code']).zfill(6) for i in (p_items + l_items)]
        
        df_insight = fetch_csv("Final_Insight.csv")
        matched_data = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(all_tickers)].to_dict(orient='records')

        prompt = f"""
        당신은 정호님의 수석 퀀트 에널리스트입니다. 
        선택된 종목들을 정밀 분석하여 최적의 투자 전략을 제시하세요.

        [분석 대상 데이터]
        {json.dumps(matched_data, ensure_ascii=False)}

        [지시사항]
        1. 각 종목의 등급(S10 등), Alpha, RVOL을 기반으로 현재 기세를 분석하세요.
        2. macro_json과 sentiment_json을 통해 시장 환경에 따른 대응 전략을 짜세요.
        3. 답변 마지막에는 반드시 '추천종목:' 섹션을 만들고 '종목명(코드)' 형식으로만 나열하세요.
        """
        response = model.generate_content(prompt)
        return {"analysis": response.text}
    except Exception as e: return {"analysis": f"오류: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
