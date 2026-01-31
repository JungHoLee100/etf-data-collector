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
        # 내 포트폴리오와 리더보드 선택 종목 분리 수신
        p_items = raw_info.get("portfolio_items", [])
        l_items = raw_info.get("leaderboard_items", [])
        
        all_tickers = [str(i['code']).zfill(6) for i in (p_items + l_items)]
        
        df_insight = fetch_csv("Final_Insight.csv")
        matched_data = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(all_tickers)].to_dict(orient='records')

        prompt = f"""
        당신은 정호님의 수석 퀀트 에널리스트입니다. 
        두 그룹의 종목들을 비교 분석하여 어느 쪽이 현재 시장 상황에서 더 우월한지 판정하세요.

        [그룹 1: 내 자산 포트폴리오]
        {json.dumps(p_items, ensure_ascii=False)}

        [그룹 2: 리더보드 선택 종목]
        {json.dumps(l_items, ensure_ascii=False)}

        [상세 데이터팩]
        {json.dumps(matched_data, ensure_ascii=False)}

        [분석 지침]
        1. 내 포트폴리오 종목들의 현재 등급과 수익률을 진단하세요.
        2. 리더보드에서 선택된 종목들이 내 포트폴리오보다 어떤 점에서 우위에 있는지(Alpha, RVOL 등) 비교하세요.
        3. 매크로(C)와 심리(E) 지표를 바탕으로 '종목 교체'가 필요한지 결론을 내주세요.
        4. 마지막에 '추천종목:' 섹션에 가장 유망한 3개를 '종목명(코드)' 형식으로 기재하세요.
        """
        response = model.generate_content(prompt)
        return {"analysis": response.text}
    except Exception as e: return {"analysis": f"오류: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
