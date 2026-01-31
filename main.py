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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Render 환경 변수 로드
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

# [기능 수정] 포트폴리오를 GitHub에 실제로 저장하는 로직
@app.post("/api/portfolio/save")
async def save_portfolio(req: Request):
    try:
        data = await req.json()
        holdings = data.get("holdings", [])
        
        url = f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/portfolio.json"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # 1. 기존 파일의 SHA 값 가져오기
        res = requests.get(url, headers=headers)
        sha = res.json().get("sha") if res.status_code == 200 else None
        
        # 2. 데이터를 JSON 문자열로 변환 후 Base64 인코딩
        content_dict = {"holdings": holdings}
        content_json = json.dumps(content_dict, indent=2, ensure_ascii=False)
        encoded_content = base64.b64encode(content_json.encode("utf-8")).decode("utf-8")
        
        # 3. GitHub에 업데이트 요청 (PUT)
        payload = {
            "message": f"Portfolio updated by User at {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": encoded_content,
            "sha": sha
        }
        
        put_res = requests.put(url, headers=headers, json=payload)
        if put_res.status_code in [200, 201]:
            return {"status": "success"}
        else:
            return {"status": "error", "message": f"GitHub Error: {put_res.status_code}"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    try:
        raw_info = await req.json()
        mode = raw_info.get("type", "SINGLE")
        if mode == "PORTFOLIO":
            target_tickers = [str(h.get("code")).strip().zfill(6) for h in raw_info.get("portfolio", [])]
        else:
            target_tickers = [str(raw_info.get("code")).strip().zfill(6)]

        df_insight = fetch_csv("Final_Insight.csv")
        matched_rows = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(target_tickers)]
        if matched_rows.empty: return {"analysis": "데이터를 찾을 수 없습니다."}

        analysis_data = matched_rows.to_dict(orient='records')
        prompt = f"퀀트 전문가로서 다음 모든 종목을 개별 분석하고 전략을 제시하세요: {json.dumps(analysis_data, ensure_ascii=False)}\n\n추천종목은 마지막에 '종목명(코드)' 형식으로만 나열하세요."
        response = model.generate_content(prompt)
        return {"analysis": response.text}
    except Exception as e: return {"analysis": f"오류: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
