from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import google.generativeai as genai
import os
import json
import requests
from io import StringIO

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
    df_a = fetch_csv("CSV_A_Analysis.csv")
    df_c = fetch_csv("CSV_C.csv")
    df_e = fetch_csv("CSV_E.csv")
    return {"static": {"A": df_a.to_dict(orient='records'), "C": df_c.tail(5).to_dict(orient='records'), "E": df_e.tail(5).to_dict(orient='records')}, "portfolio": {"holdings": []}}

@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    try:
        raw_info = await req.json()
        mode = raw_info.get("type", "SINGLE")
        if mode == "PORTFOLIO":
            target_tickers = [str(h.get("code") or h.get("ticker")).strip().zfill(6) for h in raw_info.get("portfolio", [])]
        else:
            code = str(raw_info.get("code") or raw_info.get("ticker") or "").strip().zfill(6)
            target_tickers = [code] if code != "000000" else []

        df_insight = fetch_csv("Final_Insight.csv")
        matched_rows = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(target_tickers)]
        
        if matched_rows.empty: return {"analysis": "데이터를 찾을 수 없습니다."}

        analysis_data = matched_rows.to_dict(orient='records')
        prompt = f"퀀트 전문가로서 다음 모든 종목을 개별 분석하고 전략을 제시하세요: {json.dumps(analysis_data, ensure_ascii=False)}\n\n추천 종목은 마지막에 '종목명(코드)' 형식으로만 나열하세요."
        
        response = model.generate_content(prompt)
        return {"analysis": response.text}
    except Exception as e: return {"analysis": f"오류: {str(e)}"}

@app.post("/api/portfolio/save")
async def save_portfolio(req: Request): return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
