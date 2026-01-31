from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import google.generativeai as genai
import os
import json
import requests
from datetime import datetime

app = FastAPI()

# CORS 설정 (웹 연결 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# [설정] 본인의 정보에 맞게 수정하세요
# ---------------------------------------------------------
GITHUB_USER = "your-github-id" # 본인의 깃허브 아이디
REPO_NAME = "your-repo-name"   # 본인의 레포지토리 이름
GEMINI_API_KEY = "your-gemini-api-key"

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"

# ---------------------------------------------------------
# [유틸리티] 데이터 로드 함수
# ---------------------------------------------------------
def fetch_csv(filename):
    try:
        url = f"{BASE_URL}/{filename}"
        df = pd.read_csv(url, encoding='utf-8-sig')
        return df.fillna("")
    except:
        return pd.DataFrame()

# ---------------------------------------------------------
# [API 1] 초기 데이터 로드 (Leaderboard 및 포트폴리오용)
# ---------------------------------------------------------
@app.get("/api/init")
async def init():
    df_a = fetch_csv("CSV_A_Analysis.csv")
    df_c = fetch_csv("CSV_C.csv")
    df_e = fetch_csv("CSV_E.csv")
    
    # 포트폴리오 로드 (JSON 형식)
    try:
        res = requests.get(f"{BASE_URL}/portfolio.json")
        portfolio = res.json() if res.status_code == 200 else {"holdings": []}
    except:
        portfolio = {"holdings": []}

    return {
        "static": {
            "A": df_a.to_dict(orient='records'),
            "C": df_c.tail(5).to_dict(orient='records'),
            "E": df_e.tail(5).to_dict(orient='records')
        },
        "portfolio": portfolio
    }

# ---------------------------------------------------------
# [API 2] 포트폴리오 저장
# ---------------------------------------------------------
@app.post("/api/portfolio/save")
async def save_portfolio(req: Request):
    # 이 부분은 실제 파일 저장을 위해 GitHub API 연동이 필요할 수 있으나,
    # 현재는 요청을 정상 수신하는 구조로 유지합니다.
    data = await req.json()
    return {"status": "success", "received": len(data.get("holdings", []))}

# ---------------------------------------------------------
# [API 3] 통합 데이터팩 기반 딥러닝 분석 (핵심!)
# ---------------------------------------------------------
@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    try:
        raw_info = await req.json()
        mode = raw_info.get("type", "SINGLE")
        
        # 1. 분석할 티커 리스트 추출 (표준: 6자리 문자열)
        if mode == "PORTFOLIO":
            holdings = raw_info.get("portfolio", [])
            target_tickers = [str(h.get("code") or h.get("ticker")).strip().zfill(6) for h in holdings]
        else:
            code = str(raw_info.get("code") or raw_info.get("ticker")).strip().zfill(6)
            target_tickers = [code]

        # 2. 통합 데이터팩(Final_Insight.csv) 로드
        df_insight = fetch_csv("Final_Insight.csv")
        if df_insight.empty:
            return {"analysis": "❌ 통합 데이터팩(Final_Insight.csv)을 찾을 수 없습니다. collector2.py를 먼저 실행해주세요."}

        # 3. 데이터 매칭 (Golden Key: ticker)
        # Final_Insight 내의 ticker 컬럼과 요청받은 티커들을 비교합니다.
        matched_rows = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(target_tickers)]
        
        if matched_rows.empty:
            return {"analysis": f"❌ 선택하신 종목({', '.join(target_tickers)})의 분석 데이터가 존재하지 않습니다."}

        # 4. Gemini 전송용 데이터 보따리 구성
        analysis_data = matched_rows.to_dict(orient='records')

        # 5. Gemini 프롬프트 작성 (데이터 낭비 제로 전략)
        prompt = f"""
        당신은 정호님의 개인 수석 퀀트 에널리스트입니다.
        아래는 분석 대상 종목들에 대한 [통합 데이터팩]입니다. 
        이 데이터에는 각 종목의 퀀트 점수(A), 최신 시장 매크로(C), 시장 심리(E) 정보가 모두 포함되어 있습니다.

        [데이터 분석 팩]
        {json.dumps(analysis_data, ensure_ascii=False)}

        [지시사항]
        1. 각 종목별로 '등급'과 'Alpha' 수치를 언급하며 현재 위치를 진단하세요.
        2. 'macro_json'에 담긴 시장 상황(나스닥, 환율 등)이 이 종목들에게 어떤 영향을 줄지 설명하세요.
        3. 'sentiment_json'의 지표를 활용하여 지금이 공격적으로 매수할 때인지, 관망할 때인지 결론을 내주세요.
        4. 추천 종목은 반드시 '종목명(6자리코드)' 형식으로 3개 포함하세요.
        5. '데이터 없음'이라는 말은 절대 하지 말고, 제공된 통합 정보를 바탕으로 가장 전문적인 전략을 제시하세요.
        """

        # 6. Gemini 실행
        response = model.generate_content(prompt)
        return {"analysis": response.text}

    except Exception as e:
        return {"analysis": f"🚨 분석 엔진 오류: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
