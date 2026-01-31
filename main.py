from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import google.generativeai as genai
import os
import json
import requests
from io import StringIO

app = FastAPI()

# CORS 설정: 프론트엔드 접속 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# [설정] Render의 Environment Variables에서 자동으로 가져옵니다.
# ---------------------------------------------------------
GITHUB_USER = os.getenv("GITHUB_USER", "your-github-id")
REPO_NAME = os.getenv("REPO_NAME", "your-repo-name")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Gemini AI 모델 설정
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash")

BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"

# ---------------------------------------------------------
# [유틸리티] 보안 강화된 데이터 호출 함수 (Token 사용)
# ---------------------------------------------------------
def fetch_csv(filename):
    try:
        url = f"{BASE_URL}/{filename}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
        res = requests.get(url, headers=headers)
        
        if res.status_code == 200:
            df = pd.read_csv(StringIO(res.text), encoding='utf-8-sig')
            df.columns = df.columns.str.strip() # 컬럼명 공백 제거
            return df.fillna("")
        else:
            print(f"❌ 파일 로드 실패: {filename} (Status: {res.status_code})")
            return pd.DataFrame()
    except Exception as e:
        print(f"🚨 데이터 호출 에러: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# [API] 초기화 데이터 (리더보드 및 포트폴리오)
# ---------------------------------------------------------
@app.get("/api/init")
async def init():
    df_a = fetch_csv("CSV_A_Analysis.csv")
    df_c = fetch_csv("CSV_C.csv")
    df_e = fetch_csv("CSV_E.csv")
    
    # 포트폴리오 파일 로드
    try:
        url = f"{BASE_URL}/portfolio.json"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
        res = requests.get(url, headers=headers)
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
# [API] 통합 데이터팩 기반 딥러닝 분석
# ---------------------------------------------------------
@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    try:
        raw_info = await req.json()
        mode = raw_info.get("type", "SINGLE")
        
        # 1. 티커 추출 및 6자리 표준화
        if mode == "PORTFOLIO":
            holdings = raw_info.get("portfolio", [])
            target_tickers = [str(h.get("code") or h.get("ticker")).strip().zfill(6) for h in holdings]
        else:
            code = str(raw_info.get("code") or raw_info.get("ticker") or "").strip().zfill(6)
            target_tickers = [code] if code != "000000" else []

        if not target_tickers:
            return {"analysis": "❌ 분석할 종목 코드를 확인할 수 없습니다."}

        # 2. 통합 데이터팩(Final_Insight.csv) 로드
        df_insight = fetch_csv("Final_Insight.csv")
        if df_insight.empty:
            return {"analysis": "❌ 통합 데이터팩을 찾을 수 없습니다. collector2.py를 먼저 실행해 주세요."}

        # 3. 데이터 매칭 (Golden Key: ticker)
        matched_rows = df_insight[df_insight['ticker'].astype(str).str.zfill(6).isin(target_tickers)]
        
        if matched_rows.empty:
            return {"analysis": f"❌ 선택하신 종목({', '.join(target_tickers)})의 통합 데이터를 찾지 못했습니다."}

        # 4. Gemini 전송용 데이터 구성
        analysis_data = matched_rows.to_dict(orient='records')

        # [main.py의 deep_analyze 함수 내 프롬프트 부분 수정]

        prompt = f"""
        당신은 정호님의 수석 퀀트 에널리스트입니다. 
        대상 종목들({target_tickers}) 전체에 대해 개별적으로 정밀 분석을 수행하세요.

        [분석 데이터]
        {json.dumps(analysis_data, ensure_ascii=False)}

        [지시사항]
        1. 분석 대상 리스트에 있는 **모든 종목**을 하나도 빠뜨리지 말고 각각 분석하세요. (종목수가 많아도 요약하지 마세요)
        2. 각 종목의 등급, Alpha, 그리고 macro_json/sentiment_json과의 상관관계를 설명하세요.
        3. **추천 종목 섹션**은 반드시 답변 마지막에 별도로 작성하되, 오직 '종목명(코드)' 형식만 사용하세요. 
           예: 삼성전자(005930), TIGER 반도체TOP10(396500)
        4. 추천 종목 리스트에는 중복이 없어야 하며, 분석 결과 중 가장 유망한 3개를 선정하세요.
        5. '제공해주신 데이터팩' 같은 불필요한 서두는 생략하고 바로 본론으로 들어가세요.
        """

        response = model.generate_content(prompt)
        return {"analysis": response.text}

    except Exception as e:
        return {"analysis": f"🚨 분석 중 시스템 오류: {str(e)}"}

@app.post("/api/portfolio/save")
async def save_portfolio(req: Request):
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
