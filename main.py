import pandas as pd
import os, json, ssl, requests
from io import StringIO
from google import genai
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# SSL 및 환경변수 설정
ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- 🌐 GitHub 데이터 경로 ---
GITHUB_USER = "JungHoLee100"
REPO_NAME = "etf-data-collector"
RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main/CSV_A_Analysis.csv"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

@app.get("/api/analyze/latest")
def get_latest_analysis():
    try:
        # 💡 [가장 확실한 방법] 파일을 먼저 다운로드한 후, 직접 인코딩을 지정해 읽습니다.
        response = requests.get(RAW_URL)
        response.encoding = 'utf-8' # 한글 강제 지정
        
        # 다운로드된 텍스트 데이터를 pandas로 읽기
        df = pd.read_csv(StringIO(response.text))
        
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str).str.replace("'", "")
            
        return {
            "source": "GitHub Cloud Storage (UTF-8 Verified)",
            "data": df.fillna(0).to_dict(orient="records")
        }
    except Exception as e:
        # 에러 발생 시 로그를 명확히 남깁니다.
        return {"error": f"데이터 로드 실패: {str(e)}"}

@app.post("/api/ai-strategy")
def get_ai_strategy(data: dict):
    target_stock = data.get("stock_info")
    prompt = f"""
    당신은 대한민국 최고의 ETF 퀀트 애널리스트입니다. 아래 데이터를 바탕으로 투자 전략을 수립하세요.
    
    [종목 정보]
    - 종목명: {target_stock.get('name')} ({target_stock.get('ticker')})
    - 현재 등급: {target_stock.get('grade_score')} ({target_stock.get('description')})
    - 1달 초과수익(Alpha): {target_stock.get('alpha_1m')}%
    - 거래량 에너지(RVOL): {target_stock.get('rvol')}%
    - 1주 추세: {target_stock.get('trend_1w')}
    
    [분석 지침]
    1. 해당 등급(S, A, B, F)의 의미를 설명하고 현재 점수(1~10)가 시사하는 바를 분석하세요.
    2. 수익률, 거래량, 추세의 조화를 바탕으로 '매수/보유/관망' 의견을 제시하세요.
    3. 특히 {target_stock.get('vol_status')} 상태인 거래량이 향후 주가에 미칠 영향을 서술하세요.
    
    형식: 전문가 리포트 스타일로 작성.
    """
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return {"report": response.text}
    except Exception as e:
        return {"error": f"AI 분석 중 오류 발생: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
