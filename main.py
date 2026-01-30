import pandas as pd
from datetime import datetime
import os, json, ssl
from google import genai
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# SSL 및 환경변수 설정
ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

app = FastAPI()
# CORS 설정: 프론트엔드(React)와의 통신을 위해 필수
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- 경로 설정 (상대 경로로 변경하여 GitHub/서버 어디서든 작동) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# collector2.py가 생성하는 분석 파일 경로
ANALYSIS_CSV = os.path.join(BASE_DIR, "CSV_A_Analysis.csv")
PORTFOLIO_PATH = os.path.join(BASE_DIR, "portfolio.json")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

# --- API 엔드포인트 ---

@app.get("/api/analyze/latest")
def get_latest_analysis():
    """collector2.py가 만든 S/A/B/F 분석 리포트를 읽어옵니다."""
    try:
        if not os.path.exists(ANALYSIS_CSV):
            return {"error": "분석 데이터 파일이 아직 생성되지 않았습니다."}
        
        df = pd.read_csv(ANALYSIS_CSV)
        # 엑셀 따옴표 처리 제거 및 데이터 정제
        df['ticker'] = df['ticker'].str.replace("'", "")
        return {
            "last_updated": datetime.fromtimestamp(os.path.getmtime(ANALYSIS_CSV)).strftime('%Y-%m-%d %H:%M'),
            "data": df.fillna(0).to_dict(orient="records")
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/ai-strategy")
def get_ai_strategy(data: dict):
    """
    S/A/B/F 등급 정보를 포함하여 Gemini에게 투자 전략을 묻습니다.
    """
    target_stock = data.get("stock_info") # 프론트에서 보낸 특정 종목의 모든 컬럼 정보
    
    prompt = f"""
    당신은 대한민국 최고의 ETF 퀀트 애널리스트입니다. 아래 데이터를 바탕으로 투자 전략을 수립하세요.
    
    [종목 정보]
    - 종목명: {target_stock['name']} ({target_stock['ticker']})
    - 현재 등급: {target_stock['grade_score']} ({target_stock['description']})
    - 1달 초과수익(Alpha): {target_stock['alpha_1m']}%
    - 거래량 에너지(RVOL): {target_stock['rvol']}%
    - 1주 추세: {target_stock['trend_1w']}
    
    [분석 지침]
    1. 해당 등급(S, A, B, F)의 의미를 설명하고 현재 점수(1~10)가 시사하는 바를 분석하세요.
    2. 수익률, 거래량, 추세의 조화를 바탕으로 '매수/보유/관망' 의견을 제시하세요.
    3. 특히 {target_stock['vol_status']} 상태인 거래량이 향후 주가에 미칠 영향을 서술하세요.
    
    형식: 전문가 리포트 스타일로 작성.
    """
    
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return {"report": response.text}
    except Exception as e:
        return {"error": f"AI 분석 중 오류 발생: {str(e)}"}

@app.get("/api/portfolio")
def get_portfolio():
    if not os.path.exists(PORTFOLIO_PATH): return {"holdings": []}
    with open(PORTFOLIO_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

@app.post("/api/portfolio/save")
def save_portfolio(data: dict):
    with open(PORTFOLIO_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    # 외부 접속을 허용하려면 0.0.0.0으로 실행
    uvicorn.run(app, host="0.0.0.0", port=8001)
