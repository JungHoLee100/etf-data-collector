import pandas as pd
import os, json, ssl
from google import genai
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# SSL 및 환경변수 설정
ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

app = FastAPI()
# 모든 출처에서의 접속을 허용 (Vercel/GitHub Pages 연동을 위해 필수)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- 🌐 [온라인 환경 전용] GitHub 데이터 경로 설정 ---
# 정호님의 실제 GitHub 정보를 입력해주세요.
GITHUB_USER = "JungHoLee100" 
REPO_NAME = "etf-data-collector"
# GitHub Actions가 매일 아침 업데이트하는 분석 파일의 실제 인터넷 주소입니다.
RAW_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main/CSV_A_Analysis.csv"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

@app.get("/api/analyze/latest")
def get_latest_analysis():
    """GitHub에 저장된 최신 S/A/B/F 분석 리포트를 실시간으로 읽어옵니다."""
    try:
        # 로컬 파일 대신 GitHub Raw URL에서 직접 데이터를 땡겨옵니다.
        df = pd.read_csv(RAW_URL, encoding='cp949')
        
        # 엑셀 따옴표 처리 제거 및 데이터 정제
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str).str.replace("'", "")
            
        return {
            "source": "GitHub Cloud Storage",
            "data": df.fillna(0).to_dict(orient="records")
        }
    except Exception as e:
        return {"error": f"데이터 로드 실패: {str(e)}"}

@app.post("/api/ai-strategy")
def get_ai_strategy(data: dict):
    """
    [기존 로직 유지] S/A/B/F 등급 정보를 바탕으로 Gemini에게 전략을 묻습니다.
    """
    target_stock = data.get("stock_info")
    
    # 정호님이 설계한 분석 프롬프트 로직을 그대로 보존합니다.
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

@app.get("/api/portfolio")
def get_portfolio():
    # 포트폴리오는 비공개로 유지하기 위해 메모리나 DB 연동을 권장하지만, 
    # 일단 기존 로직대로 빈 리스트를 반환하거나 파일을 읽도록 둡니다.
    return {"holdings": []}

if __name__ == "__main__":
    import uvicorn
    # Render.com은 'PORT' 환경변수를 통해 포트를 지정합니다.
    port = int(os.environ.get("PORT", 10000))
    # 외부 서버에서 실행되므로 host를 0.0.0.0으로 고정합니다.
    uvicorn.run(app, host="0.0.0.0", port=port)
