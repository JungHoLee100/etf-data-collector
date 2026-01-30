import pandas as pd
import os, json, requests, ssl
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from google import genai
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

GITHUB_USER = "JungHoLee100"
REPO_NAME = "etf-data-collector"
BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"
API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=API_KEY)

# 1. 정적 파일 (A, C, E) 로드
@app.get("/api/data/{file_type}")
def get_static_data(file_type: str):
    file_names = {"A": "CSV_A_Analysis.csv", "C": "CSV_C.csv", "E": "CSV_E.csv"}
    target = file_names.get(file_type.upper())
    if not target: return {"error": "Invalid type"}
    try:
        df = pd.read_csv(f"{BASE_URL}/{target}", encoding='utf-8-sig')
        return {"data": df.fillna(0).to_dict(orient="records")}
    except: return {"error": "파일을 찾을 수 없습니다."}

# 2. 심층 분석 (PDF 확인 -> 구성종목 추출 -> D/B API 분석)
@app.post("/api/deep-analyze")
def deep_analyze(info: dict):
    name = info.get("name")
    code = info.get("code")
    
    # 💡 PDF 파일명 규칙 확인 (날짜는 최신순 조회가 필요하므로 목록 확인 로직 권장)
    # 여기서는 GitHub API를 사용하여 reports 폴더 내 해당 종목코드가 포함된 PDF가 있는지 확인합니다.
    pdf_list_url = f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/reports"
    res = requests.get(pdf_list_url)
    files = res.json()
    
    # 파일명에 (종목코드)가 포함된 PDF 찾기
    target_pdf = next((f['download_url'] for f in files if f['name'].endswith(".pdf") and f"({code})" in f['name']), None)

    if not target_pdf:
        return {"status": "NEED_PDF", "message": f"해당 종목의 운영보고서 PDF가 없습니다. '날짜_{name}({code})_운영보고서.pdf' 형식으로 reports 폴더에 업로드해 주세요."}

    # 💡 PDF가 있을 경우 Gemini가 수행할 작업 프롬프트
    prompt = f"""
    당신은 ETF 분석 전문가입니다. 
    1. 다음 PDF({target_pdf})에서 이 ETF의 '구성 종목 리스트'를 모두 추출하세요.
    2. 추출된 개별 기업들에 대해 실시간 주가 정보(CSV_D 대체용 API 데이터)를 분석하세요.
    3. 지난 30일간 외국인/기관의 선물/옵션 포지션(CSV_B 대체용 API 데이터)을 참고하여 향후 방향성을 예측하세요.
    4. 최종적으로 이 ETF에 대한 '매수/보유/관망' 의견과 그 이유를 기술하세요.
    
    *참고: API 데이터는 당신이 보유한 실시간 금융 지식과 추론 능력을 바탕으로 최신 상태를 반영하세요.
    """
    
    try:
        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return {"status": "SUCCESS", "analysis": response.text}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}
