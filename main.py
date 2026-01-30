import pandas as pd
import os, json, requests, ssl, base64
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from google import genai
from pdfplumber import open as open_pdf
from pykrx import stock
import yfinance as yf
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 설정 및 API 키
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") # 👈 GitHub PAT 필요
GITHUB_USER = "JungHoLee100"
REPO_NAME = "etf-data-collector"
BASE_URL = f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents"
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# --- [도우미 함수] GitHub 파일 읽기/쓰기 ---
def github_action(path, method="GET", content=None, message="update"):
    url = f"{BASE_URL}/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    
    if method == "GET":
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            file_data = res.json()
            return json.loads(base64.b64decode(file_data['content']).decode('utf-8-sig')), file_data['sha']
        return None, None
    else:
        _, sha = github_action(path, "GET")
        data = {"message": message, "content": base64.b64encode(content.encode('utf-8')).decode('utf-8')}
        if sha: data["sha"] = sha
        return requests.put(url, headers=headers, json=data)

# --- [API 엔드포인트] ---

@app.get("/api/init")
def init_dashboard():
    # A, C, E 데이터 및 포트폴리오 로드
    files = {"A": "CSV_A_Analysis.csv", "C": "CSV_C.csv", "E": "CSV_E.csv"}
    res_data = {}
    for k, v in files.items():
        try:
            df = pd.read_csv(f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main/{v}", encoding='utf-8-sig')
            res_data[k] = df.fillna(0).to_dict(orient="records")
        except: res_data[k] = []
    
    portfolio, _ = github_action("portfolio.json")
    return {"static": res_data, "portfolio": portfolio or {"holdings": []}}

@app.post("/api/portfolio/save")
def save_port(data: dict):
    github_action("portfolio.json", "PUT", json.dumps(data, ensure_ascii=False, indent=2))
    return {"status": "success"}

@app.post("/api/deep-analyze")
def deep_analyze(info: dict):
    target_code = info.get("code")
    target_name = info.get("name")
    
    # 1. PDF 탐색 및 텍스트 추출
    res = requests.get(f"{BASE_URL}/reports")
    files = res.json()
    target_pdf = next((f for f in files if f"({target_code})" in f['name']), None)
    
    pdf_text = ""
    if target_pdf:
        pdf_res = requests.get(target_pdf['download_url'])
        with open_pdf(BytesIO(pdf_res.content)) as pdf:
            pdf_text = "\n".join([page.extract_text() for page in pdf.pages[:10]]) # 최대 10장

    # 2. 실시간 B(수급) 및 D(개별주가/재무) 수집
    # 선물옵션 포지션 (B) - pykrx 활용 (예시: 당일 기관/외인 순매수)
    try:
        df_inv = stock.get_market_net_purchases_of_equities_by_ticker("20260101", "20260131", "KOSPI") # 날짜 동적 처리 필요
        b_data = df_inv.head(5).to_string() # 주요 수급 요약
    except: b_data = "수급 데이터 조회 실패"

    # 3. Gemini 종합 추론
    prompt = f"""
    당신은 퀀트와 리서치를 결합한 수석 애널리스트입니다.
    - 대상: {target_name}({target_code})
    - PDF 내용: {pdf_text[:2000]}...
    - 파생/수급(B): {b_data}
    - 재무(D) 요청: 위 PDF에서 추출한 구성 종목들의 최근 30일 주가(MA 5/20), PER, PBR, PEG를 분석하세요.
    
    [미션]
    1. 투자전략(추가매수/일부매도/전부매도/보유)을 결정하고 구체적 근거를 제시할 것.
    2. CSV_A 리스트 기반으로 유사 섹터 내 '추천 종목' 3개를 선정하고 사유를 적을 것.
    """
    
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    
    # 추천 로그 저장 (GitHub)
    log_entry = {"date": "2026-01-31", "name": target_name, "code": target_code, "report": response.text}
    logs, _ = github_action("recommend_logs.json")
    logs = logs or []
    logs.append(log_entry)
    github_action("recommend_logs.json", "PUT", json.dumps(logs, ensure_ascii=False, indent=2))
    
    return {"analysis": response.text, "logs": logs}
