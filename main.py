import pandas as pd
import os, json, requests, base64, datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from google import genai
import pdfplumber
from pykrx import stock
import yfinance as yf
from io import BytesIO, StringIO

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 설정값
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_USER = "JungHoLee100"
REPO_NAME = "etf-data-collector"
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# --- [도우미] GitHub API 통신 ---
def manage_github_file(path, content=None, method="GET"):
    url = f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    
    if method == "GET":
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            info = res.json()
            return json.loads(base64.b64decode(info['content']).decode('utf-8-sig')), info['sha']
        return None, None
    else:
        _, sha = manage_github_file(path)
        payload = {"message": "Update from Matrix System", "content": base64.b64encode(content.encode('utf-8')).decode('utf-8')}
        if sha: payload["sha"] = sha
        return requests.put(url, headers=headers, json=payload)

# --- [API 엔드포인트] ---

@app.get("/api/init")
async def init_data():
    """A, C, E 데이터 및 포트폴리오/로그 초기 로드"""
    base_url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"
    files = {"A": "CSV_A_Analysis.csv", "C": "CSV_C.csv", "E": "CSV_E.csv"}
    static_data = {}
    for k, v in files.items():
        try:
            df = pd.read_csv(f"{base_url}/{v}", encoding='utf-8-sig')
            static_data[k] = df.fillna(0).to_dict(orient="records")
        except: static_data[k] = []
    
    portfolio, _ = manage_github_file("portfolio.json")
    logs, _ = manage_github_file("recommend_logs.json")
    return {
        "static": static_data,
        "portfolio": portfolio or {"holdings": []},
        "logs": logs or []
    }

@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    """B, D, PDF 융합 심층 분석"""
    info = await req.json()
    code, name = info.get("code"), info.get("name")
    
    # 1. 최신 PDF 탐색 및 텍스트 추출
    res = requests.get(f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/reports", 
                       headers={"Authorization": f"token {GITHUB_TOKEN}"})
    files = res.json()
    target_pdf = next((f for f in sorted(files, key=lambda x: x['name'], reverse=True) 
                       if f"({code})" in f['name']), None)
    
    pdf_text = "PDF 정보 없음"
    if target_pdf:
        pdf_res = requests.get(target_pdf['download_url'])
        with pdfplumber.open(BytesIO(pdf_res.content)) as pdf:
            pdf_text = "\n".join([page.extract_text() for page in pdf.pages[:5]])

    # 2. 실시간 B(수급) & D(주가/재무) 조회
    # B: 외국인/기관 선물 포지션 (단순화 예시)
    today = datetime.datetime.now().strftime("%Y%m%d")
    b_data = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "KOSPI").head(10).to_string()
    
    # D: 구성종목 주가 및 재무 (Gemini가 PDF에서 종목 추출 후 분석하도록 유도)
    prompt = f"""
    당신은 ETF 전략가입니다. 아래 데이터를 융합 분석하세요.
    [대상]: {name}({code})
    [PDF 운영보고서 핵심]: {pdf_text[:1500]}
    [시장수급(B)]: {b_data}
    
    [수행 과제]
    1. PDF에서 언급된 상위 구성종목들을 확인하고, 그들의 최근 기세와 재무(PER, PEG)를 추론하여 분석할 것.
    2. 최종 전략: '추가매수/일부매도/전부매도/보유' 중 하나를 선택하고 근거를 설명할 것.
    3. 'CSV_A' 리스트를 참고하여 이와 유사한 섹터의 추천 종목 3개를 제시할 것.
    """
    
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    report_text = response.text
    
    # 3. 로그 저장
    logs, _ = manage_github_file("recommend_logs.json")
    logs = logs or []
    new_log = {"date": today, "code": code, "name": name, "strategy": "분석완료", "reason": report_text}
    logs.append(new_log)
    manage_github_file("recommend_logs.json", content=json.dumps(logs, ensure_ascii=False), method="PUT")
    
    return {"analysis": report_text, "logs": logs}

@app.post("/api/portfolio/save")
async def save_portfolio(req: Request):
    data = await req.json()
    manage_github_file("portfolio.json", content=json.dumps(data, ensure_ascii=False), method="PUT")
    return {"status": "success"}

@app.post("/api/integrate-analysis")
async def integrate_analysis(req: Request):
    """누적된 로그를 바탕으로 통합 분석"""
    data = await req.json()
    logs = data.get("logs", [])
    prompt = f"다음은 그동안의 추천 내역입니다: {json.dumps(logs, ensure_ascii=False)}. 이 내역들을 종합하여 현재 가장 유망한 섹터와 최종 투자 결론을 내려주세요."
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    return {"analysis": response.text}
