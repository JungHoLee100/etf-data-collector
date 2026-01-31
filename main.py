import pandas as pd
import os, json, requests, datetime, base64
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from google import genai
import pdfplumber
from pykrx import stock
import yfinance as yf
from io import BytesIO

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_USER = "JungHoLee100"
REPO_NAME = "etf-data-collector"
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# GitHub 파일 처리 함수
def github_file(path, content=None, method="GET"):
    url = f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    if method == "GET":
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            info = res.json()
            return json.loads(base64.b64decode(info['content']).decode('utf-8-sig')), info['sha']
        return None, None
    else:
        _, sha = github_file(path)
        payload = {"message": "Update Portfolio", "content": base64.b64encode(content.encode('utf-8')).decode('utf-8')}
        if sha: payload["sha"] = sha
        return requests.put(url, headers=headers, json=payload)

@app.get("/api/init")
async def init():
    base_url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"
    data = {}
    for k, v in {"A": "CSV_A_Analysis.csv", "C": "CSV_C.csv", "E": "CSV_E.csv"}.items():
        try: data[k] = pd.read_csv(f"{base_url}/{v}", encoding='utf-8-sig').fillna(0).to_dict(orient="records")
        except: data[k] = []
    port, _ = github_file("portfolio.json")
    return {"static": data, "portfolio": port or {"holdings": []}}

@app.post("/api/portfolio/save")
async def save_port(req: Request):
    data = await req.json()
    github_file("portfolio.json", content=json.dumps(data, ensure_ascii=False), method="PUT")
    return {"status": "success"}

@app.post("/api/deep-analyze")
async def deep_analyze(req: Request):
    info = await req.json()
    code, name = str(info.get("code")), info.get("name")
    
    # --- [1] CSV A, C, E 데이터 로드 (Gemini 주입용) ---
    base_url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"
    try:
        df_a = pd.read_csv(f"{base_url}/CSV_A_Analysis.csv", encoding='utf-8-sig')
        df_c = pd.read_csv(f"{base_url}/CSV_C.csv", encoding='utf-8-sig') # 👈 누락된 C 추가
        df_e = pd.read_csv(f"{base_url}/CSV_E.csv", encoding='utf-8-sig')
        
        target_a = df_a[df_a['ticker'] == code].to_dict(orient='records')
        macro_c = df_c.head(10).to_dict(orient='records') # 👈 시장 전체 매크로 정보
        target_e = df_e[df_e['ticker'] == code].to_dict(orient='records')
    except:
        target_a, macro_c, target_e = [], [], []

    # --- [2] PDF 분석 (개별 종목명 추출) ---
    pdf_stocks = "PDF 정보 없음"
    try:
        res = requests.get(f"https://api.github.com/repos/{GITHUB_USER}/{REPO_NAME}/contents/reports", 
                           headers={"Authorization": f"token {GITHUB_TOKEN}"})
        files = res.json()
        target_pdf = next((f for f in files if f"({code})" in f['name']), None)
        if target_pdf:
            pdf_res = requests.get(target_pdf['download_url'])
            with pdfplumber.open(BytesIO(pdf_res.content)) as pdf:
                text = "\n".join([p.extract_text() for p in pdf.pages[:3]])
                extract_res = client.models.generate_content(
                    model="gemini-2.0-flash", 
                    contents=f"이 ETF 보고서 텍스트에서 상위 5개 보유 종목명만 리스트로 뽑아줘: {text}"
                )
                pdf_stocks = extract_res.text
    except: pass

    # --- [3] 실시간 수급(B) 및 ETF 상태(D) ---
    today = datetime.datetime.now().strftime("%Y%m%d")
    try:
        df_b = stock.get_market_net_purchases_of_equities(today, today, "KOSPI")
        b_data = df_b.loc[['외국인', '기관합계'], ['순매수거래대금']].to_dict()
        ticker_yf = yf.Ticker(f"{code}.KS" if not code.startswith('k') else f"{code}.KQ")
        hist = ticker_yf.history(period="30d")
        d_data = f"현재가: {hist['Close'].iloc[-1]}, 20일평균: {hist['Close'].rolling(20).mean().iloc[-1]:.2f}"
    except: b_data, d_data = "조회 불가", "조회 불가"

    # --- [4] Gemini 종합 추론 (A, C, E, B, D, PDF 융합) ---
    prompt = f"""
    당신은 정호님의 수석 퀀트 애널리스트입니다. 다음 데이터를 종합하여 {name}({code})에 대한 투자 전략을 수립하세요.
    
    1. 모델 지표(A): {json.dumps(target_a, ensure_ascii=False)}
    2. 시장 매크로 환경(C): {json.dumps(macro_c, ensure_ascii=False)}
    3. 상세 가감점(E): {json.dumps(target_e, ensure_ascii=False)}
    4. 실시간 시장 수급(B): {json.dumps(b_data, ensure_ascii=False)}
    5. ETF 가격 상태(D): {d_data}
    6. PDF 리포트내 상위 종목: {pdf_stocks}
    
    [미션]
    - CSV_C의 매크로 상황이 현재 종목의 A등급과 E상세 점수에 어떤 영향을 주는지 분석하세요.
    - PDF에서 나온 상위 종목들의 기세와 파생 수급(B)을 고려해 '매수/보유/매도' 결론을 내리세요.
    - CSV_A_Analysis 리스트 중 등급이 높은 유사 종목 3개를 '종목명(코드)' 형식으로 추천하세요.
    """
    
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    return {"analysis": response.text}
