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
    try:
        raw_info = await req.json()
        
        # [1] 분석 모드 판별 (개별 종목 vs 포트폴리오 전체)
        is_portfolio = raw_info.get("type") == "PORTFOLIO"
        holdings = raw_info.get("portfolio", [])

        # 분석할 티커 리스트 추출
        if is_portfolio:
            target_tickers = [str(h.get("code") or "").strip() for h in holdings if h.get("code")]
            target_name = "내 포트폴리오 전체"
        else:
            code = str(raw_info.get("code") or "").replace("'", "").strip()
            target_tickers = [code] if code else []
            target_name = str(raw_info.get("name") or "분석 대상")

        if not target_tickers:
            return {"analysis": "분석할 종목이 없습니다. 포트폴리오에 종목을 추가하거나 리스트를 선택해주세요."}

        # [2] CSV 데이터 로드 (표준 쉼표 형식 기반)
        base_url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"
        def get_clean_df(filename):
            try:
                df = pd.read_csv(f"{base_url}/{filename}", encoding='utf-8-sig')
                df.columns = df.columns.str.strip()
                return df
            except: return pd.DataFrame()

        df_a = get_clean_df("CSV_A_Analysis.csv")
        df_c = get_clean_df("CSV_C.csv")
        df_e = get_clean_df("CSV_E.csv")

        # [3] 여러 종목의 데이터를 한꺼번에 수집
        all_data_a, all_data_e = [], []
        ticker_col_a = next((c for c in df_a.columns if c.lower() in ['ticker', 'code']), 'ticker')
        ticker_col_e = next((c for c in df_e.columns if c.lower() in ['ticker', 'code']), 'ticker')

        for t in target_tickers:
            row_a = df_a[df_a[ticker_col_a].astype(str).str.contains(t)].to_dict(orient='records')
            row_e = df_e[df_e[ticker_col_e].astype(str).str.contains(t)].to_dict(orient='records')
            if row_a: all_data_a.extend(row_a)
            if row_e: all_data_e.extend(row_e)

        # [4] 실시간 B(수급) 및 C(매크로)
        data_c = df_c.head(5).to_dict(orient='records') if not df_c.empty else []
        try:
            today = datetime.datetime.now().strftime("%Y%m%d")
            df_b = stock.get_market_net_purchases_of_equities(today, today, "KOSPI")
            b_summary = df_b.loc[['외국인', '기관합계'], ['순매수거래대금']].to_dict()
        except: b_summary = "조회 지연"

        # [5] Gemini 최종 종합 분석
        prompt = f"""
        당신은 정호님의 수석 퀀트 비서입니다. 분석 대상: {target_name}
        
        - 대상 종목 코드들: {target_tickers}
        - 모델 점수(A): {json.dumps(all_data_a, ensure_ascii=False)}
        - 매크로(C): {json.dumps(data_c, ensure_ascii=False)}
        - 가감점(E): {json.dumps(all_data_e, ensure_ascii=False)}
        - 수급(B): {json.dumps(b_summary, ensure_ascii=False)}

        지시사항:
        1. 제공된 A, E 데이터를 바탕으로 각 종목의 현재 상태를 요약하세요.
        2. {target_name}의 구성이 현재 시장 매크로(C) 및 수급(B)과 잘 어울리는지 평가하세요.
        3. '미상 종목'이나 '데이터 없음'이라는 말은 피하고, 제공된 수치를 최대한 활용해 전략을 제시하세요.
        """

        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return {"analysis": response.text}

    except Exception as e:
        return {"analysis": f"🚨 시스템 연결 오류: {str(e)}"}
