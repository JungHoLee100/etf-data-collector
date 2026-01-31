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
        # 프론트엔드에서 오는 code(ticker)를 깨끗하게 정리 (따옴표 제거)
        target_code = str(raw_info.get("code") or raw_info.get("ticker") or "").replace("'", "").strip()
        target_name = raw_info.get("name") or "미상 종목"

        base_url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/main"

        def get_clean_df(filename):
            try:
                # 쉼표 구분 표준 CSV 로드
                df = pd.read_csv(f"{base_url}/{filename}", encoding='utf-8-sig')
                # 모든 컬럼명과 문자열 데이터의 양끝 공백 제거
                df.columns = df.columns.str.strip()
                return df
            except: return pd.DataFrame()

        # [1] 데이터 로드
        df_a = get_clean_df("CSV_A_Analysis.csv")
        df_c = get_clean_df("CSV_C.csv")
        df_e = get_clean_df("CSV_E.csv")

        # [2] 대상 종목 매칭 (작은따옴표가 포함된 경우와 없는 경우 모두 대응)
        def find_row(df, code):
            if df.empty: return []
            col = next((c for c in df.columns if c.lower() in ['ticker', 'code', '종목코드']), None)
            if not col: return []
            # 데이터 내의 '005930' 혹은 005930 모두를 타겟 코드와 비교
            mask = df[col].astype(str).str.replace("'", "").str.strip() == code
            return df[mask].to_dict(orient='records')

        data_a = find_row(df_a, target_code)
        data_c = df_c.head(10).to_dict(orient='records') # 매크로는 전체 흐름 전달
        data_e = find_row(df_e, target_code)

        # [3] 실시간 B(수급) 정보 수집
        try:
            today = datetime.datetime.now().strftime("%Y%m%d")
            df_b = stock.get_market_net_purchases_of_equities(today, today, "KOSPI")
            b_summary = df_b.loc[['외국인', '기관합계'], ['순매수거래대금']].to_dict()
        except: b_summary = "수급 데이터 일시적 지연"

        # [4] Gemini 최종 분석 명령
        prompt = f"""
        당신은 퀀트 전문가입니다. 분석 종목: {target_name}({target_code})
        - 모델 분석(A): {json.dumps(data_a, ensure_ascii=False)}
        - 시장 매크로(C): {json.dumps(data_c, ensure_ascii=False)}
        - 상세 지표(E): {json.dumps(data_e, ensure_ascii=False)}
        - 실시간 수급(B): {json.dumps(b_summary, ensure_ascii=False)}

        [지시]
        1. 위 데이터를 종합하여 {target_name}에 대한 투자 등급을 재평가하고 사유를 설명하세요.
        2. '데이터 없음'이라는 표현 대신, 현재 시장 지표를 통해 유추할 수 있는 최선의 전략을 제시하세요.
        3. 추천 종목은 반드시 '종목명(코드)' 형식으로 3개 포함하세요.
        """

        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return {"analysis": response.text}

    except Exception as e:
        return {"analysis": f"🛠️ 분석 도중 오류 발생: {str(e)}"}
