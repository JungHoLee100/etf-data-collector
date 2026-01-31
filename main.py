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
        
        # [수정 포인트] 데이터가 없으면 "None" 대신 빈 문자열("")을 기본값으로 설정
        # 여러 종류의 변수명(code, ticker, 종목코드)을 모두 체크합니다.
        target_code = str(raw_info.get("code") or raw_info.get("ticker") or raw_info.get("종목코드") or "").replace("'", "").strip()
        target_name = str(raw_info.get("name") or raw_info.get("종목명") or "분석 대상")

        # 만약 코드가 비어있다면 분석을 중단하고 안내 메시지 반환
        if not target_code or target_code == "":
            return {"analysis": "분석할 종목 코드가 선택되지 않았습니다. 리스트에서 종목을 다시 선택해주세요."}

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

        def find_row(df, code):
            if df.empty: return []
            col = next((c for c in df.columns if c.lower() in ['ticker', 'code', '종목코드']), None)
            if not col: return []
            mask = df[col].astype(str).str.replace("'", "").str.strip() == code
            return df[mask].to_dict(orient='records')

        data_a = find_row(df_a, target_code)
        data_c = df_c.head(5).to_dict(orient='records') if not df_c.empty else []
        data_e = find_row(df_e, target_code)

        # Gemini 프롬프트 강화: None 방지용 이름 강제 주입
        prompt = f"""
        당신은 수석 퀀트 에널리스트입니다.
        분석 종목: {target_name}({target_code})
        
        [데이터 패키지]
        - 모델 점수(A): {json.dumps(data_a, ensure_ascii=False)}
        - 시장 매크로(C): {json.dumps(data_c, ensure_ascii=False)}
        - 가감점 요인(E): {json.dumps(data_e, ensure_ascii=False)}

        지시: 위 데이터를 기반으로 {target_name}의 투자 전략을 수립하세요. 
        만약 데이터가 비어있다면, {target_name} 종목의 섹터 특성을 반영하여 전문적인 의견을 제시하세요.
        'None' 또는 '데이터 없음'이라는 단어 사용을 지양하고 전략적 가이드를 제공하세요.
        """

        response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        return {"analysis": response.text}

    except Exception as e:
        return {"analysis": f"시스템 분석 중 오류 발생: {str(e)}"}
