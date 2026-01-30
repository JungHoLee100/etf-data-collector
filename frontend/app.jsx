import React, { useState, useEffect, useMemo } from 'react';

const App = () => {
  const [etfList, setEtfList] = useState([]);
  const [portfolio, setPortfolio] = useState({ holdings: [] });
  const [aiReport, setAiReport] = useState("");
  const [isInitialLoading, setIsInitialLoading] = useState(true);
  const [isStrategyLoading, setIsStrategyLoading] = useState(false);
  const [modalStock, setModalStock] = useState(null);
  const [modalAnalysis, setModalAnalysis] = useState("");

  const API_BASE = "http://localhost:8001/api";

  // --- 1. 스타일 정의 (등급별 색상 매핑) ---
  const getGradeStyle = (gradeScore) => {
    const grade = gradeScore?.charAt(0) || 'F';
    const styles = {
      S: { bg: '#fff5f5', color: '#d9534f', border: '1px solid #d9534f', label: '주도주' },
      A: { bg: '#f0fff4', color: '#28a745', border: '1px solid #28a745', label: '급부상' },
      B: { bg: '#fffaf0', color: '#f0ad4e', border: '1px solid #f0ad4e', label: '눌림목' },
      F: { bg: '#f8f9fa', color: '#6c757d', border: '1px solid #ddd', label: '소외주' }
    };
    return styles[grade] || styles.F;
  };

  // --- 2. 데이터 로딩 ---
  const initLoad = async () => {
    setIsInitialLoading(true);
    try {
      const res = await fetch(`${API_BASE}/analyze/latest`);
      const result = await res.json();
      if (result?.data) setEtfList(result.data);
      
      const pRes = await fetch(`${API_BASE}/portfolio`);
      if (pRes.ok) setPortfolio(await pRes.json());
    } catch (err) {
      console.error("데이터 로드 실패:", err);
    } finally {
      setIsInitialLoading(false);
    }
  };

  useEffect(() => { initLoad(); }, []);

  // --- 3. Gemini 통합 분석 실행 ---
  const handleStockClick = async (etf) => {
    setModalStock(etf);
    setModalAnalysis("🤖 Gemini가 S/A/B/F 등급과 수급 데이터를 분석 중입니다...");
    
    try {
      const res = await fetch(`${API_BASE}/ai-strategy`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stock_info: etf })
      });
      const data = await res.json();
      setModalAnalysis(data.report || "분석 결과를 가져오지 못했습니다.");
    } catch (err) {
      setModalAnalysis("⚠️ AI 분석 서버와 통신할 수 없습니다.");
    }
  };

  if (isInitialLoading) return (
    <div style={{ height: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center', fontWeight: 'bold' }}>
      🚀 ETF 전략 데이터 기지국 연결 중...
    </div>
  );

  return (
    <div style={{ padding: '25px', backgroundColor: '#fdfdfd', minHeight: '100vh', fontFamily: 'Pretendard, sans-serif' }}>
      {/* 헤더 섹션 */}
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '30px', alignItems: 'center' }}>
        <div>
          <h1 style={{ color: '#007bff', margin: 0, fontSize: '26px', fontWeight: '900' }}>📡 ETF ALPHA MATRIX v2.0</h1>
          <p style={{ margin: '5px 0 0', color: '#666', fontSize: '14px' }}>S/A/B/F 퀀트 시스템 기반 실시간 투자 전략</p>
        </div>
        <button onClick={initLoad} style={{ padding: '10px 20px', background: '#007bff', color: '#fff', border: 'none', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}>🔄 데이터 새로고침</button>
      </div>

      {/* 메인 분석 테이블 */}
      <section style={{ background: '#fff', borderRadius: '15px', boxShadow: '0 4px 20px rgba(0,0,0,0.05)', overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'center' }}>
          <thead style={{ background: '#1a1a1a', color: '#fff' }}>
            <tr style={{ height: '50px' }}>
              <th style={{ paddingLeft: '20px', textAlign: 'left' }}>종목명(코드)</th>
              <th>현재가</th>
              <th>등급/점수</th>
              <th>1달 Alpha</th>
              <th>1주 추세</th>
              <th>거래에너지(RVOL)</th>
              <th style={{ textAlign: 'left' }}>데이터 분석 코멘트</th>
            </tr>
          </thead>
          <tbody>
            {etfList.map((etf, i) => {
              const style = getGradeStyle(etf.grade_score);
              return (
                <tr key={i} onDoubleClick={() => handleStockClick(etf)} style={{ borderBottom: '1px solid #f0f0f0', height: '55px', cursor: 'pointer', transition: 'background 0.2s' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}>
                  <td style={{ textAlign: 'left', paddingLeft: '20px', fontWeight: 'bold' }}>{etf.name.replace("'", "")}</td>
                  <td style={{ fontWeight: '500' }}>{Number(etf.price_curr).toLocaleString()}</td>
                  <td>
                    <span style={{ 
                      backgroundColor: style.bg, color: style.color, border: style.border,
                      padding: '4px 10px', borderRadius: '20px', fontWeight: '800', fontSize: '12px'
                    }}>
                      {etf.grade_score}
                    </span>
                  </td>
                  <td style={{ color: etf.alpha_1m > 0 ? '#d9534f' : '#0275d8', fontWeight: 'bold' }}>
                    {etf.alpha_1m > 0 ? `+${etf.alpha_1m}` : etf.alpha_1m}%
                  </td>
                  <td style={{ fontSize: '13px', color: '#333' }}>{etf.trend_1w}</td>
                  <td>
                    <div style={{ fontWeight: 'bold', color: etf.rvol >= 150 ? '#d9534f' : '#333' }}>{etf.rvol}%</div>
                    <div style={{ fontSize: '10px', color: '#999' }}>{etf.vol_status}</div>
                  </td>
                  <td style={{ textAlign: 'left', color: '#666', fontSize: '13px' }}>{etf.description}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      {/* 분석 모달 */}
      {modalStock && (
        <div style={{ position: 'fixed', top: 0, left: 0, width: '100vw', height: '100vh', backgroundColor: 'rgba(0,0,0,0.7)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 1000 }}>
          <div style={{ backgroundColor: '#fff', padding: '35px', borderRadius: '20px', width: '80vw', maxHeight: '80vh', overflowY: 'auto', boxShadow: '0 10px 40px rgba(0,0,0,0.2)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '2px solid #007bff', paddingBottom: '15px', marginBottom: '20px' }}>
              <h2 style={{ margin: 0 }}>🔍 {modalStock.name} 전략 리포트</h2>
              <button onClick={() => setModalStock(null)} style={{ fontSize: '24px', cursor: 'pointer', border: 'none', background: 'none' }}>✕</button>
            </div>
            <div style={{ whiteSpace: 'pre-wrap', fontSize: '16px', lineHeight: '1.8', color: '#333' }}>{modalAnalysis}</div>
          </div>
        </div>
      )}
    </div>
  );
};

export default App;
