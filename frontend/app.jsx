import React, { useState, useEffect, useMemo } from 'react';

// --- [PART 1] 비밀번호 게이트 (보안) ---
const App = () => {
  const [password, setPassword] = useState("");
  const [isAuthorized, setIsAuthorized] = useState(false);

  // 💡 정호님, 여기서 비밀번호를 직접 설정하세요!
  const MASTER_PASSWORD = "sd*11070823"; 

  const handleLogin = () => {
    if (password === MASTER_PASSWORD) {
      setIsAuthorized(true);
      sessionStorage.setItem("isAuth", "true");
    } else {
      alert("비밀번호가 틀렸습니다!");
    }
  };

  useEffect(() => {
    if (sessionStorage.getItem("isAuth") === "true") {
      setIsAuthorized(true);
    }
  }, []);

  if (!isAuthorized) {
    return (
      <div style={{ height: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center', backgroundColor: '#121212', fontFamily: 'Pretendard' }}>
        <div style={{ padding: '40px', background: '#fff', borderRadius: '20px', textAlign: 'center', boxShadow: '0 10px 30px rgba(0,0,0,0.3)' }}>
          <h2 style={{ color: '#007bff', marginBottom: '15px' }}>📡 ETF ALPHA MATRIX</h2>
          <p style={{ color: '#666', fontSize: '14px', marginBottom: '20px' }}>비공개 시스템 접속을 위해 인증이 필요합니다.</p>
          <input 
            type="password" 
            placeholder="Password" 
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleLogin()}
            style={{ padding: '12px', width: '220px', borderRadius: '8px', border: '1px solid #ddd', marginBottom: '15px', textAlign: 'center' }}
          />
          <br />
          <button onClick={handleLogin} style={{ width: '100%', padding: '12px', background: '#007bff', color: '#fff', border: 'none', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold' }}>시스템 로그인</button>
        </div>
      </div>
    );
  }

  return <Dashboard />;
};

// --- [PART 2] 실제 ETF 분석 대시보드 ---
const Dashboard = () => {
  const [etfList, setEtfList] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [modalStock, setModalStock] = useState(null);
  const [modalAnalysis, setModalAnalysis] = useState("");

  // 💡 Render.com에 배포한 후 받은 실제 서비스 주소를 입력하세요!
  const API_BASE = "https://your-render-app-url.onrender.com/api"; 

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

  const loadData = async () => {
    setIsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/analyze/latest`);
      const result = await res.json();
      if (result?.data) setEtfList(result.data);
    } catch (err) {
      console.error("서버 연결 실패:", err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => { loadData(); }, []);

  const handleStockClick = async (etf) => {
    setModalStock(etf);
    setModalAnalysis("🤖 Gemini AI가 퀀트 데이터와 시장 상황을 정밀 분석 중입니다...");
    
    try {
      const res = await fetch(`${API_BASE}/ai-strategy`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stock_info: etf })
      });
      const data = await res.json();
      setModalAnalysis(data.report || "분석 결과를 가져오지 못했습니다.");
    } catch (err) {
      setModalAnalysis("⚠️ AI 서버 연결에 실패했습니다.");
    }
  };

  if (isLoading) return <div style={{ height: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center', fontWeight: 'bold' }}>📡 최신 ETF 성적표 수신 중...</div>;

  return (
    <div style={{ padding: '25px', backgroundColor: '#fcfcfc', minHeight: '100vh', fontFamily: 'Pretendard, sans-serif' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '30px', alignItems: 'center' }}>
        <div>
          <h1 style={{ color: '#007bff', margin: 0, fontSize: '24px', fontWeight: '900' }}>📡 ETF ALPHA MATRIX ONLINE</h1>
          <p style={{ margin: '5px 0 0', color: '#666', fontSize: '13px' }}>GitHub Cloud 기반 자동화 시스템 (v2.1)</p>
        </div>
        <div style={{ display: 'flex', gap: '10px' }}>
          <button onClick={loadData} style={{ padding: '8px 15px', background: '#007bff', color: '#fff', border: 'none', borderRadius: '6px', cursor: 'pointer', fontSize: '13px' }}>🔄 갱신</button>
          <button onClick={() => {sessionStorage.clear(); window.location.reload();}} style={{ padding: '8px 15px', background: '#6c757d', color: '#fff', border: 'none', borderRadius: '6px', cursor: 'pointer', fontSize: '13px' }}>🔒 로그아웃</button>
        </div>
      </div>

      <div style={{ background: '#fff', borderRadius: '15px', boxShadow: '0 4px 15px rgba(0,0,0,0.05)', overflow: 'hidden', border: '1px solid #eee' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'center', fontSize: '13px' }}>
          <thead style={{ background: '#1a1a1a', color: '#fff' }}>
            <tr style={{ height: '50px' }}>
              <th style={{ paddingLeft: '20px', textAlign: 'left' }}>종목명(코드)</th>
              <th>현재가</th>
              <th>등급/점수</th>
              <th>Alpha(1M)</th>
              <th>추세</th>
              <th>거래에너지</th>
              <th style={{ textAlign: 'left' }}>AI 요약 코멘트</th>
            </tr>
          </thead>
          <tbody>
            {etfList.map((etf, i) => {
              const style = getGradeStyle(etf.grade_score);
              return (
                <tr key={i} onDoubleClick={() => handleStockClick(etf)} style={{ borderBottom: '1px solid #f0f0f0', height: '55px', cursor: 'pointer' }} onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f9f9f9'} onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}>
                  <td style={{ textAlign: 'left', paddingLeft: '20px', fontWeight: 'bold' }}>{etf.name}</td>
                  <td>{Number(etf.price_curr).toLocaleString()}</td>
                  <td>
                    <span style={{ backgroundColor: style.bg, color: style.color, border: style.border, padding: '3px 10px', borderRadius: '15px', fontWeight: 'bold', fontSize: '11px' }}>
                      {etf.grade_score}
                    </span>
                  </td>
                  <td style={{ color: etf.alpha_1m > 0 ? '#d9534f' : '#0275d8', fontWeight: 'bold' }}>{etf.alpha_1m > 0 ? `+${etf.alpha_1m}` : etf.alpha_1m}%</td>
                  <td style={{ color: '#333' }}>{etf.trend_1w}</td>
                  <td>
                    <div style={{ fontWeight: 'bold', color: etf.rvol >= 150 ? '#d9534f' : '#333' }}>{etf.rvol}%</div>
                    <div style={{ fontSize: '10px', color: '#999' }}>{etf.vol_status}</div>
                  </td>
                  <td style={{ textAlign: 'left', color: '#666', fontSize: '12px', paddingRight: '15px' }}>{etf.description}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {modalStock && (
        <div style={{ position: 'fixed', top: 0, left: 0, width: '100vw', height: '100vh', backgroundColor: 'rgba(0,0,0,0.8)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 1000 }}>
          <div style={{ backgroundColor: '#fff', padding: '35px', borderRadius: '20px', width: '85vw', maxHeight: '85vh', overflowY: 'auto' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '2px solid #007bff', paddingBottom: '15px', marginBottom: '20px' }}>
              <h2 style={{ margin: 0 }}>🔍 {modalStock.name} 전략 분석 리포트</h2>
              <button onClick={() => setModalStock(null)} style={{ fontSize: '24px', cursor: 'pointer', border: 'none', background: 'none' }}>✕</button>
            </div>
            <div style={{ whiteSpace: 'pre-wrap', fontSize: '15px', lineHeight: '1.8', color: '#2c3e50' }}>{modalAnalysis}</div>
          </div>
        </div>
      )}
    </div>
  );
};

export default App;
