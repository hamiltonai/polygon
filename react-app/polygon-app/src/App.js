import React, { useState, useEffect, useRef } from 'react';
import './App.css';
import Papa from 'papaparse';
import Chart from 'chart.js/auto';

function StockDashboard() {
  // State
  const [rawData, setRawData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [currentPage, setCurrentPage] = useState(0);
  const [rowsPerPage] = useState(25);
  const [sortConfig, setSortConfig] = useState({ key: 'change_pct', direction: 'desc' });
  const [viewMode, setViewMode] = useState('table');
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [fileName, setFileName] = useState('');
  const [qualifiedOnly, setQualifiedOnly] = useState(false);
  const trendChartRef = useRef(null);
  const chartInstance = useRef(null);

  // Chart.js setup
  useEffect(() => {
    if (!trendChartRef.current) return;
    if (chartInstance.current) {
      chartInstance.current.destroy();
    }
    chartInstance.current = new Chart(trendChartRef.current, {
      type: 'line',
      data: {
        labels: [],
        datasets: [
          {
            label: 'Qualified Stocks',
            data: [],
            borderColor: '#81c784',
            backgroundColor: 'rgba(129, 199, 132, 0.1)',
            tension: 0.4,
          },
          {
            label: 'Total Stocks',
            data: [],
            borderColor: '#bb86fc',
            backgroundColor: 'rgba(187, 134, 252, 0.1)',
            tension: 0.4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            labels: {
              color: '#ffffff',
            },
          },
        },
        scales: {
          x: {
            grid: { color: '#333' },
            ticks: { color: '#aaa' },
          },
          y: {
            grid: { color: '#333' },
            ticks: { color: '#aaa' },
          },
        },
      },
    });
    return () => chartInstance.current && chartInstance.current.destroy();
  }, []);

  // Update chart data
  useEffect(() => {
    if (!chartInstance.current) return;
    const timeData = {};
    rawData.forEach(item => {
      const time = new Date(item.timestamp).toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
      });
      if (!timeData[time]) timeData[time] = { qualified: 0, total: 0 };
      timeData[time].total++;
      if (item.meets_criteria === 'Y') timeData[time].qualified++;
    });
    const labels = Object.keys(timeData);
    const qualifiedCounts = labels.map(l => timeData[l].qualified);
    const totalCounts = labels.map(l => timeData[l].total);
    chartInstance.current.data.labels = labels;
    chartInstance.current.data.datasets[0].data = qualifiedCounts;
    chartInstance.current.data.datasets[1].data = totalCounts;
    chartInstance.current.update();
  }, [rawData]);

  // File upload handler
  const handleFileUpload = (e) => {
    const file = e.target.files[0];
    if (file) {
      setFileName(file.name);
      Papa.parse(file, {
        header: true,
        complete: function (results) {
          processData(results.data);
        },
      });
    }
  };

  // Data processing
  function processData(data) {
    setRawData(data);
    setFilteredData(data.filter(d => d.meets_criteria === 'Y'));
    setCurrentPage(0);
  }

  // Stats
  function getLatestData(data) {
    const latestBySymbol = {};
    data.forEach(item => {
      if (!latestBySymbol[item.symbol] || new Date(item.timestamp) > new Date(latestBySymbol[item.symbol].timestamp)) {
        latestBySymbol[item.symbol] = item;
      }
    });
    return Object.values(latestBySymbol);
  }

  function updateStats() {
    const latestData = getLatestData(rawData);
    const qualified = latestData.filter(d => d.meets_criteria === 'Y');
    const totalVolume = latestData.reduce((sum, d) => sum + parseInt(d.volume || 0), 0);
    const avgChange = latestData.reduce((sum, d) => sum + parseFloat(d.change_pct || 0), 0) / latestData.length;
    const gainers = latestData.filter(d => parseFloat(d.change_pct) > 0).length;
    const losers = latestData.filter(d => parseFloat(d.change_pct) < 0).length;
    return {
      total: latestData.length,
      qualified: qualified.length,
      qualificationRate: latestData.length ? (qualified.length / latestData.length * 100).toFixed(1) : 0,
      avgChange: avgChange || 0,
      gainers,
      losers,
      totalVolume,
    };
  }

  // Table sorting
  function sortTable(key) {
    let direction = 'desc';
    if (sortConfig.key === key && sortConfig.direction === 'desc') {
      direction = 'asc';
    }
    setSortConfig({ key, direction });
  }
  function getSortIcon(key) {
    if (sortConfig.key !== key) return '';
    return sortConfig.direction === 'asc' ? '▲' : '▼';
  }

  // Pagination
  function prevPage() {
    setCurrentPage(p => Math.max(0, p - 1));
  }
  function nextPage() {
    const latestData = getLatestData(qualifiedOnly ? filteredData : rawData);
    setCurrentPage(p => (p + 1) * rowsPerPage < latestData.length ? p + 1 : p);
  }

  // Filtering
  function filterData(searchQuery = '', qualified = qualifiedOnly) {
    let data = getLatestData(qualified ? filteredData : rawData);
    if (searchQuery) {
      data = data.filter(item => item.symbol.toLowerCase().includes(searchQuery.toLowerCase()));
    }
    return data;
  }

  // Render table
  function renderTable(data) {
    // Sort data
    const sorted = [...data].sort((a, b) => {
      const aValue = parseFloat(a[sortConfig.key]) || 0;
      const bValue = parseFloat(b[sortConfig.key]) || 0;
      if (sortConfig.direction === 'asc') {
        return aValue - bValue;
      }
      return bValue - aValue;
    });
    // Paginate
    const start = currentPage * rowsPerPage;
    const paginatedData = sorted.slice(start, start + rowsPerPage);
    return (
      <div>
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th onClick={() => sortTable('symbol')}>Symbol {getSortIcon('symbol')}</th>
                <th onClick={() => sortTable('current_price')}>Price {getSortIcon('current_price')}</th>
                <th onClick={() => sortTable('change_pct')}>Change % {getSortIcon('change_pct')}</th>
                <th onClick={() => sortTable('volume')}>Volume {getSortIcon('volume')}</th>
                <th>Market Cap</th>
                <th>Qualified</th>
                <th>Bid/Ask</th>
              </tr>
            </thead>
            <tbody>
              {paginatedData.map((row, idx) => {
                const changeClass = parseFloat(row.change_pct) >= 0 ? 'success' : 'error';
                return (
                  <tr key={row.symbol + idx}>
                    <td style={{ fontWeight: 600 }}>{row.symbol}</td>
                    <td>{row.current_price}</td>
                    <td>
                      <span className={`badge badge-${changeClass}`}>
                        {parseFloat(row.change_pct) >= 0 ? '▲' : '▼'} {row.change_pct}%
                      </span>
                    </td>
                    <td>{parseInt(row.volume).toLocaleString()}</td>
                    <td>{row.market_cap_millions}M</td>
                    <td style={{ textAlign: 'center' }}>
                      {row.meets_criteria === 'Y' ? (
                        <span style={{ color: 'var(--success)' }}>✅</span>
                      ) : (
                        <span style={{ color: 'var(--error)' }}>❌</span>
                      )}
                    </td>
                    <td>{row.bid}/{row.ask}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="pagination">
          <div className="pagination-info">
            Showing {start + 1}–{Math.min(start + rowsPerPage, sorted.length)} of {sorted.length} stocks
          </div>
          <div className="pagination-controls">
            <button onClick={prevPage} disabled={currentPage === 0}>Prev</button>
            <button onClick={nextPage} disabled={(start + rowsPerPage) >= sorted.length}>Next</button>
          </div>
        </div>
      </div>
    );
  }

  // Main render
  const stats = updateStats();
  const [searchQuery, setSearchQuery] = useState('');

  // Filtered data for table
  const tableData = filterData(searchQuery, qualifiedOnly);

  // Export CSV handler
  const handleExportCSV = () => {
    // Get the currently displayed table data (with filters and sorting)
    const sorted = [...tableData].sort((a, b) => {
      const aValue = parseFloat(a[sortConfig.key]) || 0;
      const bValue = parseFloat(b[sortConfig.key]) || 0;
      if (sortConfig.direction === 'asc') {
        return aValue - bValue;
      }
      return bValue - aValue;
    });
    const start = currentPage * rowsPerPage;
    const paginatedData = sorted.slice(start, start + rowsPerPage);
    // Convert to CSV
    const csv = Papa.unparse(paginatedData);
    // Download logic
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', 'stock_data.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="App">
      <header className="header">
        <div className="header-content">
          <h1>
            <span role="img" aria-label="chart">📈</span>
            Stock Monitor Dashboard
          </h1>
          <div className="header-controls">
            <div className="file-input-wrapper">
              <button className="btn" onClick={() => document.getElementById('csvFile').click()}>
                <span role="img" aria-label="folder">📁</span> Upload CSV
              </button>
              <input type="file" id="csvFile" accept=".csv" style={{ display: 'none' }} onChange={handleFileUpload} />
              {fileName && <span style={{ marginLeft: 10, color: '#aaa' }}>{fileName}</span>}
            </div>
            <button className="btn" onClick={() => window.location.reload()}>
              <span role="img" aria-label="refresh">🔄</span> Refresh
            </button>
          </div>
        </div>
      </header>
      <main className="container">
        {/* Stats Cards */}
        <div className="grid grid-4" id="statsGrid">
          <div className="card stat-card">
            <div className="stat-card-content">
              <div className="stat-card-text">
                <h3>Total Stocks</h3>
                <div className="value" id="totalStocks">{stats.total}</div>
                <div className="subtitle">Active symbols</div>
              </div>
              <div className="stat-card-icon primary">📊</div>
            </div>
          </div>
          <div className="card stat-card">
            <div className="stat-card-content">
              <div className="stat-card-text">
                <h3>Qualified</h3>
                <div className="value" id="qualifiedStocks">{stats.qualified}</div>
                <div className="subtitle" id="qualificationRate">{stats.qualificationRate}% qualification rate</div>
              </div>
              <div className="stat-card-icon success">✅</div>
            </div>
          </div>
          <div className="card stat-card">
            <div className="stat-card-content">
              <div className="stat-card-text">
                <h3>Market Trend</h3>
                <div className="value" id="marketTrend">{stats.avgChange >= 0 ? '+' : ''}{stats.avgChange.toFixed(2)}%</div>
                <div className="subtitle" id="gainersLosers">{stats.gainers} gainers, {stats.losers} losers</div>
              </div>
              <div className="stat-card-icon info">📈</div>
            </div>
          </div>
          <div className="card stat-card">
            <div className="stat-card-content">
              <div className="stat-card-text">
                <h3>Total Volume</h3>
                <div className="value" id="totalVolume">{(stats.totalVolume / 1000000).toFixed(1)}M</div>
                <div className="subtitle">Shares traded</div>
              </div>
              <div className="stat-card-icon warning">📊</div>
            </div>
          </div>
        </div>
        {/* Stock Data Table - moved above charts */}
        <div className="card">
          <h2 style={{ marginBottom: 20 }}>Stock Data</h2>
          <div className="controls">
            <div className="search-box">
              <span className="icon">🔍</span>
              <input type="text" id="searchInput" placeholder="Search symbols..." value={searchQuery} onChange={e => { setSearchQuery(e.target.value); setCurrentPage(0); }} />
            </div>
            <div className="switch-container">
              <label htmlFor="qualifiedOnly">Qualified Only</label>
              <label className="switch">
                <input type="checkbox" id="qualifiedOnly" checked={qualifiedOnly} onChange={e => { setQualifiedOnly(e.target.checked); setCurrentPage(0); }} />
                <span className="switch-slider"></span>
              </label>
            </div>
            <div className="toggle-group">
              <button className={viewMode === 'table' ? 'active' : ''} onClick={() => setViewMode('table')}>📋 Table</button>
              <button className={viewMode === 'grid' ? 'active' : ''} onClick={() => setViewMode('grid')}>⚡ Grid</button>
            </div>
            <button className="btn" style={{ marginLeft: 10 }} onClick={handleExportCSV}>
              <span role="img" aria-label="download">⬇️</span> Export CSV
            </button>
          </div>
          <div id="dataContainer">
            {renderTable(tableData)}
          </div>
        </div>
        {/* Charts Section - moved to bottom */}
        <div className="grid grid-2">
          <div className="card">
            <h2>Qualification Trend</h2>
            <div className="chart-container">
              <canvas ref={trendChartRef} height={300}></canvas>
            </div>
          </div>
          <div className="card">
            <h2>Top Movers</h2>
            {/* Top movers logic can be added here */}
          </div>
        </div>
      </main>
    </div>
  );
}

export default StockDashboard;
