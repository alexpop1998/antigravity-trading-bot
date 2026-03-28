import sqlite3
import json
import os
import base64
from datetime import datetime

# --- CONFIGURATION (Dynamic Paths) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bot_data.db")
REPORT_PATH = os.path.join(BASE_DIR, "investor_report.html")

# Data di Reset/Inizio Report: (Default: ULTIMI 7 GIORNI per vedere tutto lo storico recente)
from datetime import timedelta
DEFAULT_START_DATE = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
START_DATE = os.getenv("REPORT_START_DATE", DEFAULT_START_DATE)
INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", 10000.0))

# --- FUNZIONI DI SUPPORTO ---
def calculate_metrics(trades):
    if not trades:
        return {
            "total_pnl": 0, "win_rate": 0, "profit_factor": 0, 
            "max_drawdown": 0, "avg_trade": 0, "total_trades": 0,
            "portfolio_roi": 0
        }

    total_trades = len(trades)
    wins = [t for t in trades if (t['pnl'] or 0) > 0]
    losses = [t for t in trades if (t['pnl'] or 0) < 0]
    
    win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0
    total_pnl = sum(float(t['pnl'] or 0) for t in trades)
    avg_trade = total_pnl / total_trades if total_trades > 0 else 0
    portfolio_roi = (total_pnl / INITIAL_CAPITAL * 100) if INITIAL_CAPITAL > 0 else 0
    
    try:
        # Profit Factor
        sum_wins = sum(float(t['pnl'] or 0) for t in wins)
        sum_losses = abs(sum(float(t['pnl'] or 0) for t in losses))
        profit_factor = round(sum_wins / sum_losses, 2) if sum_losses > 0 else (sum_wins if sum_wins > 0 else 0)
    except Exception as e:
        # Use simple fallback if math fails
        profit_factor = 0

    # Max Drawdown
    peak = 0
    max_dd = 0
    cum_pnl = 0
    for t in trades:
        cum_pnl += float(t['pnl'] or 0)
        if cum_pnl > peak:
            peak = cum_pnl
        dd = peak - cum_pnl
        if dd > max_dd:
            max_dd = dd
            
    return {
        "total_pnl": total_pnl,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "max_drawdown": max_dd,
        "avg_trade": avg_trade,
        "total_trades": total_trades,
        "portfolio_roi": portfolio_roi
    }

async def async_generate():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # --- QUERY SEMPLIFICATA (TUTTI I TRADE) ---
            print(f"🔍 [Report] Scanning trades since {START_DATE}...")
            cursor.execute("""
                SELECT * FROM trade_history 
                WHERE timestamp >= ?
                ORDER BY timestamp ASC
            """, (START_DATE,))
            trades = [dict(row) for row in cursor.fetchall()]
        
        # Pre-calculate global metrics for initial display
        print(f"📊 [Report] Processing {len(trades)} trades for analysis...")
        m = calculate_metrics(trades)
        
        # Prepare Chart Data
        cumulative_pnl = 0
        shadow_cumulative_pnl = 0
        trade_data_json = []
        open_trades_map = {} # symbol -> {real_price, side}

        for t in trades:
            try:
                symbol = t.get('symbol', 'UNKNOWN')
                side = str(t.get('side', 'N/A')).upper()
                p = float(t.get('pnl') or 0)
                real_p = float(t.get('real_price') or 0)
                
                cumulative_pnl += p
                shadow_pnl = 0
                
                # Shadow PnL Calculation Logic
                if side in ['BUY', 'SELL']:
                    open_trades_map[symbol] = {
                        'price': real_p if real_p > 0 else float(t.get('price') or 0),
                        'side': side
                    }
                elif "CLOSE" in side:
                    if symbol in open_trades_map:
                        entry_data = open_trades_map[symbol]
                        exit_price = real_p if real_p > 0 else float(t.get('price') or 0)
                        entry_price = entry_data['price']
                        
                        if entry_price > 0:
                            direction = 1 if entry_data['side'] == 'BUY' else -1
                            # Simple price-delta based PnL (ignoring leverage for clean benchmarking)
                            shadow_pnl_pct = ((exit_price - entry_price) / entry_price) * direction
                            shadow_pnl = shadow_pnl_pct * (float(t.get('amount') or 0) * entry_price)
                            shadow_cumulative_pnl += shadow_pnl
                
                trade_data_json.append({
                    "timestamp": str(t.get('timestamp', 'N/A')),
                    "symbol": symbol,
                    "side": side,
                    "price": float(t.get('price') or 0),
                    "amount": float(t.get('amount') or 0),
                    "pnl": p,
                    "shadow_pnl": round(shadow_pnl, 2),
                    "pnl_pct": float(t.get('pnl_pct') or 0),
                    "reason": str(t.get('reason', 'N/A')),
                    "cum_pnl": round(cumulative_pnl, 2),
                    "shadow_cum_pnl": round(shadow_cumulative_pnl, 2)
                })
            except Exception as loop_e:
                print(f"⚠️ [Report] Skipping malformed trade row for {t.get('symbol')}: {loop_e}")

        html_template = f"""
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Futures Bot - Performance Report</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg: #0a0f1e; --card: #161d31; --accent: #ff9f43; --success: #28c76f;
            --danger: #ea5455; --text: #d0d2d6; --title: #ffffff; --border: #3b4253;
        }}
        body {{ font-family: 'Outfit', sans-serif; background-color: var(--bg); color: var(--text); margin: 0; padding: 30px; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        
        header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 20px; margin-bottom: 30px; }}
        .branding h1 {{ color: var(--accent); margin: 0; font-size: 26px; }}
        .branding p {{ margin: 5px 0 0; font-size: 13px; opacity: 0.6; }}

        .filter-bar {{ display: flex; gap: 10px; margin-bottom: 20px; }}
        .filter-btn {{
            background: var(--card); border: 1px solid var(--border); color: var(--text);
            padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; transition: 0.2s;
        }}
        .filter-btn:hover, .filter-btn.active {{ background: var(--accent); color: var(--bg); border-color: var(--accent); font-weight: 600; }}

        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 30px; }}
        .metric-card {{ background: var(--card); border: 1px solid var(--border); padding: 20px; border-radius: 12px; text-align: center; }}
        .metric-label {{ font-size: 11px; font-weight: 600; text-transform: uppercase; opacity: 0.5; margin-bottom: 10px; }}
        .metric-value {{ font-size: 22px; font-weight: 700; color: var(--title); }}
        
        .main-chart {{ background: var(--card); border: 1px solid var(--border); padding: 25px; border-radius: 12px; margin-bottom: 30px; }}
        
        table {{ width: 100%; border-collapse: collapse; background: var(--card); border-radius: 12px; overflow: hidden; font-size: 13px; }}
        th, td {{ padding: 12px 15px; text-align: left; border-bottom: 1px solid var(--border); }}
        th {{ background: #1f2945; color: var(--title); font-size: 11px; text-transform: uppercase; }}
        
        .pnl-pos {{ color: var(--success); font-weight: 700; }}
        .pnl-neg {{ color: var(--danger); font-weight: 700; }}
        .roi-badge {{ padding: 2px 6px; border-radius: 4px; font-weight: 600; font-size: 11px; }}
        .roi-pos {{ background: rgba(40, 199, 111, 0.15); color: var(--success); }}
        .roi-neg {{ background: rgba(234, 84, 85, 0.15); color: var(--danger); }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="branding">
                <h1>TRADING FUTURES BOT</h1>
                <p>Track Record Real-Time Asset Management | Capitale Base: ${INITIAL_CAPITAL:,.0f} USDT</p>
            </div>
            <div style="text-align: right">
                <div style="color: var(--success); font-weight: 700; font-size: 12px;">● LIVE MONITORING</div>
                <div style="font-size: 11px; opacity: 0.5">Ultimo Aggiornamento: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</div>
            </div>
        </header>

        <div class="filter-bar">
            <button class="filter-btn active" onclick="filterData('all', this)">Tutto</button>
            <button class="filter-btn" onclick="filterData('today', this)">Oggi</button>
            <button class="filter-btn" onclick="filterData('7d', this)">Ultimi 7 Giorni</button>
            <button class="filter-btn" onclick="filterData('30d', this)">Ultimi 30 Giorni</button>
        </div>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Profitto Netto (USDT)</div>
                <div id="stat-pnl" class="metric-value pnl-pos">$0.00</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Rendimento Portafoglio %</div>
                <div id="stat-roi" class="metric-value" style="color: var(--accent)">0.00%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Win Rate</div>
                <div id="stat-winrate" class="metric-value">0.0%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Profit Factor</div>
                <div id="stat-pf" class="metric-value">0.00</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Max Drawdown</div>
                <div id="stat-mdd" class="metric-value" style="color: var(--danger)">$0.00</div>
            </div>
        </div>        <div class="main-chart">
            <h4 style="margin: 0 0 20px 0; color: var(--title); font-weight: 400;">Analisi Equity Curve</h4>
            <div style="height: 300px; position: relative;">
                <canvas id="equityChart"></canvas>
            </div>
        </div>

        <h3>Attività Recente</h3>
        <table id="trade-table">
            <thead>
                <tr>
                    <th>Data (UTC)</th>
                    <th>Asset</th>
                    <th>Lat.</th>
                    <th>Entry (Test/Real)</th>
                    <th>PnL Netto (Testnet)</th>
                    <th>PnL Shadow (Mainnet)</th>
                    <th>ROI %</th>
                    <th>Strategia</th>
                </tr>
            </thead>
            <tbody id="table-body">
                <!-- Data populated by JS -->
            </tbody>
        </table>
    </div>

    <script>
        const INITIAL_CAPITAL = {INITIAL_CAPITAL};
        const rawData = {json.dumps(trade_data_json)};
        let activeFilter = 'all';
        let chart = null;

        function updateMetrics(data) {{
            if (data.length === 0) {{
                document.getElementById('stat-pnl').innerText = "$0.00";
                document.getElementById('stat-roi').innerText = "0.00%";
                document.getElementById('stat-winrate').innerText = "0.0%";
                document.getElementById('stat-pf').innerText = "0.00";
                document.getElementById('stat-mdd').innerText = "$0.00";
                return;
            }}

            const totalPnl = data.reduce((sum, t) => sum + t.pnl, 0);
            const wins = data.filter(t => t.pnl > 0);
            const losses = data.filter(t => t.pnl < 0);
            const winRate = (wins.length / data.length * 100).toFixed(1);
            const portfolioRoi = (totalPnl / INITIAL_CAPITAL * 100).toFixed(2);
            
            const sumWins = wins.reduce((sum, t) => sum + t.pnl, 0);
            const sumLosses = Math.abs(losses.reduce((sum, t) => sum + t.pnl, 0));
            const pf = sumLosses > 0 ? (sumWins / sumLosses).toFixed(2) : sumWins.toFixed(2);

            let peak = 0; let mdd = 0; let cum = 0;
            data.forEach(t => {{
                cum += t.pnl;
                if (cum > peak) peak = cum;
                let dd = peak - cum;
                if (dd > mdd) mdd = dd;
            }});

            const pnlEl = document.getElementById('stat-pnl');
            pnlEl.innerText = (totalPnl >= 0 ? '+' : '') + '$' + totalPnl.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
            pnlEl.className = 'metric-value ' + (totalPnl >= 0 ? 'pnl-pos' : 'pnl-neg');
            
            document.getElementById('stat-roi').innerText = (totalPnl >= 0 ? '+' : '') + portfolioRoi + '%';
            document.getElementById('stat-winrate').innerText = winRate + '%';
            document.getElementById('stat-pf').innerText = pf;
            document.getElementById('stat-mdd').innerText = '$' + mdd.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
        }}

        function updateTable(data) {{
            const tbody = document.getElementById('table-body');
            tbody.innerHTML = '';
            
            [...data].reverse().forEach(t => {{
                const row = document.createElement('tr');
                const roiClass = t.pnl_pct >= 0 ? 'roi-pos' : 'roi-neg';
                const pnlClass = t.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
                
                row.innerHTML = `
                    <td>${{t.timestamp}}</td>
                    <td><strong>${{t.symbol}}</strong></td>
                    <td style="color: ${{t.side === 'BUY' || t.side === 'LONG' || t.side === 'SHORT' ? (t.side === 'SHORT' ? 'var(--danger)' : 'var(--success)') : 'var(--text)'}}">${{t.side}}</td>
                    <td style="font-size: 11px;">$${{t.price.toFixed(4)}} / $${{(t.real_price || 0).toFixed(4)}}</td>
                    <td class="${{pnlClass}}">${{t.pnl >= 0 ? '+' : ''}}$${{t.pnl.toFixed(2)}}</td>
                    <td style="color: var(--success); font-weight: bold;">${{t.shadow_pnl >= 0 ? '+' : ''}}$${{t.shadow_pnl.toFixed(2)}}</td>
                    <td><span class="roi-badge ${{roiClass}}">${{t.pnl_pct >= 0 ? '+' : ''}}${{(t.pnl_pct).toFixed(2)}}%</span></td>
                    <td style="opacity: 0.6; font-size: 11px;">${{t.reason}}</td>
                `;
                tbody.appendChild(row);
            }});
        }}

        function updateChart(data) {{
            const canvas = document.getElementById('equityChart');
            if (!canvas) return;
            const ctx = canvas.getContext('2d');
            
            let cum = 0;
            let shadowCum = 0;
            const labels = [];
            const testnetData = [];
            const shadowData = [];

            data.forEach(t => {{
                cum += t.pnl;
                shadowCum += (t.shadow_pnl || 0);
                labels.push(t.timestamp.includes(' ') ? t.timestamp.split(' ')[1] : t.timestamp);
                testnetData.push(cum.toFixed(2));
                shadowData.push(shadowCum.toFixed(2));
            }});

            if (chart) chart.destroy();

            chart = new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: labels,
                    datasets: [
                        {{
                            label: 'Shadow Performance (REAL MARKET)',
                            data: shadowData,
                            borderColor: '#28c76f',
                            backgroundColor: 'rgba(40, 199, 111, 0.1)',
                            borderWidth: 3,
                            fill: true,
                            tension: 0.2,
                            pointRadius: data.length > 50 ? 0 : 3
                        }},
                        {{
                            label: 'Testnet Balance (SANDBOX)',
                            data: testnetData,
                            borderColor: '#ea5455',
                            borderWidth: 2,
                            fill: false,
                            tension: 0.2,
                            pointRadius: 0
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{ 
                        legend: {{ display: false }},
                        tooltip: {{
                            backgroundColor: '#161d31',
                            titleColor: '#fff',
                            bodyColor: '#ff9f43',
                            borderColor: '#3b4253',
                            borderWidth: 1,
                            padding: 10,
                            displayColors: false,
                            callbacks: {{
                                label: (ctx) => `PnL: $${{ctx.raw}}`
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{ 
                            grid: {{ display: false }}, 
                            ticks: {{ color: 'rgba(255,255,255,0.4)', font: {{ size: 9 }}, maxRotation: 45 }} 
                        }},
                        y: {{ 
                            grid: {{ color: 'rgba(255,255,255,0.05)' }}, 
                            ticks: {{ color: 'rgba(255,255,255,0.4)', font: {{ size: 9 }} }} 
                        }}
                    }}
                }}
            }});
        }}
}}

        function filterData(type, btn) {{
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            const now = new Date();
            let filtered = rawData;

            if (type === 'today') {{
                const todayStr = now.toISOString().split('T')[0];
                filtered = rawData.filter(t => t.timestamp.startsWith(todayStr));
            }} else if (type === '7d') {{
                const cutoff = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
                filtered = rawData.filter(t => new Date(t.timestamp.replace(' ', 'T')) > cutoff);
            }} else if (type === '30d') {{
                const cutoff = new Date(now.getTime() - (30 * 24 * 60 * 60 * 1000));
                filtered = rawData.filter(t => new Date(t.timestamp.replace(' ', 'T')) > cutoff);
            }}

            updateMetrics(filtered);
            updateTable(filtered);
            updateChart(filtered);
        }}

        // Initial Load
        filterData('all', document.querySelector('.filter-btn.active'));
    </script>
</body>
</html>
        """
        
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(html_template)
            
    except Exception as e:
        import traceback
        print(f"❌ [Report] FATAL ERROR generating report: {str(e)}")
        traceback.print_exc()

def generate():
    import asyncio
    asyncio.run(async_generate())

if __name__ == "__main__":
    generate()
