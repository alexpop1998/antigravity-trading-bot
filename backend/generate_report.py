import sqlite3
import json
import os
import base64
from datetime import datetime

# --- CONFIGURATION (Dynamic Paths) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bot_data.db")
REPORT_PATH = os.path.join(BASE_DIR, "investor_report.html")

# Reset date: Start Fresh from the emergency fix (Today 08:41 UTC)
START_DATE = "2026-03-28 08:41:00"

# --- HELPER FUNCTIONS ---
def get_base64_image(path):
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def calculate_metrics(trades):
    if not trades:
        return {
            "total_pnl": 0, "win_rate": 0, "profit_factor": 0, 
            "max_drawdown": 0, "avg_trade": 0, "total_trades": 0
        }

    total_trades = len(trades)
    wins = [t for t in trades if (t['pnl'] or 0) > 0]
    losses = [t for t in trades if (t['pnl'] or 0) < 0]
    
    win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0
    total_pnl = sum(float(t['pnl'] or 0) for t in trades)
    avg_trade = total_pnl / total_trades if total_trades > 0 else 0
    
    # Profit Factor
    sum_wins = sum(float(t['pnl'] or 0) for t in wins)
    sum_losses = abs(sum(float(t['pnl'] or 0) for t in losses))
    profit_factor = round(sum_wins / sum_losses, 2) if sum_losses > 0 else (sum_wins if sum_wins > 0 else 0)

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
        "total_trades": total_trades
    }

async def async_generate():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # --- DEDUPLICATION LOGIC ---
        cursor.execute("""
            WITH DedupedTrades AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, ROUND(pnl, 2), CAST(strftime('%s', timestamp) / 60 AS INT)
                        ORDER BY timestamp ASC
                    ) as rn
                FROM trade_history
                WHERE ABS(pnl) > 0.01 AND timestamp >= ?
            )
            SELECT * FROM DedupedTrades 
            WHERE rn = 1
            ORDER BY timestamp ASC
        """, (START_DATE,))
        trades = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        metrics = calculate_metrics(trades)
        
        # Chart Data
        cumulative_pnl = 0
        chart_data = []
        for t in trades:
            cumulative_pnl += float(t['pnl'] or 0)
            chart_data.append({
                "x": t['timestamp'],
                "y": round(cumulative_pnl, 2)
            })

        html_template = f"""
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Antigravity Pro Operations - Track Record</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg: #0a0f1e;
            --card: #161d31;
            --accent: #ff9f43;
            --success: #28c76f;
            --danger: #ea5455;
            --text: #d0d2d6;
            --title: #ffffff;
            --border: #3b4253;
        }}
        body {{
            font-family: 'Outfit', sans-serif;
            background-color: var(--bg);
            color: var(--text);
            margin: 0; padding: 40px;
        }}
        .container {{ max-width: 1100px; margin: 0 auto; }}
        header {{
            display: flex; justify-content: space-between; align-items: center;
            border-bottom: 1px solid var(--border); padding-bottom: 20px; margin-bottom: 30px;
        }}
        .branding h1 {{ color: var(--accent); margin: 0; font-size: 28px; letter-spacing: -1px; }}
        .branding p {{ margin: 5px 0 0; font-size: 14px; opacity: 0.6; }}
        
        .metrics-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px; margin-bottom: 30px;
        }}
        .metric-card {{
            background: var(--card); border: 1px solid var(--border); padding: 25px;
            border-radius: 12px; text-align: center;
            transition: transform 0.2s;
        }}
        .metric-card:hover {{ transform: translateY(-3px); }}
        .metric-label {{ font-size: 12px; font-weight: 600; text-transform: uppercase; opacity: 0.5; margin-bottom: 15px; }}
        .metric-value {{ font-size: 26px; font-weight: 700; color: var(--title); }}
        
        .chart-box {{
            background: var(--card); border: 1px solid var(--border);
            padding: 30px; border-radius: 12px; margin-bottom: 30px;
        }}
        
        table {{
            width: 100%; border-collapse: collapse; background: var(--card);
            border-radius: 12px; overflow: hidden; font-size: 14px;
        }}
        th, td {{ padding: 15px; text-align: left; border-bottom: 1px solid var(--border); }}
        th {{ background: #1f2945; color: var(--title); font-weight: 600; text-transform: uppercase; font-size: 11px; }}
        
        .pnl-plus {{ color: var(--success); font-weight: 700; }}
        .pnl-minus {{ color: var(--danger); font-weight: 700; }}
        
        @media print {{
            body {{ background: #fff; color: #000; }}
            .metric-card, .chart-box, table {{ border: 1px solid #ddd; background: #fff; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="branding">
                <h1>ANTIGRAVITY PRO OPERATIONS</h1>
                <p>Institutional AI Trading Track Record | Ref: TRACK-{datetime.now().strftime('%Y%m%d')}</p>
            </div>
            <div style="text-align: right">
                <div style="color: var(--success); font-weight: 700;">● SYSTEM ONLINE</div>
                <div style="font-size: 12px; opacity: 0.5">Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            </div>
        </header>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Net Profit (USDT)</div>
                <div class="metric-value {'pnl-plus' if metrics['total_pnl'] >= 0 else 'pnl-minus'}">
                    {'+' if metrics['total_pnl'] >= 0 else ''}${metrics['total_pnl']:,.2f}
                </div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Win Rate</div>
                <div class="metric-value">{metrics['win_rate']:.1f}%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Profit Factor</div>
                <div class="metric-value" style="color: var(--accent)">{metrics['profit_factor']}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Max Drawdown</div>
                <div class="metric-value" style="color: var(--danger)">${metrics['max_drawdown']:,.2f}</div>
            </div>
        </div>

        <div class="chart-box">
            <canvas id="equityChart" height="120"></canvas>
        </div>

        <h3>Recent Activity (Post-Emergency)</h3>
        <table>
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>Asset</th>
                    <th>Side</th>
                    <th>Entry Price</th>
                    <th>Ex. Amount</th>
                    <th>Net PnL</th>
                    <th>Strategy</th>
                </tr>
            </thead>
            <tbody>
                {"".join(f'''
                <tr>
                    <td>{t['timestamp']}</td>
                    <td>{t['symbol']}</td>
                    <td style="color: {'var(--success)' if t['side'].lower() in ['buy','long'] else 'var(--danger)'}">{t['side'].upper()}</td>
                    <td>${t['price']:,.4f}</td>
                    <td>{t['amount']:,.2f}</td>
                    <td class="{'pnl-plus' if (t['pnl'] or 0) >= 0 else 'pnl-minus'}">{'+' if (t['pnl'] or 0) >= 0 else ''}${t['pnl']:,.2f}</td>
                    <td style="opacity: 0.6">{t['reason']}</td>
                </tr>
                ''' for t in reversed(trades))}
            </tbody>
        </table>
    </div>

    <script>
        const ctx = document.getElementById('equityChart').getContext('2d');
        const data = {json.dumps(chart_data)};
        
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: data.map(d => d.x.split(' ')[1]),
                datasets: [{{
                    label: 'Cumulative Equity',
                    data: data.map(d => d.y),
                    borderColor: '#ff9f43',
                    backgroundColor: 'rgba(255, 159, 67, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    x: {{ grid: {{ display: false }}, ticks: {{ color: 'rgba(255,255,255,0.3)' }} }},
                    y: {{ grid: {{ color: 'rgba(255,255,255,0.05)' }}, ticks: {{ color: 'rgba(255,255,255,0.3)' }} }}
                }}
            }}
        }});
    </script>
</body>
</html>
        """
        
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(html_template)
            
    except Exception as e:
        print(f"Error generating report: {e}")

def generate():
    import asyncio
    asyncio.run(async_generate())

if __name__ == "__main__":
    generate()
