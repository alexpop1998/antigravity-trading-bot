import sqlite3
import json
import os
from datetime import datetime

import base64

DB_PATH = "/Users/alex/.gemini/antigravity/scratch/trading-terminal/backend/bot_data.db"
REPORT_PATH = "/Users/alex/.gemini/antigravity/scratch/trading-terminal/backend/investor_report.html"
LOGO_PATH = "/Users/alex/.gemini/antigravity/brain/0b75080c-71a0-40b4-8486-2c83a099fc0e/antigravity_trading_logo_gold_1774209871944.png"

def get_base64_image(path):
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')


async def async_generate():
    logo_base64 = get_base64_image(LOGO_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # --- DEDUPLICATION LOGIC ---
    cursor.execute("""
        WITH DedupedTrades AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, ROUND(pnl, 2), CAST(strftime('%s', timestamp) / 60 AS INT)
                    ORDER BY CASE WHEN reason = 'BINANCE' THEN 0 ELSE 1 END, timestamp ASC
                ) as rn
            FROM trade_history
            WHERE ABS(pnl) > 0.01
        )
        SELECT * FROM DedupedTrades 
        WHERE rn = 1
        ORDER BY timestamp ASC
    """)
    trades = [dict(row) for row in cursor.fetchall()]
    
    if not trades:
        print("No trades found in database.")
        return

    total_trades = len(trades)
    winning_trades = [t for t in trades if (t['pnl'] or 0) > 0]
    win_rate = (len(winning_trades) / total_trades * 100) if total_trades > 0 else 0
    total_pnl = sum(float(t['pnl'] or 0) for t in trades)
    avg_trade = total_pnl / total_trades if total_trades > 0 else 0
    
    cumulative_pnl = 0
    chart_data = []
    for t in trades:
        cumulative_pnl += float(t['pnl'] or 0)
        chart_data.append({
            "x": t['timestamp'],
            "y": round(cumulative_pnl, 2)
        })


    # Load Logo (if exists)
    # Note: In a real scenario we'd use the actual path, but for HTML we'll just reference it
    
    html_template = f"""
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Antigravity Trading - Investor Report</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg: #0f172a;
            --card-bg: #1e293b;
            --text: #f8fafc;
            --accent: #fbbf24;
            --success: #10b981;
            --danger: #ef4444;
            --border: #334155;
        }}
        body {{
            font-family: 'Inter', sans-serif;
            background-color: var(--bg);
            color: var(--text);
            margin: 0;
            padding: 40px;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1000px;
            margin: 0 auto;
        }}
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid var(--border);
            padding-bottom: 20px;
            margin-bottom: 40px;
        }}
        .logo-area {{
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        .logo {{
            width: 60px;
            height: 60px;
            border-radius: 8px;
            object-fit: cover;
        }}
        .title-area h1 {{
            margin: 0;
            font-size: 24px;
            letter-spacing: -0.5px;
            color: var(--accent);
        }}
        .title-area p {{
            margin: 5px 0 0;
            opacity: 0.6;
            font-size: 14px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin-bottom: 40px;
        }}
        .stat-card {{
            background: var(--card-bg);
            padding: 20px;
            border-radius: 12px;
            border: 1px solid var(--border);
            text-align: center;
        }}
        .stat-card .label {{
            font-size: 12px;
            text-transform: uppercase;
            opacity: 0.6;
            margin-bottom: 10px;
            letter-spacing: 1px;
        }}
        .stat-card .value {{
            font-size: 24px;
            font-weight: 700;
        }}
        .chart-container {{
            background: var(--card-bg);
            padding: 25px;
            border-radius: 12px;
            border: 1px solid var(--border);
            margin-bottom: 40px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--card-bg);
            border-radius: 12px;
            overflow: hidden;
            font-size: 13px;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        th {{
            background: #1e293b;
            color: var(--accent);
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 1px;
        }}
        .side-buy {{ color: var(--success); font-weight: 600; }}
        .side-sell {{ color: var(--danger); font-weight: 600; }}
        .pnl-pos {{ color: var(--success); font-weight: 700; }}
        .pnl-neg {{ color: var(--danger); font-weight: 700; }}
        
        @media print {{
            body {{ background: #fff; color: #000; padding: 20px; }}
            .stat-card, .chart-container, table {{ border: 1px solid #ddd; background: #fff; page-break-inside: avoid; }}
            th {{ background: #eee; color: #000; }}
            .pnl-pos {{ color: #059669; }}
            .pnl-neg {{ color: #dc2626; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo-area">
                <img src="data:image/png;base64,{logo_base64}" class="logo" alt="Antigravity Logo">
                <div class="title-area">
                    <h1>ANTIGRAVITY TRADING SYSTEMS</h1>
                    <p>Performance Report | {datetime.now().strftime('%d %B %Y')}</p>
                </div>
            </div>
            <div style="text-align: right">
                <div style="font-size: 12px; opacity: 0.6">REPORT ID</div>
                <div style="font-weight: 600">#{datetime.now().strftime('%Y%m%d')}-001</div>
            </div>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="label">Historical Net PnL</div>
                <div class="value pnl-{'pos' if total_pnl >= 0 else 'neg'}">{'+' if total_pnl >= 0 else ''}${total_pnl:,.2f}</div>
            </div>
            <div class="stat-card">
                <div class="label">Win Rate</div>
                <div class="value">{win_rate:.1f}%</div>
            </div>
            <div class="stat-card">
                <div class="label">Closed Positions</div>
                <div class="value">{total_trades}</div>
            </div>
            <div class="stat-card">
                <div class="label">Avg Profit/Trade</div>
                <div class="value pnl-{'pos' if avg_trade >= 0 else 'neg'}">{'+' if avg_trade >= 0 else ''}${avg_trade:,.2f}</div>
            </div>
        </div>


        <div class="chart-container">
            <h3 style="margin-top: 0; color: var(--accent); font-weight: 400;">Equity Curve (Cumulative PnL)</h3>
            <canvas id="equityChart" height="100"></canvas>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Price</th>
                    <th>Amount</th>
                    <th>PnL</th>
                    <th>Reason</th>
                </tr>
            </thead>
            <tbody>
                {"".join(f'''
                <tr>
                    <td>{t['timestamp']}</td>
                    <td>{t['symbol']}</td>
                    <td class="side-{t['side'].lower()}">{t['side'].upper()}</td>
                    <td>${t['price']:,.4f}</td>
                    <td>{t['amount']:,.2f}</td>
                    <td class="pnl-{'pos' if (t['pnl'] or 0) >= 0 else 'neg'}">{'+' if (t['pnl'] or 0) >= 0 else ''}${t['pnl']:,.2f}</td>
                    <td style="opacity: 0.6">{t['reason']}</td>
                </tr>
                ''' for t in reversed(trades))}
            </tbody>
        </table>
        
        <footer style="margin-top: 50px; text-align: center; font-size: 10px; opacity: 0.4;">
            © 2026 Antigravity Trading. Proprietary and Confidential. For information purposes only.
        </footer>
    </div>

    <script>
        const ctx = document.getElementById('equityChart').getContext('2d');
        const data = {json.dumps(chart_data)};
        
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: data.map(d => d.x.split(' ')[1]),
                datasets: [{{
                    label: 'Cumulative PnL (USDT)',
                    data: data.map(d => d.y),
                    borderColor: '#fbbf24',
                    backgroundColor: 'rgba(251, 191, 36, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    x: {{ grid: {{ display: false }}, ticks: {{ color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }},
                    y: {{ grid: {{ color: 'rgba(255,255,255,0.05)' }}, ticks: {{ color: 'rgba(255,255,255,0.4)', font: {{ size: 10 }} }} }}
                }}
            }}
        }});
    </script>
</body>
</html>
    """
    
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html_template)
    print("Report generated successfully: {REPORT_PATH}")

def generate():
    import asyncio
    asyncio.run(async_generate())

if __name__ == "__main__":
    generate()
