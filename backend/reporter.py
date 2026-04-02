import sqlite3
import json
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("Reporter")

class BotReporter:
    def __init__(self, db_path="bot_data.db", report_path="investor_report.html"):
        self.db_path = db_path
        self.report_path = report_path
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.abs_report_path = os.path.join(self.base_dir, self.report_path)
        self.abs_db_path = os.path.join(self.base_dir, self.db_path)

    def _get_metric_data(self):
        try:
            with sqlite3.connect(self.abs_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Fetch Initial Balance
                cursor.execute("SELECT value FROM bot_state WHERE key='initial_balance'")
                row = cursor.fetchone()
                initial_balance = float(json.loads(row[0])) if row else 100.0
                
                # Fetch Trade History
                cursor.execute("SELECT * FROM trade_history ORDER BY timestamp ASC")
                trades = [dict(r) for r in cursor.fetchall()]
                
                # Fetch Active Positions from state
                cursor.execute("SELECT value FROM bot_state WHERE key='latest_account_data'")
                row = cursor.fetchone()
                account_data = json.loads(row[0]) if row else {}
                active_positions = account_data.get('positions', [])
                
                return initial_balance, trades, active_positions
        except Exception as e:
            logger.error(f"Error fetching report data: {e}")
            return 100.0, [], []

    def generate_html(self):
        try:
            initial_balance, raw_trades, active_positions = self._get_metric_data()
            
            # Calculate Metrics
            closed_trades = [t for t in raw_trades if t.get('pnl') is not None and abs(float(t['pnl'])) > 0.0001]
            
            total_pnl = sum(float(t['pnl']) for t in closed_trades) if closed_trades else 0.0
            roi = (total_pnl / initial_balance) * 100 if initial_balance > 0 else 0
            win_rate = (len([t for t in closed_trades if float(t['pnl']) > 0]) / len(closed_trades) * 100) if closed_trades else 0
            
            # Prepare Chart Data
            cum_pnl = 0
            chart_points = []
            for t in closed_trades:
                cum_pnl += float(t['pnl'])
                chart_points.append({
                    "x": t['timestamp'].split(' ')[1] if ' ' in t['timestamp'] else t['timestamp'],
                    "y": round(cum_pnl, 2)
                })
            
            # If no trades, add a 0 point
            if not chart_points:
                chart_points.append({"x": "N/A", "y": 0.0})

        except Exception as e:
            logger.error(f"Error in metrics calculation: {e}")
            initial_balance, total_pnl, roi, win_rate, chart_points, active_positions = 100.0, 0.0, 0.0, 0.0, [{"x": "N/A", "y": 0.0}], []

        html_template = f"""
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <title>FUTURES TRADING BOT | Institutional Report</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{ 
            --bg: #020408; 
            --card: rgba(13, 17, 23, 0.9); 
            --accent: #7367f0; 
            --accent-glow: rgba(115, 103, 240, 0.3);
            --text: #c9d1d9; 
            --title: #ffffff;
            --success: #28c76f; 
            --danger: #ea5455; 
            --border: rgba(255, 255, 255, 0.05);
        }}
        
        body {{ 
            font-family: 'Outfit', sans-serif; 
            background-color: var(--bg); 
            background-image: radial-gradient(circle at 50% 10%, rgba(115, 103, 240, 0.08), transparent 40%);
            color: var(--text); 
            margin: 0; padding: 40px; 
            min-height: 100vh;
        }}

        .container {{ max-width: 1200px; margin: 0 auto; }}

        .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 40px; }}
        .logo-text {{ font-weight: 800; font-size: 24px; letter-spacing: 1px; background: linear-gradient(to right, #fff, var(--accent)); -webkit-background-clip: text; background-clip: text; -webkit-text-fill-color: transparent; }}
        
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 25px; margin-bottom: 40px; }}
        
        .card {{ 
            background: var(--card); 
            padding: 24px; 
            border-radius: 16px; 
            border: 1px solid var(--border);
            box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            backdrop-filter: blur(10px);
            position: relative;
        }}
        .card::before {{ content: ''; position: absolute; top: 0; left: 0; width: 4px; height: 100%; background: var(--accent); border-radius: 16px 0 0 16px; opacity: 0.5; }}
        
        .card-label {{ font-size: 11px; opacity: 0.5; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 8px; }}
        .card-value {{ font-size: 28px; font-weight: 800; color: var(--title); }}
        
        .chart-container {{ background: var(--card); padding: 30px; border-radius: 16px; border: 1px solid var(--border); height: 350px; margin-bottom: 40px; }}
        
        table {{ width: 100%; border-collapse: separate; border-spacing: 0 8px; }}
        th {{ text-align: left; padding: 15px 20px; opacity: 0.5; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; }}
        td {{ padding: 18px 20px; background: rgba(255,255,255,0.02); border-top: 1px solid var(--border); border-bottom: 1px solid var(--border); }}
        td:first-child {{ border-left: 1px solid var(--border); border-radius: 8px 0 0 8px; }}
        td:last-child {{ border-right: 1px solid var(--border); border-radius: 0 8px 8px 0; }}
        
        .pos {{ color: var(--success); }} .neg {{ color: var(--danger); }}
        .badge {{ padding: 5px 12px; border-radius: 6px; font-size: 10px; font-weight: 800; background: rgba(115, 103, 240, 0.1); color: var(--accent); border: 1px solid rgba(115, 103, 240, 0.2); }}
        .status-badge {{ background: rgba(40, 199, 111, 0.1); color: var(--success); border-color: rgba(40, 199, 111, 0.2); }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <div class="logo-text">FUTURES <span>TRADING BOT</span></div>
                <p style="opacity:0.4; font-size:13px; margin:5px 0;">Digital Asset Performance Report</p>
            </div>
            <div style="text-align:right">
                <div class="badge status-badge">SYSTEM OPERATIONAL</div>
                <p style="font-size:11px; opacity:0.4; margin-top:8px">Last Sync: {datetime.now().strftime('%d %b %Y | %H:%M:%S')}</p>
            </div>
        </div>
        
        <div class="grid">
            <div class="card"><div class="card-label">Net Profit / Loss</div><div class="card-value {'pos' if total_pnl >= 0 else 'neg'}">${total_pnl:,.2f}</div></div>
            <div class="card"><div class="card-label">Return on Investment</div><div class="card-value" style="color:var(--accent)">{roi:.2f}%</div></div>
            <div class="card"><div class="card-label">Success Rate</div><div class="card-value">{win_rate:.1f}%</div></div>
            <div class="card"><div class="card-label">Current Equity</div><div class="card-value">${(initial_balance + total_pnl):,.2f}</div></div>
        </div>

        <div class="chart-container"><canvas id="pnlChart"></canvas></div>

        <h3 style="font-weight:700; letter-spacing:1px; margin-bottom:20px;">EXPOSURE: ACTIVE POSITIONS</h3>
        <table style="margin-bottom:40px">
            <thead><tr><th>Asset</th><th>Side</th><th>Entry Price</th><th>Unrealized Performance</th></tr></thead>
            <tbody>
                {"".join([f"<tr><td style='font-weight:700'>{p.get('symbol')}</td><td><span class='badge'>{p.get('side','').upper()}</span></td><td style='font-family:monospace'>${float(p.get('entryPrice',0)):,.4f}</td><td class='{'pos' if float(p.get('unrealizedPnl',0)) >= 0 else 'neg'}' style='font-weight:800'>${float(p.get('unrealizedPnl',0)):+,.2f}</td></tr>" for p in active_positions]) if active_positions else "<tr><td colspan='4' style='text-align:center; opacity:0.3; padding:40px;'>No active market exposure detected.</td></tr>"}
            </tbody>
        </table>

        <h3 style="font-weight:700; letter-spacing:1px; margin-bottom:20px;">EXECUTION HISTORY</h3>
        <table>
            <thead><tr><th>Timestamp</th><th>Symbol</th><th>Side</th><th>Closed PnL</th><th>Execution Note</th></tr></thead>
            <tbody>
                {"".join([f"<tr><td style='opacity:0.4; font-size:13px;'>{t['timestamp']}</td><td style='font-weight:700'>{t['symbol']}</td><td><span class='badge' style='background:rgba(255,255,255,0.05); color:var(--text);'>{t['side']}</span></td><td class='{'pos' if float(t['pnl']) >= 0 else 'neg'}' style='font-weight:800'>${float(t['pnl']):+,.2f}</td><td style='font-size:12px; opacity:0.6;'>{t['reason']}</td></tr>" for t in list(reversed(closed_trades))[:15]]) if closed_trades else "<tr><td colspan='5' style='text-align:center; opacity:0.3; padding:40px;'>Historical data pending synchronisation.</td></tr>"}
            </tbody>
        </table>
    </div>

    <script>
        const ctx = document.getElementById('pnlChart').getContext('2d');
        const gradient = ctx.createLinearGradient(0, 0, 0, 400);
        gradient.addColorStop(0, 'rgba(115, 103, 240, 0.2)');
        gradient.addColorStop(1, 'rgba(115, 103, 240, 0)');

        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {json.dumps([p['x'] for p in chart_points])},
                datasets: [{{
                    label: 'Cumulative Performance (USDT)',
                    data: {json.dumps([p['y'] for p in chart_points])},
                    borderColor: '#7367f0',
                    borderWidth: 3,
                    tension: 0.4,
                    pointRadius: 4,
                    pointBackgroundColor: '#7367f0',
                    fill: true,
                    backgroundColor: gradient
                }}]
            }},
            options: {{ 
                responsive: true, 
                maintainAspectRatio: false, 
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ 
                    y: {{ grid: {{ color: 'rgba(255,255,255,0.03)' }}, ticks: {{ color: 'rgba(255,255,255,0.5)', font: {{ family: 'Outfit' }} }} }},
                    x: {{ grid: {{ display: false }}, ticks: {{ color: 'rgba(255,255,255,0.5)', font: {{ family: 'Outfit' }} }} }}
                }} 
            }}
        }});
    </script>
</body>
</html>
"""
        with open(self.abs_report_path, "w", encoding="utf-8") as f:
            f.write(html_template)
        return True
