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
    <title>Antigravity Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{ --bg: #0b0e11; --card: #1e2329; --accent: #fcd535; --text: #eaecef; --success: #0ecb81; --danger: #f6465d; }}
        body {{ font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .card {{ background: var(--card); padding: 20px; border-radius: 12px; border-left: 4px solid var(--accent); }}
        .card-label {{ font-size: 12px; opacity: 0.6; text-transform: uppercase; }}
        .card-value {{ font-size: 24px; font-weight: 800; margin-top: 5px; }}
        .chart-container {{ background: var(--card); padding: 20px; border-radius: 12px; height: 300px; margin-bottom: 30px; }}
        table {{ width: 100%; border-collapse: collapse; background: var(--card); border-radius: 12px; overflow: hidden; }}
        th {{ text-align: left; padding: 15px; opacity: 0.6; font-size: 12px; background: rgba(255,255,255,0.05); }}
        td {{ padding: 15px; border-bottom: 1px solid rgba(255,255,255,0.05); }}
        .pos {{ color: var(--success); }} .neg {{ color: var(--danger); }}
        .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 10px; font-weight: 800; background: rgba(252,213,53,0.1); color: var(--accent); }}
    </style>
</head>
<body>
    <div class="header">
        <div><h1 style="margin:0">ANTIGRAVITY <span>v31.0</span></h1><p style="opacity:0.5;margin:5px 0">Institutional Crypto Portfolio</p></div>
        <div style="text-align:right"><div class="badge">LIVE TRADING ACTIVE</div><p style="font-size:11px;margin-top:5px">Updated: {datetime.now().strftime('%H:%M:%S')}</p></div>
    </div>
    
    <div class="grid">
        <div class="card"><div class="card-label">Total PnL</div><div class="card-value {'pos' if total_pnl >= 0 else 'neg'}">${total_pnl:.2f}</div></div>
        <div class="card"><div class="card-label">ROI %</div><div class="card-value" style="color:var(--accent)">{roi:.2f}%</div></div>
        <div class="card"><div class="card-label">Win Rate</div><div class="card-value">{win_rate:.1f}%</div></div>
        <div class="card"><div class="card-label">Initial Balance</div><div class="card-value">${initial_balance:.2f}</div></div>
    </div>

    <div class="chart-container"><canvas id="pnlChart"></canvas></div>

    <h3>Active Positions</h3>
    <table style="margin-bottom:30px">
        <thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Unrealized PnL</th></tr></thead>
        <tbody>
            {"".join([f"<tr><td>{p.get('symbol')}</td><td><span class='badge'>{p.get('side','').upper()}</span></td><td>${float(p.get('entryPrice',0)):.4f}</td><td class='{'pos' if float(p.get('unrealizedPnl',0)) >= 0 else 'neg'}'>${float(p.get('unrealizedPnl',0)):.2f}</td></tr>" for p in active_positions]) if active_positions else "<tr><td colspan='4' style='text-align:center;opacity:0.5'>None</td></tr>"}
        </tbody>
    </table>

    <h3>Trade History</h3>
    <table>
        <thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>PnL</th><th>Reason</th></tr></thead>
        <tbody>
            {"".join([f"<tr><td style='opacity:0.5'>{t['timestamp'].split(' ')[1] if ' ' in t['timestamp'] else t['timestamp']}</td><td>{t['symbol']}</td><td>{t['side']}</td><td class='{'pos' if float(t['pnl']) >= 0 else 'neg'}'>${float(t['pnl']):.2f}</td><td style='font-size:11px;opacity:0.7'>{t['reason']}</td></tr>" for t in list(reversed(closed_trades))[:10]])}
        </tbody>
    </table>

    <script>
        const ctx = document.getElementById('pnlChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {json.dumps([p['x'] for p in chart_points])},
                datasets: [{{
                    label: 'Cumulative PnL',
                    data: {json.dumps([p['y'] for p in chart_points])},
                    borderColor: '#fcd535',
                    borderWidth: 3,
                    tension: 0.4,
                    pointRadius: 4,
                    fill: true,
                    backgroundColor: 'rgba(252,213,53,0.05)'
                }}]
            }},
            options: {{ responsive: true, maintainAspectRatio: false, scales: {{ y: {{ grid: {{ color: 'rgba(255,255,255,0.05)' }} }} }} }}
        }});
    </script>
</body>
</html>
"""
        with open(self.abs_report_path, "w", encoding="utf-8") as f:
            f.write(html_template)
        return True
