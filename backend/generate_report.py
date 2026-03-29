import sqlite3
import json
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bot_data.db")
REPORT_PATH = os.path.join(BASE_DIR, "investor_report.html")

# START_DATE fissata a OGGI (29/03/2026) per escludere vecchi test (v9.8.2)
START_DATE = "2026-03-29 00:00:00"
INITIAL_CAPITAL = 10127.96

def _load_dynamic_config():
    global INITIAL_CAPITAL, START_DATE
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # 1. Load Initial Capital
            cursor.execute("SELECT value FROM bot_state WHERE key='initial_balance'")
            row = cursor.fetchone()
            if row: 
                val = json.loads(row[0])
                INITIAL_CAPITAL = float(val) if val else INITIAL_CAPITAL
            
            # 2. Load Start Date (Clean Session Start)
            cursor.execute("SELECT value FROM bot_state WHERE key='report_start_date'")
            row = cursor.fetchone()
            if row: 
                START_DATE = row[0].strip('"') or START_DATE
    except:
        pass

def calculate_metrics(trades):
    _load_dynamic_config()
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
    portfolio_roi = (total_pnl / INITIAL_CAPITAL * 100) if INITIAL_CAPITAL > 0 else 0
    
    try:
        sum_wins = sum(float(t['pnl'] or 0) for t in wins)
        sum_losses = abs(sum(float(t['pnl'] or 0) for t in losses))
        profit_factor = round(sum_wins / sum_losses, 2) if sum_losses > 0 else (sum_wins if sum_wins > 0 else 0.0)
    except:
        profit_factor = 0.0

    peak = INITIAL_CAPITAL
    max_dd = 0
    running_equity = INITIAL_CAPITAL
    for t in trades:
        running_equity += float(t['pnl'] or 0)
        if running_equity > peak: peak = running_equity
        dd = peak - running_equity
        if dd > max_dd: max_dd = dd
            
    return {
        "total_pnl": total_pnl, "win_rate": win_rate, "profit_factor": profit_factor,
        "max_drawdown": max_dd, "total_trades": total_trades, "portfolio_roi": portfolio_roi
    }

async def async_generate():
    """Wrapper for bot.py compatibility"""
    return generate()

def generate():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # Ripristinato in v9.8.2: Filtra per data d'inizio (Oggi 29/03)
            cursor.execute("SELECT * FROM trade_history WHERE timestamp >= ? ORDER BY timestamp ASC", (START_DATE,))
            rows = cursor.fetchall()
        
        cumulative_pnl = 0
        shadow_cumulative_pnl = 0
        trade_data_json = []
        open_trades_map = {}

        for row in rows:
            t = dict(row)
            symbol = t.get('symbol', 'UNKNOWN')
            side = str(t.get('side', 'N/A')).upper()
            raw_p = t.get('pnl')
            p = float(raw_p) if raw_p is not None else 0.0
            
            # LOGIC: Shadow PnL still needs entries to calculate benchmark
            price = float(t.get('price') or 0)
            real_p = float(t.get('real_price') or 0)
            if any(x in side for x in ['BUY', 'SELL', 'LONG', 'SHORT']) and "CLOSE" not in side and p == 0:
                open_trades_map[symbol] = {'price': real_p if real_p > 0 else price, 'side': side, 'amount': float(t.get('amount') or 0)}
            
            # FILTRO VISUALIZZAZIONE (v9.7.5): In tabella se CHIUSURA (parola CLOSE) o PnL rilevato (>0.05)
            # Questo permette di vedere anche i trade manuali sincronizzati da Binance
            is_close = "CLOSE" in side or abs(p) > 0.01
            if not is_close: continue

            cumulative_pnl += p
            
            # Shadow PnL calculation on close
            shadow_pnl = 0
            if symbol in open_trades_map:
                entry = open_trades_map[symbol]
                exit_p = real_p if real_p > 0 else price
                if entry['price'] > 0:
                    dir = 1 if entry['side'] in ['BUY', 'LONG'] else -1
                    shadow_pnl = ((exit_p - entry['price']) / entry['price']) * dir * (entry['amount'] * entry['price']) * 10
                    shadow_cumulative_pnl += shadow_pnl
            
            trade_data_json.append({
                "timestamp": str(t.get('timestamp', 'N/A')),
                "symbol": symbol, "side": side, "price": price,
                "pnl": p, "pnl_pct": float(t.get('pnl_pct') or 0),
                "reason": str(t.get('reason', 'N/A')),
                "cum_pnl": round(cumulative_pnl, 2),
                "shadow_cum_pnl": round(shadow_cumulative_pnl, 2)
            })

        m = calculate_metrics(trade_data_json)

        html_template = f"""
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FUTURES TRADING BOT</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        :root {{
            --bg: #020408; --card: rgba(13, 17, 23, 0.8); --accent: #7367f0; 
            --success: #28c76f; --danger: #ea5455; --text: #c9d1d9; --title: #ffffff;
            --border: rgba(255, 255, 255, 0.05);
        }}
        body {{ 
            font-family: 'Outfit', sans-serif; background-color: var(--bg); color: var(--text); 
            margin: 0; padding: 30px; min-height: 100vh;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; }}
        .branding h1 {{ color: var(--title); margin: 0; font-size: 28px; letter-spacing: -1px; }}
        .branding h1 span {{ color: var(--accent); }}
        
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }}
        .metric-card {{ background: var(--card); border: 1px solid var(--border); padding: 20px; border-radius: 12px; }}
        .metric-label {{ font-size: 11px; font-weight: 600; text-transform: uppercase; opacity: 0.5; margin-bottom: 10px; }}
        .metric-value {{ font-size: clamp(20px, 4vw, 26px); font-weight: 700; color: var(--title); }}
        
        .chart-box {{ background: var(--card); border: 1px solid var(--border); padding: 25px; border-radius: 15px; height: 350px; margin-bottom: 30px; }}
        .table-box {{ background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px; overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        th {{ padding: 12px; text-align: left; opacity: 0.4; font-size: 10px; text-transform: uppercase; border-bottom: 1px solid var(--border); }}
        td {{ padding: 12px; border-bottom: 1px solid rgba(255,255,255,0.02); }}
        
        .pos {{ color: var(--success); }} .neg {{ color: var(--danger); }}
        .badge {{ padding: 3px 8px; border-radius: 4px; font-size: 10px; font-weight: 700; background: rgba(115,103,240,0.1); color: var(--accent); }}

        @media (max-width: 768px) {{
            body {{ padding: 15px; }}
            header {{ flex-direction: column; align-items: flex-start; gap: 10px; }}
            .metrics-grid {{ grid-template-columns: repeat(2, 1fr); }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="branding">
                <h1>FUTURES <span>TRADING BOT</span></h1>
                <p style="font-size: 12px; opacity: 0.5; margin: 5px 0 0;">Report Istituzionale | Da {START_DATE}</p>
            </div>
            <div style="text-align: right">
                <div style="font-size: 12px; font-weight: 600; color: var(--success);"><span style="width:8px;height:8px;background:var(--success);border-radius:50%;display:inline-block;margin-right:5px;"></span> OPERATIVO</div>
                <div style="font-size: 11px; opacity: 0.4; margin-top: 3px">Aggiornato: {datetime.now().strftime('%H:%M:%S')}</div>
            </div>
        </header>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Profitto Chiuso (USDT)</div>
                <div class="metric-value { 'pos' if m['total_pnl'] >= 0 else 'neg' }">
                    {'+' if m['total_pnl'] >= 0 else ''}${m['total_pnl']:.2f}
                </div>
            </div>
            <div class="metric-card">
                <div class="metric-label">ROI Sessione %</div>
                <div class="metric-value" style="color: var(--accent)">{m['portfolio_roi']:.2f}%</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Trades Conclusi</div>
                <div class="metric-value">{m['total_trades']}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Drawdown Max</div>
                <div class="metric-value" style="color: var(--danger)">-${m['max_drawdown']:.2f}</div>
            </div>
        </div>

        <div class="chart-box">
            <canvas id="mainChart"></canvas>
        </div>

        <div class="table-box">
            <h4 style="margin: 0 0 15px 10px;">Cronologia Operazioni Concluse</h4>
            <table>
                <thead>
                    <tr>
                        <th>Orario</th><th>Strumento</th><th>Tipo</th><th>Uscita</th><th>Profitto</th><th>ROI %</th><th>Analisi AI</th>
                    </tr>
                </thead>
                <tbody id="table-body"></tbody>
            </table>
        </div>
    </div>

    <script>
        const tradeData = {json.dumps(trade_data_json)};
        if(tradeData.length === 0) {{
            tradeData.push({{
                timestamp: "{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                symbol: "ATTESA...", side: "N/A", price: 0, 
                pnl: 0, pnl_pct: 0, reason: "In attesa della prima operazione della sessione...",
                cum_pnl: 0, shadow_cum_pnl: 0
            }});
        }}
        
        const tbody = document.getElementById('table-body');
        tbody.innerHTML = [...tradeData].reverse().slice(0, 100).map(t => {{
            const pClass = t.pnl >= 0 ? 'pos' : 'neg';
            return `<tr>
                <td style="opacity:0.5">${{t.timestamp.split(' ')[1]}}</td>
                <td style="font-weight:600">${{t.symbol.split('/')[0]}}</td>
                <td><span class="badge">${{t.side}}</span></td>
                <td>$${{t.price.toFixed(4)}}</td>
                <td class="${{pClass}}" style="font-weight:700">${{t.pnl >= 0 ? '+' : ''}}$${{t.pnl.toFixed(2)}}</td>
                <td class="${{pClass}}">${{t.pnl_pct >= 0 ? '+' : ''}}${{t.pnl_pct.toFixed(2)}}%</td>
                <td style="opacity:0.6; font-size:11px">${{t.reason}}</td>
            </tr>`;
        }}).join('');

        const ctx = document.getElementById('mainChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: tradeData.map(t => t.timestamp.split(' ')[1]),
                datasets: [
                    {{ label: 'Portafoglio', data: tradeData.map(t => t.cum_pnl), borderColor: '#7367f0', borderWidth: 3, tension: 0.4, fill: false, pointRadius: 0 }},
                    {{ label: 'Shadow (Mainnet)', data: tradeData.map(t => t.shadow_cum_pnl), borderColor: '#28c76f', borderDash: [5,5], borderWidth: 2, tension: 0.4, fill: false, pointRadius: 0 }}
                ]
            }},
            options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ display: false }}, y: {{ grid: {{ color: 'rgba(255,255,255,0.05)' }} }} }} }}
        }});
    </script>
</body>
</html>
        """
        with open(REPORT_PATH, "w", encoding="utf-8") as f: f.write(html_template)
        return True
    except Exception as e:
        print(f"Error: {e}"); return False

if __name__ == "__main__": generate()
