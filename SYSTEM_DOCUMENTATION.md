# 🛰️ Autonomous AI Trading Bot: Technical Documentation

## 1. Visione d'Insieme (Core Philosophy)
Il software è un **Agente di Trading Autonomo** progettato per operare in mercati ad alta volatilità (Crypto USDT-M Futures). La sua filosofia si basa sulla **Consensus Intelligence**: nessuna operazione viene eseguita basandosi su un singolo indicatore, ma solo quando una tripla barriera di analisi (Tecnica, Quantitativa e AI Strategica) converge verso la stessa direzione.

---

## 2. Architettura Modulare (Componenti)

Il sistema è diviso in moduli indipendenti che collaborano in tempo reale:

### A. Core Orchestrator (`bot.py`)
È il "cervello" operativo. Gestisce:
- La connessione con l'Exchange (Binance/Bitget) tramite CCXT.
- Il mantenimento dello stato interno delle posizioni e dei livelli di prezzo.
- I cicli di monitoraggio (HFT, MTF, AI Audit).
- La sincronizzazione delle posizioni "zombie" (recupero di trade aperti altrove).

### B. Strategic AI Analyst (`llm_analyst.py`)
Utilizza **Google Gemini** come decisore finale. Il suo ruolo non è generare segnali tecnici, ma agire come un **Hedge Fund Risk Manager**.
- **Refinement**: Filtra i simboli più promettenti scansionati dal mercato.
- **Veto/Approval**: Analizza il contesto macro, news e trend prima di confermare un'entrata.
- **Active Audit**: Monitora le posizioni aperte decidendo se chiuderle in anticipo, scalarle (50%) o ruotarle (Pivot).

### C. Market Intelligence Layer
- **AssetScanner (`asset_scanner.py`)**: Ogni 4 ore scansiona centinaia di coppie cercando le top 50 per mix di volume (>30M) e volatilità.
- **NewsRadar (`news_radar.py`)**: Filtra news RSS in tempo reale (RSS) usando l'IA per determinare se un evento è "Market Moving".
- **SignalManager (`signal_manager.py`)**: Riceve input da indicatori e ML, assegna pesi e calcola il punteggio di consenso.

### D. Quantitative Engine (`ml_predictor.py`)
Utilizza modelli di Machine Learning per prevedere il prezzo a breve termine (Next Candle) basandosi sulle ultime 100 candele, fornendo un "bias" quantitativo al sistema.

### E. Risk & Sector Manager (`sector_manager.py`)
Previene la sovra-esposizione diversificando per settori (AI, L1, Layer2, Meme). In modalità produzione, limita l'esposizione di ogni settore al 30% del capitale.

---

## 3. Strategia di Trading (Hybrid Consensus)

Il bot entra a mercato solo quando il **Consensus Score** supera la soglia impostata (es. 1.5). Il punteggio è composto da:

1.  **Analisi Tecnica (TF 15m/1h/4h)**:
    *   **RSI**: Rilevamento ipercomperato/ipervenduto.
    *   **MACD**: Momentum del trend.
    *   **Bollinger Bands**: Volatilità e deviazione dal prezzo medio.
    *   **EMA 200**: Filtro per il trend primario (sopra = Bullish, sotto = Bearish).
2.  **Order Book Analysis**: Monitora lo squilibrio tra Bids e Asks (Imbalance) per rilevare pressione d'acquisto o vendita istituzionale.
3.  **Funding Rate Filter**: Evita di aprire Long se il funding è troppo alto (costoso) o Short se è troppo negativo (rischio squeeze).
4.  **MTF Trend**: Verifica che il trend a 4 ore e 1 giorno sia coerente con l'entrata a 15 minuti.

---

## 4. Parametri di Configurazione (`client_config.json`)

Il comportamento del bot è guidato da questi parametri chiave:
- `consensus_threshold`: Punteggio minimo per l'ingresso (1.5 = Equilibrato, 3.0 = Molto conservativo).
- `leverage`: Leva finanziaria applicata (es. 10x).
- `percent_per_trade`: Percentuale di equity allocata a ogni singola operazione.
- `stop_loss_pct` / `take_profit_pct`: Livelli di uscita predefiniti (gestiti via codice come "Soft Stops").
- `max_concurrent_positions`: Numero massimo di operazioni aperte simultaneamente.
- `max_global_margin_ratio`: Soglia di sicurezza (es. 0.8) sopra la quale il bot smette di aprire posizioni per proteggere l'intero conto.

---

## 5. Il Ruolo dell'Intelligenza Artificiale (Gemini)

L'IA non è un semplice "indicatore", ma l'intero strato strategico:
1.  **Memoria Storica (RAG)**: Il bot memorizza gli ultimi trade nel database e li passa a Gemini come contesto. L'IA impara dagli errori (es. "In questo momento i segnali RSI su SOL sono stati falsi, sii più prudente su SOL").
2.  **Sentiment Sentiment**: Integra le news RSS nel processo decisionale.
3.  **Dynamic SOS**: Se il prezzo si muove violentemente contro, Gemini decide se è un "Flash Crash" da ignorare o un'inversione di trend che richiede la chiusura immediata (Velocity Exit).

---

## 6. Gestione Avanzata Uscite (TP/SL & SOS)
Il sistema non utilizza ordini "limit" statici sull'exchange, ma un motore di monitoraggio in tempo reale che gestisce le uscite in modo dinamico:

### A. Take Profit (TP)
- **TP1 (Partial Exit)**: Al raggiungimento del primo target tecnico, il bot chiude automaticamente il **50% della posizione**.
- **Break-even Lock**: Immediatamente dopo il TP1, lo Stop Loss viene spostato al prezzo di ingresso + 0.30% (per coprire le commissioni), garantendo che l'operazione non finisca in perdita.
- **TP2 (Final Exit)**: Al raggiungimento del secondo target, la posizione viene chiusa al 100%.

### B. Stop Loss (SL)
- **Technical Trailing Stop**: Uno stop dinamico posizionato a **2x ATR** di distanza dal prezzo. Segue il trend a favore della posizione, proteggendo il profitto latente.
- **Survival Stop**: Uno stop loss di sicurezza estrema posizionato a **5x ATR**. Agisce come barriera contro eventi "Black Swan".
- **AI Audit Exit**: Gemini può decidere di chiudere la posizione in qualsiasi momento se l'analisi fondamentale o tecnica cambia radicalmente.

### C. Velocity Exit (SOS)
È il protocollo di emergenza per la volatilità estrema. Se il bot rileva un movimento di prezzo violento contro la posizione in un arco di soli **2 minuti** (superando una soglia dinamica basata sull'ATR), chiude istantaneamente l'operazione senza aspettare lo stop loss tecnico.

---

## 7. Ciclo di Vita di un'Operazione
1.  **Scanner** trova un asset volatile.
2.  **Segnale Tecnico** scatta (es. RSI basso).
3.  **SignalManager** raccoglie pesi e calcola score.
4.  **Gemini** esegue l'audit finale (APPROVE/REJECT).
5.  **Bot** apre l'ordine e imposta il monitoraggio.
6.  **Ciclo Close**: La posizione viene chiusa da TP/SL tecnico o da un comando di Gemini dopo un audit di metà percorso.
