# Rapporto Post-Mortem: Lezioni Apprese (v9.8.7)
**Data: 30 Marzo 2026**
**Evento: Crash Operativo e Liquidazione d'Emergenza (~ -3100 USDT)**

## 1. Analisi del Fallimento (Cosa è successo?)
Il bot è entrato in un loop di crash (`KeyError`) causato dall'incontro con nuovi simboli non presenti nelle sue mappe di memoria. 
- **Effetto**: Il crash ha impedito l'esecuzione del modulo di gestione del rischio.
- **Conseguenza**: Le posizioni sono rimaste "orfane" su Binance, senza Stop Loss e senza monitoraggio, subendo perdite incontrollate mentre il mercato invertiva il trend.

---

## 2. Lezioni Infrastrutturali
> [!IMPORTANT]
> **Priorità Assoluta alla Sicurezza**
> La strategia non può dipendere dalla stabilità dei moduli analitici. Se l'IA o il Sentiment crashano, il sistema di protezione (SL/TP) *deve* continuare a girare.

### Contromisure implementate:
- **Audit Prioritario**: Il controllo `_check_soft_stop_loss` è stato spostato all'inizio del loop di analisi. Viene eseguito *prima* di qualsiasi calcolo tecnico o chiamata AI.
- **Defensive Coding**: Tutte le strutture dati indicizzate per simbolo (prezzi, volumi, logiche) sono ora basate su `defaultdict`. Questo rende il bot "immune" ai simboli nuovi o inaspettati, eliminando i `KeyError`.

---

## 3. Lezioni Strategiche (IA & Rischio)
> [!WARNING]
> **Il Pericolo della Stagnazione**
> Le posizioni che restano aperte troppo a lungo senza raggiungere l'obiettivo tendono a diventare tossiche. La speranza non è una strategia.

### Apprendimenti:
- **Sincronizzazione Leva-IA**: Un'IA molto convinta (Consensus 9+) deve poter operare con una leva adeguata anche in presenza di volatilità. Schiacciare la leva a 5x quando l'IA è sicura al 100% crea un'incoerenza che danneggia il rapporto Rischio/Rendimento.
- **Micro-PnL Filter**: I trade che generano meno di 0.05 USDT di profitto/perdita sporcano il report e creano rumore. Sono stati filtrati per una lettura più chiara del PnL reale.

---

## 4. Nuove Regole d'Oro (v9.8.7)
1. **Never Orphaned**: Nessuna posizione può esistere su Binance senza che il bot abbia una "firma" locale attiva. Se il bot non riconosce più una posizione, la chiude istantaneamente al riavvio (`sync_zombie_positions`).
2. **Confidence Floor**: Se Gemini ha confidenza > 8.5/10, la leva minima viene alzata a **8x-10x** ignorando parte della volatilità ATR.
3. **Tabula Rasa Policy**: In caso di fallimento sistemico, si procede alla liquidazione totale e al reset dello stato per evitare di trascinare errori di calcolo tra una sessione e l'altra.

---

**Stato Sistema**: Operativo, Hardened, Reset Completo.
**Obiettivo Prossima Sessione**: Monitoraggio accumulo PnL su base pulita ($10.488).
