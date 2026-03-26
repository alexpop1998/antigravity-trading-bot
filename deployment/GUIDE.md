# Installation Guide: Institutional Trading Terminal

## 1. VPS Recommendation
For optimal performance-to-price ratio, I recommend **Hetzner Cloud** or **DigitalOcean**.
- **Hetzner (Recommended)**: Model **CPX21** (3 vCPU, 4GB RAM) for ~€8/month.
- **DigitalOcean**: Basic Droplet (2 vCPU, 4GB RAM) for ~$24/month.

---

## 2. Server Preparation (Ubuntu 22.04 LTS)
Once logged into your server via SSH, run these commands:

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.9+, pip, venv, nginx, and git
sudo apt install python3-pip python3-venv nginx certbot python3-certbot-nginx git -y
```

## 3. Project Setup
```bash
# Clone your project from Git
cd /home/$(whoami)/
git clone https://github.com/alexpop1998/antigravity-trading-bot.git trading-terminal
cd trading-terminal

# Setup Virtual Environment
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Environment Configuration
Create the `.env` file in the `backend/` directory:
```bash
nano backend/.env
```
Paste your keys. Ensure you include the LLM configuration:
```env
EXCHANGE_API_KEY=your_key
EXCHANGE_API_SECRET=your_secret
LLM_API_KEY=your_gemini_key
LLM_BASE_URL=https://generativelanguage.googleapis.com/v1/openai/
LLM_MODEL_NAME=gemini-1.5-flash
BINANCE_SANDBOX=false
```

## 5. Systemd Persistence (Uptime 24/7)
Edit the service file to match your VPS username:
```bash
nano deployment/trading-bot.service
# Change User=alex to your VPS username
# Change /Users/alex/... paths to /home/your_user/trading-terminal/...
```

Then install it:
```bash
sudo cp deployment/trading-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable trading-bot
sudo systemctl start trading-bot

# Check if it's running
sudo systemctl status trading-bot
```

## 6. Nginx & SSL (Public Access)
```bash
# Copy and edit nginx config
sudo cp deployment/nginx.conf /etc/nginx/sites-available/trading-bot
sudo ln -s /etc/nginx/sites-available/trading-bot /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl restart nginx

# Optional: Add SSL (HTTPS)
sudo certbot --nginx -d yourdomain.com
```

---

## 7. Remote Support (Antigravity AI)
Since I cannot "live" inside the VPS, when you need my help to debug or upgrade:
1. Copy the logs using: `sudo journalctl -u trading-bot -n 100` or `tail -f /var/log/trading-bot.log`
2. Paste them here in our chat.
3. I will analyze them and provide you with updated code or configuration!
