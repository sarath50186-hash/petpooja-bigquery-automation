# Petpooja to BigQuery Automation

**OIAS Consulting** — Entrance Cafe daily report automation.

## What it does
1. Logs into Petpooja POS (`billing.petpooja.com`) via Playwright browser automation
2. Downloads **Order Master** and **Order Item Wise** reports as Excel/CSV
3. Uploads CSV data directly to **Google BigQuery** (`entrance_cafe` dataset)
4. Verifies upload with row count queries

## Project Structure
```
petpooja-bigquery-automation/
├── main.py                          # Entry point — run this daily
├── petpooja_bigquery_automation.py  # Full automation (download + upload)
├── requirements.txt                 # Python dependencies
├── service_account.json             # GCP key (DO NOT commit — in .gitignore)
└── .gitignore
```

## Setup (one-time)

### 1. Install dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Service Account Key
Place `sa-key.json` (or `service_account.json`) in the project folder.
The script automatically searches these locations:
- `./service_account.json` (same folder as script)
- `./sa-key.json`
- `C:\Users\oiasa\Desktop\janson key\sa-key.json`
- `C:\Users\oiasa\Downloads\sa-key.json`

### 3. BigQuery Setup
- **Project**: `project-690d1610-b6e3-4b8a-bc1`
- **Dataset**: `entrance_cafe` (region: `asia-south1` Mumbai)
- **Tables**: `order_master`, `order_item_wise`
- **Service Account**: `petpooja-bigquery` (BigQuery Admin role)

## Usage
```bash
# Yesterday's data (default)
python main.py

# Specific date range
python main.py --from 2026-03-12 --to 2026-03-12

# Upload existing CSVs only (skip Petpooja download)
python main.py --bigquery-only

# Test BigQuery connection
python main.py --test

# Single report only
python main.py --report order_master
```

## Daily Automation
Scheduled to run daily at **1:00 PM IST** via Windows Task Scheduler or Cowork scheduled task.

## Security
- `service_account.json` / `sa-key.json` — **NEVER** commit to GitHub
- Petpooja credentials loaded from environment variables (fallback to hardcoded defaults)
- Set env vars: `PETPOOJA_EMAIL`, `PETPOOJA_PASSWORD` for production use
