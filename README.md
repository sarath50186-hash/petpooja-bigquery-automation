# Petpooja to BigQuery Automation

OIAS Consulting - Entrance Cafe daily report automation.

## What it does
1. Downloads Order Master and Order Item Wise reports from Petpooja POS
2. Converts Excel to CSV locally
3. Uploads CSV data directly to Google BigQuery

## Files
- main.py - Entry point, runs full automation
- petpooja_bigquery_automation.py - Combined download + upload script
- requirements.txt - Python dependencies

## Setup
Install dependencies:
\`\`\`
pip install -r requirements.txt
playwright install chromium
\`\`\`

Place service_account.json (from GCP) in the project folder.

## Usage
\`\`\`
python main.py                                    # yesterday data
python main.py --from 2026-03-12 --to 2026-03-12  # specific date
python main.py --bigquery-only                     # upload existing CSVs only
python main.py --test                              # test BigQuery connection
\`\`\`

## BigQuery
- Project: project-690d1610-b6e3-4b8a-bc1
- Dataset: entrance_cafe (asia-south1)
- Tables: order_master, order_item_wise

## Security
Do NOT upload service_account.json to GitHub.
