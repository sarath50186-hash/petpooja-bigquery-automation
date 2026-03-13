"""
OIAS Consulting — Petpooja → BigQuery Automation (Zero Cost)
=============================================================
Downloads reports from Petpooja POS and uploads CSV directly to BigQuery.
Everything runs locally on your laptop — no Cloud Shell needed.

SETUP (one-time):
  1. Install dependencies:
     pip install playwright google-cloud-bigquery pandas openpyxl
     playwright install chromium

  2. Create a Service Account Key in GCP Console:
     a. Go to https://console.cloud.google.com/iam-admin/serviceaccounts
     b. Select project: project-690d1610-b6e3-4b8a-bc1
     c. Click on service account "petpooja-bigquery"
     d. Go to "Keys" tab → "Add Key" → "Create new key" → JSON
     e. Save the downloaded JSON file as "service_account.json"
        in the SAME folder as this script

     If org policy blocks key creation, use OAuth2 instead:
     a. Go to https://console.cloud.google.com/apis/credentials
     b. Click "+ CREATE CREDENTIALS" → "OAuth client ID"
     c. Application type: "Desktop app", Name: "Petpooja Automation"
     d. Download JSON and save as "client_secrets.json" next to this script
     e. First run opens browser for Google login (one-time only)

  3. Run:
     python petpooja_bigquery_automation.py                           # yesterday's data
     python petpooja_bigquery_automation.py --from 2026-03-11 --to 2026-03-11  # specific date
     python petpooja_bigquery_automation.py --bigquery-only           # skip download, upload existing CSVs
     python petpooja_bigquery_automation.py --test                    # test BigQuery connection only
"""

import asyncio
import os
import csv
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
PETPOOJA_EMAIL    = os.getenv("PETPOOJA_EMAIL",    "theentrancecafeoias@gmail.com")
PETPOOJA_PASSWORD = os.getenv("PETPOOJA_PASSWORD", "Oias@1234")

PETPOOJA_BASE_URL = "https://billing.petpooja.com"

# BigQuery Configuration
GCP_PROJECT_ID = "project-690d1610-b6e3-4b8a-bc1"
BQ_DATASET     = "entrance_cafe"
BQ_LOCATION    = "asia-south1"

# File paths — change SAVE_FOLDER to match your laptop
SAVE_FOLDER   = Path(r"C:\Users\oiasa\Desktop\EntranceCafe\March2026")
TEMP_FOLDER   = SAVE_FOLDER / "temp_downloads"
CSV_FOLDER    = SAVE_FOLDER / "csv_data"

# Auth files — searches multiple locations for service account key
SCRIPT_DIR     = Path(__file__).parent
SA_KEY_PATHS   = [
    SCRIPT_DIR / "service_account.json",
    SCRIPT_DIR / "sa-key.json",
    Path(r"C:\Users\oiasa\Desktop\janson key\sa-key.json"),
    Path(r"C:\Users\oiasa\Downloads\sa-key.json"),
]
SA_KEY_FILE    = next((p for p in SA_KEY_PATHS if p.exists()), SA_KEY_PATHS[0])
SECRETS_FILE   = SCRIPT_DIR / "client_secrets.json"
TOKEN_FILE     = SCRIPT_DIR / "bigquery_token.json"

REPORTS_CONFIG = {
    "order_master": {
        "enabled": True,
        "name": "Order Master Report",
        "url": "order_summary_ho",
        "sheet": "Order Master",
        "bq_table": "order_master",
    },
    "order_item_wise": {
        "enabled": True,
        "name": "Order Report Item Wise",
        "url": "order_summary_item",
        "sheet": "Item Wise",
        "bq_table": "order_item_wise",
    },
}

# Numeric columns for BigQuery FLOAT64 type
FLOAT_COLS = {
    'persons', 'my_amount', 'total_tax', 'discount', 'delivery_charge',
    'container_charge', 'service_charge', 'additional_charge',
    'deduction_charge', 'waived_off', 'round_off', 'total',
    'item_price', 'item_quantity', 'item_total',
}


# ─── HELPERS ───────────────────────────────────────────────────────────────────

def get_date_range(from_date=None, to_date=None):
    if not to_date:
        to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not from_date:
        from_date = to_date
    return from_date, to_date


def setup_folders():
    SAVE_FOLDER.mkdir(parents=True, exist_ok=True)
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)
    CSV_FOLDER.mkdir(parents=True, exist_ok=True)


# ─── BIGQUERY AUTH ─────────────────────────────────────────────────────────────

def get_bigquery_client():
    """
    Authenticate to BigQuery. Tries in order:
    1. Service account JSON key file (service_account.json)
    2. GOOGLE_APPLICATION_CREDENTIALS env var
    3. OAuth2 user credentials (client_secrets.json + browser login)
    4. Application Default Credentials (gcloud auth)
    """
    from google.cloud import bigquery

    # Method 1: Service account key file next to script
    if SA_KEY_FILE.exists():
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            str(SA_KEY_FILE),
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        print("   Auth: Service account key (service_account.json)")
        return bigquery.Client(project=GCP_PROJECT_ID, credentials=creds, location=BQ_LOCATION)

    # Method 2: Env var pointing to service account key
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("   Auth: GOOGLE_APPLICATION_CREDENTIALS env var")
        return bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)

    # Method 3: OAuth2 user credentials
    if SECRETS_FILE.exists():
        print("   Auth: OAuth2 user credentials")
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow

        SCOPES = ["https://www.googleapis.com/auth/bigquery"]
        creds = None

        if TOKEN_FILE.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
            except Exception:
                creds = None

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(str(SECRETS_FILE), SCOPES)
                creds = flow.run_local_server(port=0)
            TOKEN_FILE.write_text(creds.to_json())
            print("   Token cached for future runs")

        return bigquery.Client(project=GCP_PROJECT_ID, credentials=creds, location=BQ_LOCATION)

    # Method 4: Application Default Credentials
    try:
        print("   Auth: Application Default Credentials")
        return bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    except Exception:
        print("\n   ERROR: No authentication found!")
        print(f"   Option A: Place 'service_account.json' in: {SCRIPT_DIR}")
        print(f"   Option B: Place 'client_secrets.json' in: {SCRIPT_DIR}")
        print(f"   Option C: Run: gcloud auth application-default login")
        sys.exit(1)


# ─── BIGQUERY UPLOAD ───────────────────────────────────────────────────────────

def upload_csv_to_bigquery(csv_path, table_name):
    """Upload a CSV file directly to BigQuery."""
    from google.cloud import bigquery

    print(f"\n   Uploading {csv_path.name} -> {BQ_DATASET}.{table_name}")

    client = get_bigquery_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    # Ensure dataset exists
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"   Created dataset '{BQ_DATASET}'")

    # Read CSV headers to build schema
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

    schema = []
    for col in headers:
        col_clean = col.strip().lower().replace(' ', '_')
        if col_clean in FLOAT_COLS:
            schema.append(bigquery.SchemaField(col_clean, "FLOAT64", mode="NULLABLE"))
        else:
            schema.append(bigquery.SchemaField(col_clean, "STRING", mode="NULLABLE"))

    # Configure and run load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_quoted_newlines=True,
    )

    with open(csv_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result()  # Wait for completion

    table = client.get_table(table_id)
    print(f"   OK: {load_job.output_rows} rows loaded (total in table: {table.num_rows})")
    return load_job.output_rows


def verify_bigquery_data(from_date, to_date):
    """Verify uploaded data with count queries."""
    from google.cloud import bigquery

    print(f"\n   Verifying BigQuery data...")
    client = get_bigquery_client()

    for table_name in ["order_master", "order_item_wise"]:
        try:
            query = f"""
                SELECT COUNT(*) as cnt, SUM(total) as revenue
                FROM `{BQ_DATASET}.{table_name}`
                WHERE `date` >= '{from_date}' AND `date` <= '{to_date} 23:59:59'
            """
            result = client.query(query).result()
            for row in result:
                rev = f", revenue: {row.revenue:,.2f}" if row.revenue else ""
                print(f"   {table_name}: {row.cnt} rows{rev}")
        except Exception as e:
            print(f"   {table_name}: {e}")


def test_bigquery_connection():
    """Test that authentication and BigQuery access work."""
    from google.cloud import bigquery

    print("\n Testing BigQuery connection...")
    try:
        client = get_bigquery_client()
        # Try listing tables in the dataset
        tables = list(client.list_tables(f"{GCP_PROJECT_ID}.{BQ_DATASET}"))
        print(f"   Connected to {GCP_PROJECT_ID}.{BQ_DATASET}")
        print(f"   Tables found: {[t.table_id for t in tables]}")

        # Count rows in each table
        for t in tables:
            query = f"SELECT COUNT(*) as cnt FROM `{BQ_DATASET}.{t.table_id}`"
            result = client.query(query).result()
            for row in result:
                print(f"   {t.table_id}: {row.cnt} rows")

        print("\n   BigQuery connection test PASSED!")
        return True
    except Exception as e:
        print(f"\n   BigQuery connection test FAILED: {e}")
        return False


# ─── PETPOOJA LOGIN ───────────────────────────────────────────────────────────

async def login(page):
    print("\n   Logging in to Petpooja...")
    await page.goto(f"{PETPOOJA_BASE_URL}/users/login", wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(2000)

    email_input = page.locator('input[placeholder*="Email" i], input[placeholder*="mobile" i]').first
    await email_input.click()
    await email_input.press("Control+a")
    await page.keyboard.type(PETPOOJA_EMAIL)
    await page.wait_for_timeout(500)

    await page.locator('button:has-text("Continue")').click()
    await page.wait_for_url("**/users/valid_login", timeout=15000)
    await page.wait_for_timeout(1500)

    pwd_input = page.locator('input[type="password"]').first
    await pwd_input.click()
    await page.keyboard.type(PETPOOJA_PASSWORD)
    await page.wait_for_timeout(500)

    await page.locator('button:has-text("Sign In")').click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await page.wait_for_timeout(2000)

    if "login" in page.url or "valid_login" in page.url:
        raise Exception("Login failed - check email/password.")

    print("   Logged in!")


# ─── SET DATE ─────────────────────────────────────────────────────────────────

async def set_date_via_js(page, field_index, date_value):
    await page.evaluate(f"""
        (function() {{
            const inputs = document.querySelectorAll('input.reportsatrtdate');
            const input = inputs[{field_index}];
            if (!input) return;
            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
            setter.call(input, '{date_value}');
            input.dispatchEvent(new Event('input',  {{ bubbles: true }}));
            input.dispatchEvent(new Event('change', {{ bubbles: true }}));
        }})();
    """)
    await page.wait_for_timeout(400)


# ─── DOWNLOAD REPORT ──────────────────────────────────────────────────────────

async def download_report(page, report_key, report_config, from_date, to_date):
    from playwright.async_api import TimeoutError as PlaywrightTimeout

    report_name = report_config["name"]
    report_url  = report_config["url"]

    print(f"\n   {report_name} ({from_date} to {to_date})")

    await page.goto(f"{PETPOOJA_BASE_URL}/reports/{report_url}", wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(2500)
    await page.wait_for_selector('input.reportsatrtdate', timeout=15000)

    await set_date_via_js(page, 0, from_date)
    await set_date_via_js(page, 1, to_date)
    await page.wait_for_timeout(500)

    await page.locator('button:has-text("Export")').click()
    print("   Exporting...")

    try:
        await page.wait_for_selector('text=Data exported successfully', timeout=30000)
    except PlaywrightTimeout:
        pass

    await page.wait_for_timeout(1000)
    await page.wait_for_selector('a:has-text("Download")', timeout=30000)

    # Get download URL (Petpooja serves CSV from S3)
    download_url = await page.locator('a:has-text("Download")').first.get_attribute("href")

    async with page.expect_download(timeout=60000) as dl_info:
        await page.locator('a:has-text("Download")').first.click()

    download = await dl_info.value

    # Save as Excel (original)
    temp_xlsx = TEMP_FOLDER / f"{report_key}_{from_date}.xlsx"
    await download.save_as(temp_xlsx)

    # Save as CSV for BigQuery
    csv_path = CSV_FOLDER / f"{report_key}_{from_date}.csv"

    if download_url and "s3" in download_url.lower() and download_url.endswith(".csv"):
        import urllib.request
        urllib.request.urlretrieve(download_url, csv_path)
    else:
        import pandas as pd
        df = pd.read_excel(temp_xlsx)
        df.to_csv(csv_path, index=False)

    print(f"   Saved: {csv_path.name}")
    return temp_xlsx, csv_path


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main(from_date=None, to_date=None, report_filter=None, bigquery_only=False):
    from_date, to_date = get_date_range(from_date, to_date)
    setup_folders()

    print(f"\n{'='*60}")
    print(f"  OIAS — Petpooja -> BigQuery Automation")
    print(f"  Date: {from_date} to {to_date}")
    print(f"{'='*60}")

    xlsx_files_map = {}
    csv_files_map  = {}

    # ── Step 1: Download from Petpooja ─────────────────────────────────────
    if not bigquery_only:
        from playwright.async_api import async_playwright

        print("\n[STEP 1] Downloading from Petpooja...")
        async with async_playwright() as p:
            headless_mode = os.getenv("HEADLESS", "true").lower() == "true"
            browser = await p.chromium.launch(headless=headless_mode, downloads_path=str(TEMP_FOLDER))
            context = await browser.new_context(accept_downloads=True)
            page    = await context.new_page()

            try:
                await login(page)
                for report_key, config in REPORTS_CONFIG.items():
                    if report_filter and report_key != report_filter:
                        continue
                    if not config["enabled"]:
                        continue
                    try:
                        xlsx_path, csv_path = await download_report(
                            page, report_key, config, from_date, to_date
                        )
                        xlsx_files_map[config["sheet"]] = xlsx_path
                        csv_files_map[report_key] = csv_path
                    except Exception as e:
                        print(f"   FAILED: {config['name']}: {e}")
                        import traceback; traceback.print_exc()
            except Exception as e:
                print(f"\n   Error: {e}")
                import traceback; traceback.print_exc()
            finally:
                await page.wait_for_timeout(2000)
                await browser.close()
    else:
        print("\n[STEP 1] Skipped (--bigquery-only mode)")
        for report_key, config in REPORTS_CONFIG.items():
            csv_path = CSV_FOLDER / f"{report_key}_{from_date}.csv"
            if csv_path.exists():
                csv_files_map[report_key] = csv_path
                print(f"   Found: {csv_path.name}")
            else:
                print(f"   Not found: {csv_path.name}")

    # ── Step 2: Upload to BigQuery ─────────────────────────────────────────
    print("\n[STEP 2] Uploading to BigQuery...")

    if not csv_files_map:
        print("   No CSV files to upload.")
    else:
        try:
            total_rows = 0
            for report_key, csv_path in csv_files_map.items():
                table_name = REPORTS_CONFIG[report_key]["bq_table"]
                rows = upload_csv_to_bigquery(csv_path, table_name)
                total_rows += rows

            print(f"\n   Upload complete: {total_rows} total rows")
            verify_bigquery_data(from_date, to_date)

        except Exception as e:
            print(f"\n   BigQuery upload failed: {e}")
            import traceback; traceback.print_exc()
            print("\n   Run with --test flag to diagnose: python petpooja_bigquery_automation.py --test")

    # ── Done ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  DONE! BigQuery: {GCP_PROJECT_ID}.{BQ_DATASET}")
    print(f"{'='*60}")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OIAS Petpooja -> BigQuery Automation")
    parser.add_argument("--from",          dest="from_date",     default=None,          help="From date YYYY-MM-DD")
    parser.add_argument("--to",            dest="to_date",       default=None,          help="To date YYYY-MM-DD")
    parser.add_argument("--report",        dest="report",        default=None,          help="order_master or order_item_wise")
    parser.add_argument("--bigquery-only", dest="bigquery_only", action="store_true",   help="Skip Petpooja download, upload existing CSVs")
    parser.add_argument("--test",          dest="test",          action="store_true",   help="Test BigQuery connection only")
    args = parser.parse_args()

    if args.test:
        test_bigquery_connection()
    else:
        asyncio.run(main(args.from_date, args.to_date, args.report, args.bigquery_only))
