"""
OIAS Consulting — Petpooja → BigQuery Multi-Restaurant Automation (Zero Cost)
==============================================================================
Downloads reports from Petpooja POS for ALL clients & outlets, uploads to BigQuery.
Each client gets its own BigQuery dataset. Each row has an outlet_name column.

SETUP (one-time):
  1. pip install playwright google-cloud-bigquery pandas openpyxl
     playwright install chromium
  2. Place service_account.json (or sa-key.json) in the same folder as this script
  3. Run:
     python petpooja_bigquery_automation.py                     # all clients, yesterday
     python petpooja_bigquery_automation.py --client entrance_cafe  # one client only
     python petpooja_bigquery_automation.py --from 2026-03-12 --to 2026-03-12
     python petpooja_bigquery_automation.py --test              # test BigQuery connection
"""

import asyncio
import os
import csv
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# ─── GLOBAL CONFIG ────────────────────────────────────────────────────────────
PETPOOJA_BASE_URL = "https://billing.petpooja.com"
GCP_PROJECT_ID    = "project-690d1610-b6e3-4b8a-bc1"
BQ_LOCATION       = "asia-south1"

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

# Temp folders
TEMP_FOLDER = SCRIPT_DIR / "temp_downloads"
CSV_FOLDER  = SCRIPT_DIR / "csv_data"

# ─── CLIENTS CONFIG ──────────────────────────────────────────────────────────
# Each client = one Petpooja login, one BigQuery dataset, one or more outlets
# If outlets list is empty or None, downloads for the default outlet (no switching)

CLIENTS = {
    "entrance_cafe": {
        "name": "Entrance Cafe",
        "email": "theentrancecafeoias@gmail.com",
        "password": "Oias@1234",
        "bq_dataset": "entrance_cafe",
        "outlets": [
            {"name": "Entrance Cafe", "id": "227746"},
        ],
    },
    "svr_sangeetha": {
        "name": "SVR Sangeetha",
        "email": "svrsangeethaoias@gmail.com",
        "password": "Oias@1234",
        "bq_dataset": "svr_sangeetha",
        "outlets": [
            {"name": "Sangeetha - Ramapuram",       "id": "410625"},
            {"name": "SVR medavakkam kitchen",       "id": "375799"},
            {"name": "Sangeetha - Guindy",           "id": "112218"},
            {"name": "Sangeetha - Kovilambakkam",    "id": "112242"},
            {"name": "Sangeetha - Medavakkam",       "id": "333319"},
            {"name": "Sangeetha - Nandambakkam",     "id": "112231"},
            {"name": "Sangeetha - Perangalathur",    "id": "112244"},
            {"name": "Sangeetha - Poonamallee",      "id": "112248"},
            {"name": "Sangeetha - Urapakkam",        "id": "112246"},
        ],
    },
}

# Reports to download for each outlet
REPORTS_CONFIG = {
    "order_master": {
        "name": "Order Master Report",
        "url": "order_summary_ho",
        "bq_table": "order_master",
    },
    "order_item_wise": {
        "name": "Order Report Item Wise",
        "url": "order_summary_item",
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


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_date_range(from_date=None, to_date=None):
    if not to_date:
        to_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not from_date:
        from_date = to_date
    return from_date, to_date


def setup_folders():
    TEMP_FOLDER.mkdir(parents=True, exist_ok=True)
    CSV_FOLDER.mkdir(parents=True, exist_ok=True)


# ─── BIGQUERY AUTH ────────────────────────────────────────────────────────────

def get_bigquery_client(dataset=None):
    """Authenticate to BigQuery. Tries: SA key → env var → OAuth2 → ADC."""
    from google.cloud import bigquery

    if SA_KEY_FILE.exists():
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            str(SA_KEY_FILE),
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        return bigquery.Client(project=GCP_PROJECT_ID, credentials=creds, location=BQ_LOCATION)

    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)

    if SECRETS_FILE.exists():
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
        return bigquery.Client(project=GCP_PROJECT_ID, credentials=creds, location=BQ_LOCATION)

    try:
        return bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    except Exception:
        print("\n   ERROR: No authentication found!")
        print(f"   Place 'service_account.json' or 'sa-key.json' in: {SCRIPT_DIR}")
        sys.exit(1)


# ─── BIGQUERY UPLOAD ──────────────────────────────────────────────────────────

def ensure_dataset(client, dataset_name):
    """Create BigQuery dataset if it doesn't exist."""
    from google.cloud import bigquery
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, dataset_name)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"   Created dataset '{dataset_name}'")


def upload_csv_to_bigquery(csv_path, table_name, dataset_name, outlet_name=None):
    """Upload a CSV file to BigQuery, adding outlet_name column."""
    from google.cloud import bigquery
    import pandas as pd

    print(f"\n   Uploading {csv_path.name} -> {dataset_name}.{table_name}")
    if outlet_name:
        print(f"   Outlet: {outlet_name}")

    client = get_bigquery_client()
    table_id = f"{GCP_PROJECT_ID}.{dataset_name}.{table_name}"

    ensure_dataset(client, dataset_name)

    # Read CSV and add outlet_name column
    df = pd.read_csv(csv_path)
    if outlet_name:
        df.insert(0, 'outlet_name', outlet_name)

    # Clean column names
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    # Build schema
    schema = []
    for col in df.columns:
        if col in FLOAT_COLS:
            schema.append(bigquery.SchemaField(col, "FLOAT64", mode="NULLABLE"))
        else:
            schema.append(bigquery.SchemaField(col, "STRING", mode="NULLABLE"))

    # Convert float columns to string-safe format
    for col in df.columns:
        if col not in FLOAT_COLS:
            df[col] = df[col].astype(str).replace('nan', '')

    # Upload
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(table_id)
    print(f"   OK: {load_job.output_rows} rows loaded (total in table: {table.num_rows})")
    return load_job.output_rows


def verify_bigquery_data(dataset_name, from_date, to_date):
    """Verify uploaded data with count queries."""
    print(f"\n   Verifying {dataset_name} data...")
    client = get_bigquery_client()

    for table_name in ["order_master", "order_item_wise"]:
        try:
            query = f"""
                SELECT outlet_name, COUNT(*) as cnt, SUM(CAST(total AS FLOAT64)) as revenue
                FROM `{dataset_name}.{table_name}`
                WHERE `date` >= '{from_date}' AND `date` <= '{to_date} 23:59:59'
                GROUP BY outlet_name
                ORDER BY outlet_name
            """
            result = client.query(query).result()
            for row in result:
                rev = f", revenue: {row.revenue:,.2f}" if row.revenue else ""
                print(f"   {table_name} | {row.outlet_name}: {row.cnt} rows{rev}")
        except Exception as e:
            print(f"   {table_name}: {e}")


def test_bigquery_connection():
    """Test BigQuery connection for all client datasets."""
    from google.cloud import bigquery

    print("\n Testing BigQuery connection...")
    try:
        client = get_bigquery_client()
        print(f"   Auth OK — connected to {GCP_PROJECT_ID}")

        for client_key, config in CLIENTS.items():
            ds = config["bq_dataset"]
            try:
                tables = list(client.list_tables(f"{GCP_PROJECT_ID}.{ds}"))
                print(f"\n   [{config['name']}] Dataset: {ds}")
                for t in tables:
                    query = f"SELECT COUNT(*) as cnt FROM `{ds}.{t.table_id}`"
                    result = client.query(query).result()
                    for row in result:
                        print(f"     {t.table_id}: {row.cnt} rows")
            except Exception:
                print(f"\n   [{config['name']}] Dataset '{ds}' not yet created (will auto-create on first run)")

        print("\n   BigQuery connection test PASSED!")
        return True
    except Exception as e:
        print(f"\n   BigQuery connection test FAILED: {e}")
        return False


# ─── PETPOOJA LOGIN ──────────────────────────────────────────────────────────

async def login(page, email, password):
    """Log into Petpooja with given credentials."""
    print(f"\n   Logging in as {email}...")
    await page.goto(f"{PETPOOJA_BASE_URL}/users/login", wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(2000)

    email_input = page.locator('input[placeholder*="Email" i], input[placeholder*="mobile" i]').first
    await email_input.click()
    await email_input.press("Control+a")
    await page.keyboard.type(email)
    await page.wait_for_timeout(500)

    await page.locator('button:has-text("Continue")').click()
    await page.wait_for_url("**/users/valid_login", timeout=15000)
    await page.wait_for_timeout(1500)

    pwd_input = page.locator('input[type="password"]').first
    await pwd_input.click()
    await page.keyboard.type(password)
    await page.wait_for_timeout(500)

    await page.locator('button:has-text("Sign In")').click()
    await page.wait_for_load_state("networkidle", timeout=20000)
    await page.wait_for_timeout(2000)

    if "login" in page.url or "valid_login" in page.url:
        raise Exception(f"Login failed for {email}")

    print("   Logged in!")


async def switch_outlet(page, outlet_id, outlet_name):
    """Switch to a specific outlet in Petpooja dashboard."""
    print(f"\n   Switching to: {outlet_name} (ID: {outlet_id})")

    # Go to dashboard first
    await page.goto(f"{PETPOOJA_BASE_URL}/users/dashboard", wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(1500)

    # Click the restaurant name dropdown in header to open outlet picker
    try:
        header_dropdown = page.locator('.restro-title-name, .restaurant-name, [class*="restro-name"]').first
        await header_dropdown.click(timeout=5000)
    except Exception:
        # Try the dropdown arrow/selector area
        try:
            await page.locator('select.restro-dropdown, .outlet-dropdown').first.click(timeout=3000)
        except Exception:
            pass

    await page.wait_for_timeout(1500)

    # Check if "Select Outlet" modal appeared
    modal_visible = await page.locator('text="Select Outlet"').is_visible()

    if modal_visible:
        # Click the outlet by its ID in the list (format: "[ id : XXXXXX ]")
        try:
            await page.locator(f'text="{outlet_id}"').first.click(timeout=8000)
        except Exception:
            try:
                await page.locator(f'text="{outlet_name}"').first.click(timeout=5000)
            except Exception:
                pass

        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(2000)
    else:
        # Fallback: use direct URL with restaurant ID
        await page.goto(f"{PETPOOJA_BASE_URL}/users/change_restaurant/{outlet_id}",
                      wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

    print(f"   Switched to {outlet_name}")


async def logout(page):
    """Logout from Petpooja."""
    try:
        await page.goto(f"{PETPOOJA_BASE_URL}/users/logout", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(1500)
    except Exception:
        pass


# ─── SET DATE ────────────────────────────────────────────────────────────────

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


# ─── DOWNLOAD REPORT ─────────────────────────────────────────────────────────

async def download_report(page, report_key, report_config, from_date, to_date, outlet_name=""):
    from playwright.async_api import TimeoutError as PlaywrightTimeout

    report_name = report_config["name"]
    report_url  = report_config["url"]
    safe_outlet = outlet_name.replace(" ", "_").replace("-", "_").lower()[:30] if outlet_name else "default"

    print(f"\n   {report_name} | {outlet_name} ({from_date} to {to_date})")

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

    download_url = await page.locator('a:has-text("Download")').first.get_attribute("href")

    async with page.expect_download(timeout=60000) as dl_info:
        await page.locator('a:has-text("Download")').first.click()

    download = await dl_info.value

    # Save as Excel (original)
    temp_xlsx = TEMP_FOLDER / f"{safe_outlet}_{report_key}_{from_date}.xlsx"
    await download.save_as(temp_xlsx)

    # Save as CSV for BigQuery
    csv_path = CSV_FOLDER / f"{safe_outlet}_{report_key}_{from_date}.csv"

    if download_url and "s3" in download_url.lower() and download_url.endswith(".csv"):
        import urllib.request
        urllib.request.urlretrieve(download_url, csv_path)
    else:
        import pandas as pd
        df = pd.read_excel(temp_xlsx)
        df.to_csv(csv_path, index=False)

    print(f"   Saved: {csv_path.name}")
    return csv_path


# ─── PROCESS ONE CLIENT ──────────────────────────────────────────────────────

async def process_client(client_key, client_config, from_date, to_date, bigquery_only=False):
    """Download reports + upload to BigQuery for one client (all its outlets)."""
    client_name = client_config["name"]
    email       = client_config["email"]
    password    = client_config["password"]
    dataset     = client_config["bq_dataset"]
    outlets     = client_config["outlets"]

    print(f"\n{'─'*60}")
    print(f"  CLIENT: {client_name} ({len(outlets)} outlets)")
    print(f"  Dataset: {GCP_PROJECT_ID}.{dataset}")
    print(f"{'─'*60}")

    csv_files = []  # list of (csv_path, report_key, outlet_name)

    # ── Step 1: Download from Petpooja ────────────────────────────────────
    if not bigquery_only:
        from playwright.async_api import async_playwright

        print(f"\n[STEP 1] Downloading from Petpooja for {client_name}...")
        async with async_playwright() as p:
            headless_mode = os.getenv("HEADLESS", "true").lower() == "true"
            browser = await p.chromium.launch(headless=headless_mode, downloads_path=str(TEMP_FOLDER))
            context = await browser.new_context(accept_downloads=True)
            page    = await context.new_page()

            try:
                await login(page, email, password)

                for outlet in outlets:
                    outlet_name = outlet["name"]
                    outlet_id   = outlet["id"]

                    # Switch to this outlet (skip if only 1 outlet)
                    if len(outlets) > 1:
                        await switch_outlet(page, outlet_id, outlet_name)

                    # Download each report
                    for report_key, report_config in REPORTS_CONFIG.items():
                        try:
                            csv_path = await download_report(
                                page, report_key, report_config,
                                from_date, to_date, outlet_name
                            )
                            csv_files.append((csv_path, report_key, outlet_name))
                        except Exception as e:
                            print(f"   FAILED: {outlet_name} / {report_config['name']}: {e}")
                            import traceback; traceback.print_exc()

            except Exception as e:
                print(f"\n   Error: {e}")
                import traceback; traceback.print_exc()
            finally:
                await logout(page)
                await page.wait_for_timeout(1000)
                await browser.close()
    else:
        print(f"\n[STEP 1] Skipped (--bigquery-only mode)")

    # ── Step 2: Upload to BigQuery ────────────────────────────────────────
    print(f"\n[STEP 2] Uploading to BigQuery ({dataset})...")

    if not csv_files:
        print("   No CSV files to upload.")
        return 0

    total_rows = 0
    try:
        for csv_path, report_key, outlet_name in csv_files:
            table_name = REPORTS_CONFIG[report_key]["bq_table"]
            rows = upload_csv_to_bigquery(csv_path, table_name, dataset, outlet_name)
            total_rows += rows

        print(f"\n   Upload complete: {total_rows} total rows for {client_name}")
        verify_bigquery_data(dataset, from_date, to_date)

    except Exception as e:
        print(f"\n   BigQuery upload failed: {e}")
        import traceback; traceback.print_exc()

    return total_rows


# ─── MAIN ────────────────────────────────────────────────────────────────────

async def main(from_date=None, to_date=None, report_filter=None, bigquery_only=False, client_filter=None):
    from_date, to_date = get_date_range(from_date, to_date)
    setup_folders()

    print(f"\n{'='*60}")
    print(f"  OIAS — Petpooja -> BigQuery Multi-Restaurant Automation")
    print(f"  Date: {from_date} to {to_date}")
    print(f"  Clients: {len(CLIENTS)}")
    print(f"{'='*60}")

    grand_total = 0

    for client_key, client_config in CLIENTS.items():
        if client_filter and client_key != client_filter:
            continue

        try:
            rows = await process_client(client_key, client_config, from_date, to_date, bigquery_only)
            grand_total += rows
        except Exception as e:
            print(f"\n   CLIENT FAILED: {client_config['name']}: {e}")
            import traceback; traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"  ALL DONE! Total rows uploaded: {grand_total}")
    print(f"  Clients processed: {len(CLIENTS) if not client_filter else 1}")
    print(f"{'='*60}")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OIAS Petpooja -> BigQuery Multi-Restaurant Automation")
    parser.add_argument("--from",          dest="from_date",     default=None,        help="From date YYYY-MM-DD")
    parser.add_argument("--to",            dest="to_date",       default=None,        help="To date YYYY-MM-DD")
    parser.add_argument("--report",        dest="report",        default=None,        help="order_master or order_item_wise")
    parser.add_argument("--client",        dest="client",        default=None,        help="Client key: entrance_cafe, svr_sangeetha")
    parser.add_argument("--bigquery-only", dest="bigquery_only", action="store_true", help="Skip download, upload existing CSVs")
    parser.add_argument("--test",          dest="test",          action="store_true", help="Test BigQuery connection only")
    args = parser.parse_args()

    if args.test:
        test_bigquery_connection()
    else:
        asyncio.run(main(args.from_date, args.to_date, args.report, args.bigquery_only, args.client))
