"""
OIAS Consulting - Petpooja to BigQuery Automation
Entry point - run this file daily at 1:00 PM
"""
from petpooja_bigquery_automation import *
import asyncio, argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OIAS Petpooja -> BigQuery")
    parser.add_argument("--from", dest="from_date", default=None, help="From date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=None, help="To date YYYY-MM-DD")
    parser.add_argument("--report", dest="report", default=None, help="order_master or order_item_wise")
    parser.add_argument("--bigquery-only", dest="bigquery_only", action="store_true", help="Skip download, upload CSVs")
    parser.add_argument("--test", dest="test", action="store_true", help="Test BigQuery connection")
    args = parser.parse_args()
    if args.test:
        test_bigquery_connection()
    else:
        asyncio.run(main(args.from_date, args.to_date, args.report, args.bigquery_only))
