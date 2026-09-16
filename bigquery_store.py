"""
BigQuery mirror store for the GAM AdX Sync Bot.

Appends every MA write into Google BigQuery so the full MA dataset
lives in a big-data warehouse and can be queried there.

Design:
  * Append-only: new rows are always loaded via load_table_from_json.
    No MERGE / DELETE DML — full history is kept (retention cleanups
    are NOT mirrored).
  * Tables are created automatically on first use.

Config (env vars, see .env.example):
  BIGQUERY_ENABLED             default "true"
  BIGQUERY_PROJECT_ID          required, e.g. "adglobe-x"
  BIGQUERY_DATASET             default "ma_data"
  GOOGLE_APPLICATION_CREDENTIALS  path to a BigQuery service-account JSON
  BIGQUERY_CREDENTIALS_JSON     inline service-account JSON (alternative)
"""

import datetime as _dt
import json
import logging
import os
import time

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("gam_sync.bigquery")
import threading

# Limit concurrent BigQuery load jobs to avoid table.write rate limits
_MAX_CONCURRENT_LOADS = int(os.getenv("BIGQUERY_MAX_CONCURRENT_LOADS", "8"))
_load_semaphore = threading.Semaphore(_MAX_CONCURRENT_LOADS)

_STRING = "STRING"
_INTEGER = "INTEGER"
_INT64 = "INT64"
_NUMERIC = "NUMERIC"
_DATE = "DATE"
_TIMESTAMP = "TIMESTAMP"
_BOOL = "BOOL"

TABLES = {
    "network_codes": {
        "columns": [
            ("network_code", _STRING),
            ("label", _STRING),
            ("network_name", _STRING),
            ("account_status", _STRING),
            ("invitation_status", _STRING),
            ("delegation_type", _STRING),
            ("approval_status", _STRING),
            ("revenue_share_millipercent", _INTEGER),
            ("email", _STRING),
            ("seller_id", _STRING),
            ("child_publisher_id", _STRING),
            ("last_modified_at", _TIMESTAMP),
            ("active_since", _TIMESTAMP),
            ("declined_at", _TIMESTAMP),
            ("source", _STRING),
            ("last_synced_at", _TIMESTAMP),
            ("created_at", _TIMESTAMP),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code"],
    },
    "adx_daily_stats": {
        "columns": [
            ("network_code", _STRING),
            ("date", _DATE),
            ("platform", _STRING),
            ("revenue", _NUMERIC),
            ("ecpm", _NUMERIC),
            ("impressions", _INTEGER),
            ("clicks", _INTEGER),
            ("ctr", _NUMERIC),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code", "date", "platform"],
    },
    "adx_os_stats": {
        "columns": [
            ("network_code", _STRING),
            ("date", _DATE),
            ("os", _STRING),
            ("impressions", _INTEGER),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code", "date", "os"],
    },
    "network_code_performance": {
        "columns": [
            ("network_code", _STRING),
            ("today_revenue", _NUMERIC),
            ("today_impressions", _INT64),
            ("today_clicks", _INT64),
            ("week_revenue", _NUMERIC),
            ("week_impressions", _INT64),
            ("week_clicks", _INT64),
            ("week_applicable_revenue", _NUMERIC),
            ("week_not_applicable_revenue", _NUMERIC),
            ("today_applicable_revenue", _NUMERIC),
            ("today_not_applicable_revenue", _NUMERIC),
            ("today_ios_share_pct", _NUMERIC),
            ("week_ios_share_pct", _NUMERIC),
            ("first_data_date", _DATE),
            ("last_synced_at", _TIMESTAMP),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code"],
    },
    "adx_sync_errors": {
        "columns": [
            ("network_code", _STRING),
            ("error_message", _STRING),
            ("failed_at", _TIMESTAMP),
        ],
        "primary_key": ["network_code"],
    },
    "mcm_earnings": {
        "columns": [
            ("month", _STRING),
            ("child_network_code", _STRING),
            ("child_name", _STRING),
            ("delegation_type", _STRING),
            ("total_earnings_micros", _INT64),
            ("total_earnings_currency_code", _STRING),
            ("parent_payment_micros", _INT64),
            ("parent_payment_currency_code", _STRING),
            ("child_payment_micros", _INT64),
            ("child_payment_currency_code", _STRING),
            ("deductions_micros", _INT64),
            ("deductions_currency_code", _STRING),
            ("updated_at", _TIMESTAMP),
        ],
        "primary_key": ["month", "child_network_code"],
    },
}

_TYPES = {
    _STRING: "STRING",
    _INTEGER: "INTEGER",
    _INT64: "INT64",
    _NUMERIC: "NUMERIC",
    _DATE: "DATE",
    _TIMESTAMP: "TIMESTAMP",
    _BOOL: "BOOL",
}


def _bq():
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError as e:
        raise RuntimeError(
            "google-cloud-bigquery is not installed — run: pip install -r requirements.txt"
        ) from e

    project_id = os.getenv("BIGQUERY_PROJECT_ID", "").strip()
    if not project_id:
        raise RuntimeError("BIGQUERY_PROJECT_ID is not set")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    creds_json = os.getenv("BIGQUERY_CREDENTIALS_JSON", "").strip()
    if creds_json:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(creds_json)
        )
        return bigquery.Client(project=project_id, credentials=credentials)
    if creds_path and os.path.isfile(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=project_id, credentials=credentials)
    return bigquery.Client(project=project_id)


def get_bq_client():
    return _bq()


def _as_date(value):
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _as_ts(value):
    if not value:
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return _dt.datetime.fromisoformat(s)
    except ValueError:
        try:
            return _dt.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


class BigQueryStore:
    def __init__(self, enabled=None):
        if enabled is None:
            enabled = os.getenv("BIGQUERY_ENABLED", "true").strip().lower() not in (
                "0", "false", "no", "off",
            )
        self.enabled = enabled
        self._client = None
        self._client_error = None

    @property
    def client(self):
        if not self.enabled:
            return None
        if self._client is None and self._client_error is None:
            try:
                self._client = _bq()
            except Exception as e:
                self._client_error = e
                log.warning("bigquery: mirror disabled — %s", str(e)[:200])
        return self._client

    @property
    def dataset(self):
        return "%s.%s" % (os.getenv("BIGQUERY_PROJECT_ID", "").strip(), os.getenv("BIGQUERY_DATASET", "ma_data").strip())

    def table_ref(self, table):
        return "%s.%s" % (self.dataset, table)

    def ensure_dataset(self):
        if not self.client:
            return False
        from google.cloud import bigquery, exceptions

        dataset_id = self.dataset
        try:
            self.client.get_dataset(dataset_id)
        except exceptions.NotFound:
            self.client.create_dataset(bigquery.Dataset(dataset_id))
        return True

    def ensure_tables(self):
        if not self.ensure_dataset():
            return False
        for name, spec in TABLES.items():
            self._create_table(name, spec)
        return True

    def _create_table(self, name, spec):
        cols = []
        for col, typ in spec["columns"]:
            cols.append("  %s %s" % (col, _TYPES[typ]))
        ddl = "CREATE TABLE IF NOT EXISTS %s (\n%s\n)" % (self.table_ref(name), ",\n".join(cols))
        try:
            self.client.query(ddl).result()
        except Exception as e:
            log.warning("bigquery: create table %s failed: %s", name, str(e)[:300])

    def _convert_row(self, row, spec):
        out = {}
        for col, typ in spec["columns"]:
            value = row.get(col)
            if value is None:
                out[col] = None
                continue
            if typ == _DATE:
                d = _as_date(value)
                out[col] = d.isoformat() if d else None
            elif typ == _TIMESTAMP:
                t = _as_ts(value)
                out[col] = t.isoformat() if t else None
            elif typ in (_INTEGER, _INT64):
                out[col] = int(value)
            elif typ == _NUMERIC:
                out[col] = round(float(value), 6)
            elif typ == _BOOL:
                out[col] = bool(value)
            else:
                out[col] = str(value)
        return out

    def load(self, table, rows):
        if not self.enabled or not rows or not self.client:
            return 0
        self.ensure_tables()
        from google.cloud.bigquery import LoadJobConfig

        spec = TABLES[table]
        converted = [self._convert_row(r, spec) for r in rows]

        # Retry on rate-limit / transient BigQuery errors with exponential backoff
        max_attempts = int(os.getenv("BIGQUERY_LOAD_RETRIES", "5"))
        base_delay = float(os.getenv("BIGQUERY_LOAD_BASE_DELAY", "1.0"))

        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            try:
                with _load_semaphore:
                    job = self.client.load_table_from_json(
                        converted,
                        self.table_ref(table),
                        job_config=LoadJobConfig(write_disposition="WRITE_APPEND"),
                    )
                    job.result()
                    return job.output_rows
            except Exception as e:
                # Log details for diagnosis
                msg = str(e)
                log.warning("bigquery: load attempt %d/%d for %s failed: %s", attempt, max_attempts, table, msg[:300])
                # If last attempt, log and return 0 (caller may record sync error)
                if attempt >= max_attempts:
                    log.error("bigquery: load failed for table %s after %d attempts", table, max_attempts)
                    return 0
                # Exponential backoff
                delay = base_delay * (2 ** (attempt - 1))
                time.sleep(delay)

    def clear_sync_errors(self, codes):
        return 0


def print_schema():
    lines = [
        "-- Google BigQuery MA data schema (mirrors scripts/gam-sync-bot synced tables)",
        "-- Run with: bq query --use_legacy_sql=false < bigquery_schema.sql",
        "-- Or auto-create with: py bigquery_store.py --create-schema",
        "",
        "-- Requires the dataset to exist (see BIGQUERY_DATASET).",
    ]
    for name, spec in TABLES.items():
        cols = [("  %s %s" % (c, _TYPES[t])) for c, t in spec["columns"]]
        lines.append("")
        lines.append("CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET>.%s` (" % name)
        lines.append(",\n".join(cols))
        lines.append(")")
    lines.append("")
    lines.append("-- All-MA-data query (counts every MA table written by the bot):")
    lines.append("-- SELECT 'network_codes' AS source, COUNT(*) AS rows FROM `<PROJECT_ID>.<DATASET>.network_codes`")
    lines.append("-- UNION ALL SELECT 'adx_daily_stats', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_daily_stats`")
    lines.append("-- UNION ALL SELECT 'adx_os_stats', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_os_stats`")
    lines.append("-- UNION ALL SELECT 'network_code_performance', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.network_code_performance`")
    lines.append("-- UNION ALL SELECT 'adx_sync_errors', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.adx_sync_errors`")
    lines.append("-- UNION ALL SELECT 'mcm_earnings', COUNT(*) FROM `<PROJECT_ID>.<DATASET>.mcm_earnings`;")
    print("\n".join(lines))


if __name__ == "__main__":
    import sys

    if "--print-schema" in sys.argv:
        print_schema()
    elif "--create-schema" in sys.argv:
        logging.basicConfig(level=logging.INFO)
        store = BigQueryStore()
        if store.ensure_tables():
            print("BigQuery schema ready (dataset %s)" % store.dataset)
        else:
            print("BigQuery mirror is disabled (BIGQUERY_ENABLED=false) or not configured")
