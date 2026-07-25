"""
GAM AdX Sync Bot — production-ready single-file version

Runs every SYNC_INTERVAL minutes:
  1. Auto-fetch all Child Publishers from GAM Ad Manager 360 Parent (MCM)
     via PublisherQueryLanguageService and upsert into network_codes table
  2. Fetch fresh AdX report for each ACTIVE network code (5 concurrent)
  3. Upsert rows into adx_daily_stats
  4. Delete rows older than RETENTION_DAYS
  5. Log errors to adx_sync_errors

Non-active publishers (pending, invited, inactive, suspended) are tracked
in the DB but skipped for report generation until they become ACTIVE.

Usage:
  python sync.py                    # run in loop
  python sync.py --once             # single sync, no loop
  python sync.py --interval 15      # custom interval in minutes

Requires .env file with:
  SUPABASE_URL, SUPABASE_SERVICE_KEY,
  GAM_CLIENT_EMAIL, GAM_PRIVATE_KEY,
  GAM_NETWORK_CODE                  # parent MCM network code (for auto-fetch)
"""

import os
import re
import sys
import json
import time
import gzip
import logging
import signal
import threading
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client as SupabaseClient
from google.oauth2 import service_account
from google.auth.transport import requests as gauth_requests

load_dotenv()

# ── Configuration ───────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
GAM_CLIENT_EMAIL = os.getenv("GAM_CLIENT_EMAIL")
GAM_PRIVATE_KEY = os.getenv("GAM_PRIVATE_KEY")
GAM_NETWORK_CODE = os.getenv("GAM_NETWORK_CODE", "").strip()

SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "13"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "60"))
LOG_DIR = os.getenv("LOG_DIR", "logs")
GAM_VERSION = "v202602"

# ── Logging Setup ───────────────────────────────────────────────

os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"sync_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger("gam_sync")

# ── Shutdown Flag ───────────────────────────────────────────────

shutdown_flag = threading.Event()


def handle_signal(signum, frame):
    log.warning("Received signal %s — shutting down gracefully...", signum)
    shutdown_flag.set()


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ═════════════════════════════════════════════════════════════════
#  GAM HELPERS
# ═════════════════════════════════════════════════════════════════


def sanitize_private_key(raw: str) -> str:
    key = raw.strip()
    key = key.replace("\\\\n", "\n")
    key = key.replace("\\n", "\n")
    key = key.replace("\r\n", "\n").replace("\r", "\n")
    key = key.replace('^"|"$', "").strip()
    if not key.endswith("\n"):
        key += "\n"
    return key


def get_access_token(client_email: str, private_key_pem: str) -> str:
    key = sanitize_private_key(private_key_pem)
    creds = service_account.Credentials.from_service_account_info(
        {
            "client_email": client_email.strip(),
            "private_key": key,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=["https://www.googleapis.com/auth/dfp"],
    )
    creds.refresh(gauth_requests.Request())
    if not creds.token:
        raise RuntimeError("Failed to obtain GAM access token")
    return creds.token


def build_envelope(network_code: str, body: str) -> str:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
  xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Header>
    <ns1:RequestHeader
      soapenv:actor="http://schemas.xmlsoap.org/soap/actor/next"
      soapenv:mustUnderstand="0"
      xmlns:ns1="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <ns1:networkCode>{network_code}</ns1:networkCode>
      <ns1:applicationName>AdGlobeX</ns1:applicationName>
    </ns1:RequestHeader>
  </soapenv:Header>
  <soapenv:Body>{body}</soapenv:Body>
</soapenv:Envelope>'''


def soap_call(network_code: str, service: str, body: str, token: str) -> str:
    url = f"https://ads.google.com/apis/ads/publisher/{GAM_VERSION}/{service}"
    envelope = build_envelope(network_code, body)
    req = urllib.request.Request(
        url,
        data=envelope.encode("utf-8"),
        headers={
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": '""',
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GAM SOAP {service} HTTP {e.code}: {error_body[:500]}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"GAM SOAP {service} connection error: {e.reason}")


def extract_text(xml: str, tag: str) -> Optional[str]:
    m = re.search(
        rf"<(?:[a-z0-9_]+:)?{tag}[^>]*>([\s\S]*?)</(?:[a-z0-9_]+:)?{tag}>",
        xml,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else None


def date_to_gam_xml(date_str: str, tag_name: str) -> str:
    parts = date_str.split("-")
    return f"<{tag_name}><year>{parts[0]}</year><month>{int(parts[1])}</month><day>{int(parts[2])}</day></{tag_name}>"


def parse_adx_csv(csv_text: str) -> list[dict]:
    lines = [l.strip() for l in csv_text.split("\n") if l.strip()]
    if len(lines) < 2:
        return []

    headers = [h.replace('"', "").strip().lower() for h in lines[0].split(",")]

    def idx(keywords: list[str]) -> int:
        for i, h in enumerate(headers):
            for kw in keywords:
                if kw in h:
                    return i
        return -1

    date_idx = idx(["date"])
    revenue_idx = idx(["revenue"])
    ecpm_idx = idx(["ecpm"])
    imp_idx = idx(["impression"])
    ctr_idx = idx(["ctr"])
    platform_idx = idx(["app"])

    rows: list[dict] = []
    for line in lines[1:]:
        cols = [c.replace('"', "").strip() for c in line.split(",")]
        if len(cols) < 4:
            continue
        try:
            revenue = (float(cols[revenue_idx]) if revenue_idx >= 0 else 0) / 1_000_000
            ecpm = (float(cols[ecpm_idx]) if ecpm_idx >= 0 else 0) / 1_000_000
            impressions = int(float(cols[imp_idx])) if imp_idx >= 0 else 0
            ctr = (float(cols[ctr_idx]) if ctr_idx >= 0 else 0) / 100.0
        except (ValueError, IndexError):
            continue
        rows.append({
            "date": cols[date_idx] if date_idx >= 0 else "",
            "platform": cols[platform_idx] if platform_idx >= 0 else "Unknown",
            "revenue": round(revenue, 6),
            "ecpm": round(ecpm, 6),
            "impressions": impressions,
            "ctr": round(ctr, 6),
        })
    return rows


def fetch_adx_report(network_code: str, start_date: str, end_date: str, token: str) -> list[dict]:
    run_body = f'''
    <runReportJob xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJob>
        <reportQuery>
          <dimensions>DATE</dimensions>
          <dimensions>MOBILE_APP_NAME</dimensions>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_CTR</columns>
          {date_to_gam_xml(start_date, "startDate")}
          {date_to_gam_xml(end_date, "endDate")}
          <dateRangeType>CUSTOM_DATE</dateRangeType>
          <reportCurrency>USD</reportCurrency>
        </reportQuery>
      </reportJob>
    </runReportJob>'''

    log.debug("Submitting report job for %s [%s → %s]", network_code, start_date, end_date)
    run_resp = soap_call(network_code, "ReportService", run_body, token)
    job_id = extract_text(run_resp, "id")
    if not job_id:
        raise RuntimeError(f"Could not extract report job ID for {network_code}")

    status = "IN_PROGRESS"
    for _ in range(60):
        time.sleep(2)
        status_body = f'''
        <getReportJobStatus xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
          <reportJobId>{job_id}</reportJobId>
        </getReportJobStatus>'''
        status_resp = soap_call(network_code, "ReportService", status_body, token)
        rval = extract_text(status_resp, "rval")
        if rval:
            status = rval
        if status == "COMPLETED":
            break
        if status == "FAILED":
            raise RuntimeError(f"Report job {job_id} failed for {network_code}")

    if status != "COMPLETED":
        raise RuntimeError(f"Report job {job_id} timed out for {network_code}")

    url_body = f'''
    <getReportDownloadURL xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
      <exportFormat>CSV_DUMP</exportFormat>
    </getReportDownloadURL>'''
    url_resp = soap_call(network_code, "ReportService", url_body, token)
    download_url = extract_text(url_resp, "rval")
    if not download_url:
        raise RuntimeError(f"Could not get download URL for {network_code}")
    download_url = download_url.replace("&amp;", "&")

    csv_req = urllib.request.Request(download_url)
    with urllib.request.urlopen(csv_req, timeout=120) as resp:
        raw = resp.read()

    try:
        csv_text = gzip.decompress(raw).decode("utf-8")
    except (gzip.BadGzipFile, OSError):
        csv_text = raw.decode("utf-8")

    rows = parse_adx_csv(csv_text)
    log.info("Fetched %d rows for %s", len(rows), network_code)
    return rows


# ═════════════════════════════════════════════════════════════════
#  MCM CHILD PUBLISHER FETCH
# ═════════════════════════════════════════════════════════════════


def _extract_result_blocks(xml_str: str) -> list[dict[str, str]]:
    """
    Parse GAM SOAP response with <results> blocks (from getCompaniesByStatement),
    extracting simple child elements as key-value pairs.
    """
    cleaned = re.sub(r'\s+xmlns[^=]*="[^"]*"', "", xml_str)
    blocks: list[dict[str, str]] = []
    for rval in re.finditer(r"<results>([\s\S]*?)</results>", cleaned):
        fields = {}
        for child in re.finditer(r"<(\w+)>([^<]*)</\1>", rval.group(1)):
            fields[child.group(1)] = child.group(2).strip()
        if fields:
            blocks.append(fields)
    return blocks


def fetch_child_publishers(token: str, parent_code: str) -> list[dict]:
    """
    Fetch MCM child publishers via CompanyService.getCompaniesByStatement.
    Returns list of dicts with keys:
      network_code, name, seller_id, account_status, invitation_status, delegation_type
    Only returns companies of type CHILD_PUBLISHER that have a valid childNetworkCode.
    """
    body = f'''<getCompaniesByStatement xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
    <filterStatement>
      <query>WHERE type = 'CHILD_PUBLISHER'</query>
    </filterStatement>
  </getCompaniesByStatement>'''

    try:
        resp = soap_call(parent_code, "CompanyService", body, token)
    except RuntimeError as e:
        log.warning("getCompaniesByStatement error: %s", str(e)[:300])
        return []

    companies = _extract_result_blocks(resp)
    if not companies:
        log.info("No CHILD_PUBLISHER companies found")
        return []

    # Filter to those with a valid childNetworkCode
    children = [c for c in companies if c.get("childNetworkCode")]
    log.info(
        "Fetched %d MCM children from CompanyService (total %d CHILD_PUBLISHER companies)",
        len(children), len(companies),
    )

    mapped = []
    for c in children:
        status = c.get("accountStatus", "").upper()
        inv_status = c.get("invitationStatus", "").upper()
        is_active = status == "APPROVED" and inv_status == "ACCEPTED"

        mapped.append({
            "network_code": c.get("childNetworkCode", ""),
            "name": c.get("name", "").strip(),
            "seller_id": c.get("sellerId", ""),
            "account_status": "ACTIVE" if is_active else status,
            "invitation_status": inv_status,
            "delegation_type": c.get("approvedDelegationType", ""),
        })

    return mapped


def sync_network_codes(supabase: SupabaseClient, token: str) -> dict:
    """
    Fetch ALL MCM child publishers from GAM via CompanyService and
    upsert into network_codes table.
    Every child publisher with a childNetworkCode is stored.
    Only children with account_status='ACTIVE' are synced for reports.
    """
    if not GAM_NETWORK_CODE:
        log.info("GAM_NETWORK_CODE not set — skipping MCM child publisher sync")
        return {"status": "skipped", "reason": "no_parent_code"}

    log.info("Fetching MCM children from parent %s...", GAM_NETWORK_CODE)
    children = fetch_child_publishers(token, GAM_NETWORK_CODE)
    if not children:
        return {"status": "completed", "children_found": 0}

    # Load existing codes from DB
    existing_resp = supabase.table("network_codes").select("network_code").execute()
    existing_codes = {r["network_code"] for r in (existing_resp.data or [])}

    now_iso = datetime.now(timezone.utc).isoformat()
    upserted = 0

    for child in children:
        code = child["network_code"]
        name = child.get("name", "").strip()
        dt = child.get("delegation_type", "")
        status = child.get("account_status", "INACTIVE")
        inv = child.get("invitation_status", "")

        row = {
            "network_code": code,
            "label": name[:255] or code,
            "network_name": name[:255] or None,
            "account_status": status,
            "invitation_status": inv,
            "delegation_type": dt,
            "seller_id": child.get("seller_id", "").strip() or None,
            "source": "mcm_child",
            "updated_at": now_iso,
        }

        # Only auto-insert new records that are the user's actual children
        # (MANAGE_ACCOUNT delegation or pending invites).
        # Legacy MANAGE_INVENTORY children already in DB are updated but not auto-inserted.
        is_user_child = (
            dt == "MANAGE_ACCOUNT"
            or (dt == "" and inv == "PENDING")
            or (dt == "MANAGE_INVENTORY" and status == "PENDING_GOOGLE_APPROVAL")
        )

        try:
            if code in existing_codes:
                update_row = {**row, "last_synced_at": now_iso}
                supabase.table("network_codes").update(update_row).eq("network_code", code).execute()
                upserted += 1
                log.info("Updated child: %s (%s) status=%s dt=%s", code, name or "no name", status, dt)
            elif is_user_child:
                supabase.table("network_codes").insert(row).execute()
                upserted += 1
                log.info("Inserted new child: %s (%s) status=%s dt=%s (last_synced_at not set — first sync pending)", code, name or "no name", status, dt)
            else:
                log.debug("Skipped legacy child not in DB: %s (%s) dt=%s", code, name or "no name", dt)
        except Exception as e:
            err = str(e)
            if "not-null" in err:
                log.error(
                    "Cannot insert MCM code %s — run migration.sql first:\n"
                    "  ALTER TABLE network_codes ALTER COLUMN user_id DROP NOT NULL;\n"
                    "  ALTER TABLE network_codes ALTER COLUMN label DROP NOT NULL;",
                    code,
                )
            else:
                log.error("Failed to sync network_code %s: %s", code, err)

    log.info(
        "MCM sync complete: %d upserted (%d found, %d in DB, legacy skipped)",
        upserted, len(children), len(existing_codes) - 1,  # -1 for parent
    )
    return {"status": "completed", "children_found": len(children), "upserted": upserted}


# ═════════════════════════════════════════════════════════════════
#  TOKEN CACHE
# ═════════════════════════════════════════════════════════════════

_token_lock = threading.Lock()
_cached_token: Optional[str] = None
_token_obtained: float = 0
_TOKEN_TTL = 55 * 60


def get_gam_token_cached() -> str:
    global _cached_token, _token_obtained
    now = time.monotonic()
    with _token_lock:
        if _cached_token and (now - _token_obtained) < _TOKEN_TTL:
            return _cached_token
        log.info("Refreshing GAM access token...")
        token = get_access_token(GAM_CLIENT_EMAIL, GAM_PRIVATE_KEY)
        _cached_token = token
        _token_obtained = now
        return token


# ═════════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════════

def validate_config():
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_KEY:
        missing.append("SUPABASE_SERVICE_KEY")
    if not GAM_CLIENT_EMAIL:
        missing.append("GAM_CLIENT_EMAIL")
    if not GAM_PRIVATE_KEY:
        missing.append("GAM_PRIVATE_KEY")
    if missing:
        log.error("Missing required env vars: %s", ", ".join(missing))
        log.error("Copy .env.example to .env and fill in your credentials")
        sys.exit(1)


def get_date_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")


def effective_start(last_synced_at: Optional[str], default_start: str) -> str:
    """
    Returns the earlier of last_synced_at (minus 1 day buffer) or default_start.
    For new children (no last_synced_at), returns default_start.
    """
    if not last_synced_at:
        return default_start
    last_day = last_synced_at.split("T")[0]
    # Subtract 1 day buffer for late-reporting data
    from datetime import datetime, timedelta
    try:
        d = datetime.strptime(last_day, "%Y-%m-%d") - timedelta(days=1)
        buffer_day = d.strftime("%Y-%m-%d")
    except ValueError:
        return default_start
    return buffer_day if buffer_day < default_start else default_start


# ═════════════════════════════════════════════════════════════════
#  SYNC LOGIC
# ═════════════════════════════════════════════════════════════════

def cleanup_old_data(supabase: SupabaseClient, cutoff_date: str) -> int:
    try:
        resp = supabase.table("adx_daily_stats").delete().lt("date", cutoff_date).execute()
        deleted = len(resp.data) if resp.data else 0
        if deleted:
            log.info("Cleaned up %d rows older than %s", deleted, cutoff_date)
        return deleted
    except Exception as e:
        log.warning("Cleanup error: %s", e)
        return 0


def sync_single_code(network_code: str, start_date: str, end_date: str, supabase: SupabaseClient, token: str) -> dict:
    try:
        rows = fetch_adx_report(network_code, start_date, end_date, token)
        if not rows:
            supabase.table("adx_sync_errors").delete().eq("network_code", network_code).execute()
            return {"network_code": network_code, "rows": 0}

        now_iso = datetime.now(timezone.utc).isoformat()
        to_upsert = [
            {
                "network_code": network_code,
                "date": r["date"],
                "platform": r["platform"],
                "revenue": r["revenue"],
                "ecpm": r["ecpm"],
                "impressions": r["impressions"],
                "ctr": r["ctr"],
                "updated_at": now_iso,
            }
            for r in rows
        ]

        total = 0
        for i in range(0, len(to_upsert), 500):
            batch = to_upsert[i:i + 500]
            resp = supabase.table("adx_daily_stats").upsert(batch, on_conflict="network_code,date,platform").execute()
            total += len(resp.data) if resp.data else 0

        supabase.table("adx_sync_errors").delete().eq("network_code", network_code).execute()

        # Mark last_synced_at so next cycle only fetches incremental data
        supabase.table("network_codes").update({
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": now_iso,
        }).eq("network_code", network_code).execute()

        log.info("Upserted %d rows for %s", total, network_code)
        return {"network_code": network_code, "rows": total}

    except Exception as e:
        err_msg = str(e)
        log.error("Error syncing %s: %s", network_code, err_msg)

        # If service account has no access, mark as NO_ACCESS so we don't retry every cycle
        if "NO_NETWORKS_TO_ACCESS" in err_msg:
            try:
                supabase.table("network_codes").update({
                    "account_status": "NO_ACCESS",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }).eq("network_code", network_code).execute()
                log.info("Marked %s as NO_ACCESS (not a managed child)", network_code)
            except Exception:
                pass

        try:
            supabase.table("adx_sync_errors").upsert(
                {
                    "network_code": network_code,
                    "error_message": err_msg[:500],
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                },
                on_conflict="network_code",
            ).execute()
        except Exception:
            pass
        return {"network_code": network_code, "error": err_msg}


def run_sync_cycle() -> dict:
    start_time = time.time()
    end_date = get_date_days_ago(0)
    week_start = get_date_days_ago(7)

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    token = get_gam_token_cached()

    # Step 1: Auto-fetch child publishers from MCM parent (if configured)
    mcm_stats = sync_network_codes(supabase, token)
    if mcm_stats.get("children_found", 0) > 0:
        log.info("MCM sync found %d children", mcm_stats["children_found"])

    # Step 2: Fetch all network codes from DB
    resp = supabase.table("network_codes").select("network_code, created_at, last_synced_at, account_status").execute()
    all_codes = resp.data or []
    log.info("Found %d entries in network_codes", len(all_codes))

    if not all_codes:
        return {"status": "skipped", "reason": "no codes", "elapsed": time.time() - start_time}

    # Step 3: Filter to only ACTIVE publishers (or codes without status = backward compat)
    active_list = []
    skipped_list = []
    for entry in all_codes:
        code = entry.get("network_code", "").strip()
        if not code:
            continue
        acct_status = entry.get("account_status")
        if acct_status is not None and acct_status.upper() != "ACTIVE":
            skipped_list.append(code)
        else:
            active_list.append(entry)

    if skipped_list:
        log.info(
            "Skipping %d non-ACTIVE code(s) (only tracked in DB): %s",
            len(skipped_list), skipped_list,
        )

    if not active_list:
        log.info("No ACTIVE network codes found — nothing to sync")
        return {
            "status": "skipped",
            "reason": "no active codes",
            "elapsed": time.time() - start_time,
        }

    # Step 4: Deduplicate and sync only ACTIVE codes
    unique: dict[str, str] = {}
    for entry in active_list:
        code = entry.get("network_code", "").strip()
        if not code:
            continue
        last_synced = entry.get("last_synced_at")
        if last_synced:
            start = effective_start(last_synced, week_start)
        else:
            # First sync — only fetch from the date the code was added
            created = entry.get("created_at", "").split("T")[0] if entry.get("created_at") else week_start
            start = created if created else week_start
        if code not in unique or start < (unique.get(code) or week_start):
            unique[code] = start

    codes = list(unique.items())
    log.info("Syncing %d ACTIVE network codes (concurrency=%d)", len(codes), CONCURRENCY)

    total_rows = 0
    error_count = 0

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {executor.submit(sync_single_code, code, start, end_date, supabase, token): code for code, start in codes}
        for future in as_completed(futures):
            result = future.result()
            if "error" in result:
                error_count += 1
            else:
                total_rows += result.get("rows", 0)

    cutoff = get_date_days_ago(RETENTION_DAYS)
    deleted = cleanup_old_data(supabase, cutoff)

    elapsed = round(time.time() - start_time, 1)
    stats = {
        "status": "completed",
        "codes_found": len(all_codes),
        "codes_found_mcm": mcm_stats.get("children_found", 0),
        "codes_active": len(codes),
        "codes_skipped_non_active": len(skipped_list),
        "codes_errored": error_count,
        "total_rows_upserted": total_rows,
        "rows_deleted": deleted,
        "retention_days": RETENTION_DAYS,
        "elapsed_seconds": elapsed,
    }
    log.info("Sync cycle complete: %s", json.dumps(stats))
    return stats


# ═════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════

def main():
    validate_config()
    once = "--once" in sys.argv

    global SYNC_INTERVAL
    for arg in sys.argv:
        if arg.startswith("--interval="):
            SYNC_INTERVAL = int(arg.split("=")[1])

    log.info("=" * 60)
    log.info("GAM AdX Sync Bot starting")
    log.info("Interval: %d min | Concurrency: %d | Retention: %d days", SYNC_INTERVAL, CONCURRENCY, RETENTION_DAYS)
    log.info("Supabase: %s", SUPABASE_URL)
    log.info("=" * 60)

    cycle = 0
    while not shutdown_flag.is_set():
        cycle += 1
        log.info("─── Cycle %d ─────────────────────────────────────", cycle)
        try:
            run_sync_cycle()
        except Exception as e:
            log.exception("Unhandled error in sync cycle: %s", e)

        if once:
            break

        for _ in range((SYNC_INTERVAL * 60) // 5):
            if shutdown_flag.wait(5):
                break

    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
