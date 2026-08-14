"""
GAM AdX Sync Bot — production-ready single-file version

Runs every SYNC_INTERVAL minutes:
  1. Auto-fetch all Child Publishers from GAM Ad Manager 360 Parent (MCM)
     via PublisherQueryLanguageService and upsert into network_codes table
  2. Fetch fresh AdX report for each ACTIVE network code via a two-phase
     pipeline (submit all report jobs concurrently, poll in parallel, then
     download concurrently)
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
import random
import gzip
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import requests
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
SUBMIT_CONCURRENCY = int(os.getenv("SUBMIT_CONCURRENCY", "16"))
DOWNLOAD_CONCURRENCY = int(os.getenv("DOWNLOAD_CONCURRENCY", "8"))
REPORT_POLL_INTERVAL = int(os.getenv("REPORT_POLL_INTERVAL", "3"))
REPORT_POLL_TIMEOUT = int(os.getenv("REPORT_POLL_TIMEOUT", "300"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "60"))
LOG_DIR = os.getenv("LOG_DIR", "logs")
MCM_EARNINGS_MONTHS = int(os.getenv("MCM_EARNINGS_MONTHS", "3"))
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


# Connection-pooled HTTP sessions (one per thread) so SOAP calls and report
# downloads reuse keep-alive connections instead of opening a new TCP/TLS
# handshake for every request (~1000+ per cycle).
_http_local = threading.local()

# Supabase's PostgREST client uses httpx with an HTTP/2 multiplexed connection
# that is NOT safe to share across threads. Concurrent worker threads writing to
# the DB through one shared client corrupt the connection state, which Supabase's
# edge terminates -> httpx raises "Server disconnected" / ConnectionTerminated.
# Give each worker thread its own client (same pattern as _http_session).
_supabase_local = threading.local()


def _supabase_client() -> SupabaseClient:
    client = getattr(_supabase_local, "client", None)
    if client is None:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        _supabase_local.client = client
    return client


def _http_session() -> requests.Session:
    session = getattr(_http_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=8, pool_maxsize=32, max_retries=0
        )
        session.mount("https://", adapter)
        _http_local.session = session
    return session


def soap_call(network_code: str, service: str, body: str, token: str) -> str:
    url = f"https://ads.google.com/apis/ads/publisher/{GAM_VERSION}/{service}"
    envelope = build_envelope(network_code, body)
    try:
        resp = _http_session().post(
            url,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml;charset=UTF-8",
                "SOAPAction": '""',
                "Authorization": f"Bearer {token}",
            },
            timeout=60,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"GAM SOAP {service} HTTP {resp.status_code}: {resp.text[:500]}")
        return resp.text
    except requests.RequestException as e:
        raise RuntimeError(f"GAM SOAP {service} connection error: {e}")
    except Exception as e:
        raise RuntimeError(f"GAM SOAP {service} error: {e}")


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
    clk_idx = idx(["clicks"])
    ctr_idx = idx(["ctr"])
    platform_idx = idx(["app"])
    os_idx = idx(["device"])

    rows: list[dict] = []
    for line in lines[1:]:
        cols = [c.replace('"', "").strip() for c in line.split(",")]
        if len(cols) < 4:
            continue
        try:
            revenue = (float(cols[revenue_idx]) if revenue_idx >= 0 else 0) / 1_000_000
            ecpm = (float(cols[ecpm_idx]) if ecpm_idx >= 0 else 0) / 1_000_000
            impressions = int(float(cols[imp_idx])) if imp_idx >= 0 else 0
            clicks = int(float(cols[clk_idx])) if clk_idx >= 0 else 0
            ctr = (float(cols[ctr_idx]) if ctr_idx >= 0 else 0) / 100.0
        except (ValueError, IndexError):
            continue
        rows.append({
            "date": cols[date_idx] if date_idx >= 0 else "",
            "platform": cols[platform_idx] if platform_idx >= 0 else "Unknown",
            "os": cols[os_idx] if os_idx >= 0 else "Unknown",
            "revenue": round(revenue, 6),
            "ecpm": round(ecpm, 6),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(ctr, 6),
        })
    return rows


def aggregate_daily_rows(rows: list[dict]) -> list[dict]:
    """Collapse report rows (now per date + app + OS) back to per date + app so the
    existing adx_daily_stats schema and unique key (network_code,date,platform) stay
    unchanged — the revenue/impression/clicks sums are identical."""
    agg: dict[tuple[str, str], list[float]] = {}
    for r in rows:
        key = (r["date"], r["platform"])
        entry = agg.setdefault(key, [0.0, 0.0, 0.0])
        entry[0] += float(r.get("revenue") or 0)
        entry[1] += float(r.get("impressions") or 0)
        entry[2] += float(r.get("clicks") or 0)
    out: list[dict] = []
    for (date, platform), (rev, imp, clk) in agg.items():
        out.append({
            "date": date,
            "platform": platform,
            "revenue": rev,
            "impressions": int(imp),
            "clicks": int(clk),
            "ecpm": round((rev / imp) * 1000, 6) if imp else 0,
            "ctr": round(clk / imp, 6) if imp else 0,
        })
    return out


def aggregate_os_rows(rows: list[dict]) -> list[dict]:
    """Per date + OS impression counts for the iOS-share breakdown (adx_os_stats)."""
    agg: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (r["date"], r.get("os") or "Unknown")
        agg[key] = agg.get(key, 0) + int(float(r.get("impressions") or 0))
    return [
        {"date": date, "os": os, "impressions": imp}
        for (date, os), imp in sorted(agg.items())
    ]


def is_ios_os(os: str) -> bool:
    s = (os or "").strip().lower()
    return any(k in s for k in ("iphone", "ipad", "ipod", "ios", "apple"))


def build_report_query(start_date: str, end_date: str) -> str:
    return f'''
    <runReportJob xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJob>
        <reportQuery>
          <dimensions>DATE</dimensions>
          <dimensions>MOBILE_APP_NAME</dimensions>
          <dimensions>MOBILE_DEVICE_NAME</dimensions>
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


def soap_call_with_retry(network_code: str, service: str, body: str, token: str, attempts: int = 3) -> str:
    """SOAP call that retries transient GAM failures (server errors HTTP 500 /
    ServerError.SERVER_ERROR, and connection drops like "Server disconnected" /
    ConnectionTerminated) with exponential backoff before giving up."""
    delay = 2.0
    for i in range(attempts):
        try:
            return soap_call(network_code, service, body, token)
        except RuntimeError as e:
            msg = str(e)
            if i >= attempts - 1:
                raise
            lowered = msg.lower()
            if (
                "server_error" in lowered
                or "connection" in lowered
                or "disconnect" in lowered
                or "terminat" in lowered
                or "aborted" in lowered
                or "reset" in lowered
                or "timed out" in lowered
                or "timeout" in lowered
                or "http 500" in lowered
                or "http 502" in lowered
                or "http 503" in lowered
                or "http 429" in lowered
            ):
                time.sleep(delay)
                delay *= 2
                continue
            raise


def submit_report_job(network_code: str, start_date: str, end_date: str, token: str) -> str:
    """Phase A — submit one report job; returns the GAM job id."""
    log.debug("Submitting report job for %s [%s → %s]", network_code, start_date, end_date)
    run_resp = soap_call_with_retry(network_code, "ReportService", build_report_query(start_date, end_date), token)
    job_id = extract_text(run_resp, "id")
    if not job_id:
        raise RuntimeError(f"Could not extract report job ID for {network_code}")
    return job_id


def submit_report_job_staggered(network_code: str, start_date: str, end_date: str, token: str) -> str:
    """Phase A worker — staggers the initial connection opens by a small random
    jitter so concurrent workers don't hit GAM with a simultaneous burst of new
    keep-alive connections (which GAM can terminate, dropping many in-flight jobs)."""
    time.sleep(random.uniform(0, 0.5))
    return submit_report_job(network_code, start_date, end_date, token)


def get_report_job_status(network_code: str, job_id: str, token: str) -> str:
    status_body = f'''
    <getReportJobStatus xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
    </getReportJobStatus>'''
    status_resp = soap_call_with_retry(network_code, "ReportService", status_body, token)
    rval = extract_text(status_resp, "rval")
    return rval if rval else "IN_PROGRESS"


def download_report_rows(network_code: str, job_id: str, token: str) -> list[dict]:
    """Phase C — resolve the download URL and parse the gzip CSV into rows.
    Retries transient connection drops during the file download."""
    url_body = f'''
    <getReportDownloadURL xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
      <exportFormat>CSV_DUMP</exportFormat>
    </getReportDownloadURL>'''
    url_resp = soap_call_with_retry(network_code, "ReportService", url_body, token)
    download_url = extract_text(url_resp, "rval")
    if not download_url:
        raise RuntimeError(f"Could not get download URL for {network_code}")
    download_url = download_url.replace("&amp;", "&")

    delay = 2.0
    raw = None
    for i in range(3):
        try:
            dl_resp = _http_session().get(download_url, timeout=120)
            dl_resp.raise_for_status()
            raw = dl_resp.content
            break
        except Exception as e:
            if i >= 2:
                raise RuntimeError(f"GAM report download failed for {network_code}: {e}")
            time.sleep(delay)
            delay *= 2

    try:
        csv_text = gzip.decompress(raw).decode("utf-8")
    except (gzip.BadGzipFile, OSError):
        csv_text = raw.decode("utf-8")

    rows = parse_adx_csv(csv_text)
    log.info("Fetched %d rows for %s", len(rows), network_code)
    return rows


def fetch_adx_report(network_code: str, start_date: str, end_date: str, token: str) -> list[dict]:
    """Single-code convenience path: submit → poll → download (used for retries)."""
    job_id = submit_report_job(network_code, start_date, end_date, token)
    status = "IN_PROGRESS"
    for _ in range(60):
        time.sleep(2)
        status = get_report_job_status(network_code, job_id, token)
        if status in ("COMPLETED", "FAILED"):
            break
    if status == "FAILED":
        raise RuntimeError(f"Report job {job_id} failed for {network_code}")
    if status != "COMPLETED":
        raise RuntimeError(f"Report job {job_id} timed out for {network_code}")
    return download_report_rows(network_code, job_id, token)


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


def parse_pql_result(xml_str: str) -> list[dict[str, str]]:
    """
    Parse a PublisherQueryLanguageService.select SOAP response.
    Returns one dict per row, keyed by the result column labels (lowercased).
    Handles TextValue / NumberValue / DateValue cells.
    """
    def cell_text(cell: str) -> str:
        m = re.search(r"<value>([\s\S]*?)</value>", cell)
        if not m:
            return ""
        inner = m.group(1)
        y = re.search(r"<year>(\d+)</year>", inner)
        mo = re.search(r"<month>(\d+)</month>", inner)
        if y:
            return f"{int(y.group(1)):04d}-{int(mo.group(1)):02d}" if mo else y.group(1)
        return re.sub(r"<[^>]+>", "", inner).strip()

    cleaned = re.sub(r'\s+xmlns[^=]*="[^"]*"', "", xml_str)
    labels = re.findall(r"<labelName>([^<]*)</labelName>", cleaned)
    rows = []
    for block in re.finditer(r"<rows>([\s\S]*?)</rows>", cleaned):
        cells = re.findall(r"<values[^>]*>([\s\S]*?)</values>", block.group(1))
        if not cells or len(cells) != len(labels):
            continue
        row = {}
        for lbl, cell in zip(labels, cells):
            row[lbl.lower()] = cell_text(cell)
        rows.append(row)
    return rows


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

        # GAM returns the Company's last modified DateTime as flat year/month/day/hour/minute/second fields.
        # For declined children this is the date the invitation was rejected / withdrawn.
        last_modified = None
        try:
            tz = c.get("timeZoneId") or "UTC"
            dt_naive = datetime(
                int(c.get("year", 0)), int(c.get("month", 1)), int(c.get("day", 1)),
                int(c.get("hour", 0)), int(c.get("minute", 0)), int(c.get("second", 0)),
            )
            last_modified = dt_naive.replace(tzinfo=ZoneInfo(tz)).astimezone(timezone.utc).isoformat()
        except Exception:
            last_modified = None

        mapped.append({
            "network_code": c.get("childNetworkCode", ""),
            "name": c.get("name", "").strip(),
            "seller_id": c.get("sellerId", ""),
            "child_publisher_id": c.get("id", ""),
            "last_modified_at": last_modified,
            "account_status": "ACTIVE" if is_active else status,
            "invitation_status": inv_status,
            "delegation_type": c.get("approvedDelegationType", ""),
        })

    return mapped


def fetch_child_publisher_details(token: str, parent_code: str) -> dict[str, dict]:
    """
    Query the child_publisher PQL view on the PARENT network for every child's
    detailed approval status + approved Manage Account revenue share.

    Returns { child_network_code: { approval_status, revenue_share_millipercent } }.
    revenue_share_millipercent is the PARENT's share in millipercent (15000 = 15%);
    only populated for approved Manage Account children.
    """
    body = f'''<select xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <selectStatement>
        <query>
          SELECT ChildNetworkCode, Email, ApprovalStatus, ApprovedManageAccountRevshareMillipercent
          FROM child_publisher
        </query>
      </selectStatement>
    </select>'''
    resp = soap_call(parent_code, "PublisherQueryLanguageService", body, token)
    rows = parse_pql_result(resp)

    details: dict[str, dict] = {}
    for r in rows:
        code = (r.get("childnetworkcode") or "").strip()
        if not code:
            continue
        rev = 0
        try:
            rev = int(float(r.get("approvedmanageaccountrevsharemillipercent") or 0))
        except (TypeError, ValueError):
            rev = 0
        details[code] = {
            "approval_status": (r.get("approvalstatus") or "").strip(),
            "revenue_share_millipercent": rev,
            "email": (r.get("email") or "").strip(),
        }
    return details


def fetch_all_network_codes(
    supabase: SupabaseClient,
    columns: str,
    page_size: int = 1000,
) -> list[dict]:
    """Fetch all network_codes rows in pages so we never hit PostgREST's 1000-row cap."""
    rows: list[dict] = []
    offset = 0
    while True:
        resp = supabase.table("network_codes").select(columns).range(offset, offset + page_size - 1).execute()
        chunk = resp.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows


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

    # Detailed approval status + approved revshare (child_publisher PQL view).
    child_details: dict[str, dict] = {}
    try:
        child_details = fetch_child_publisher_details(token, GAM_NETWORK_CODE)
        log.info("child_publisher details: %d children with status/revshare", len(child_details))
    except Exception as e:
        log.warning("child_publisher details fetch failed: %s", str(e)[:300])

    # Load existing codes from DB (gracefully drop active_since if the column
    # hasn't been migrated yet, so the bot keeps running pre-migration).
    has_active_since = True
    try:
        existing_rows = fetch_all_network_codes(
            supabase,
            "network_code, account_status, declined_at, active_since, created_at, source",
        )
    except Exception:
        has_active_since = False
        existing_rows = fetch_all_network_codes(
            supabase,
            "network_code, account_status, declined_at, source",
        )
    existing_codes = {r["network_code"]: r for r in existing_rows}

    now_iso = datetime.now(timezone.utc).isoformat()

    # Build the full set of rows in memory, then bulk-upsert (avoids ~800
    # sequential DB round-trips — the MCM step's real bottleneck).
    update_rows: list[dict] = []
    insert_rows: list[dict] = []
    legacy_skipped = 0

    for child in children:
        code = child["network_code"]
        name = child.get("name", "").strip()
        dt = child.get("delegation_type", "")
        status = child.get("account_status", "INACTIVE")
        inv = child.get("invitation_status", "")
        detail = child_details.get(code, {})

        row = {
            "network_code": code,
            "label": name[:255] or code,
            "network_name": name[:255] or None,
            "account_status": status,
            "invitation_status": inv,
            "delegation_type": dt,
            "approval_status": detail.get("approval_status") or None,
            "revenue_share_millipercent": detail.get("revenue_share_millipercent") or None,
            "email": detail.get("email") or None,
            "seller_id": child.get("seller_id", "").strip() or None,
            "child_publisher_id": child.get("child_publisher_id", "").strip() or None,
            "last_modified_at": child.get("last_modified_at"),
            "source": "mcm_child",
            "updated_at": now_iso,
        }

        # Only auto-insert new records that are MANAGE_ACCOUNT children
        # (or pending invites). MANAGE_INVENTORY children are excluded —
        # MA reporting is for manage-account relationships only.
        is_user_child = (
            dt == "MANAGE_ACCOUNT"
            or (dt == "" and inv == "PENDING")
        )

        if code in existing_codes:
            prev = existing_codes[code]
            # Rejected invitations (declined by child) or withdrawn (declined by parent):
            # stamp the REAL decline date from GAM's last modified DateTime every sync
            # (idempotent — the date doesn't change). Fall back to existing/now if missing.
            if inv == "REJECTED" or inv == "WITHDRAWN":
                row["declined_at"] = child.get("last_modified_at") or prev.get("declined_at") or now_iso
            # First time we observe a child as ACTIVE: stamp the date the bot
            # discovered it. For pre-existing codes that's created_at (the day we
            # first saw them in MCM) — NOT "now", otherwise we'd wipe their
            # earlier history. For genuinely new children created_at == now anyway.
            if (
                has_active_since
                and status == "ACTIVE"
                and not prev.get("active_since")
            ):
                row["active_since"] = prev.get("created_at") or now_iso
            update_rows.append({**row, "last_synced_at": now_iso})
        elif is_user_child:
            if inv in ("REJECTED", "WITHDRAWN"):
                row["declined_at"] = child.get("last_modified_at") or now_iso
            if has_active_since and status == "ACTIVE":
                row["active_since"] = now_iso
            insert_rows.append(row)
            log.info("Queued new child: %s (%s) status=%s dt=%s (last_synced_at not set — first sync pending)", code, name or "no name", status, dt)
        else:
            legacy_skipped += 1
            log.debug("Skipped legacy child not in DB: %s (%s) dt=%s", code, name or "no name", dt)

    upserted = _upsert_network_codes(supabase, insert_rows, update_rows)

    # Remove codes that are no longer managed under our MCM parent (deleted from
    # GAM or relationship ended). They'll be re-added automatically if they come
    # back. Only touches rows we auto-created (source='mcm_child').
    removed_count = _remove_orphaned_network_codes(
        supabase, existing_codes, {c["network_code"] for c in children}
    )

    log.info(
        "MCM sync complete: %d upserted (%d found, %d in DB, %d legacy skipped, %d removed)",
        upserted, len(children), len(existing_codes), legacy_skipped, removed_count,
    )
    return {"status": "completed", "children_found": len(children), "upserted": upserted}


def _upsert_network_codes(
    supabase: SupabaseClient,
    insert_rows: list[dict],
    update_rows: list[dict],
    batch_size: int = 200,
) -> int:
    """Bulk-update then bulk-insert existing/new network codes, chunked so a
    single bad row can't fail the whole batch (falls back to per-row for that
    chunk, preserving the legacy error logging). Returns number upserted."""
    upserted = 0

    # Existing codes → coalescing upsert (with last_synced_at)
    for i in range(0, len(update_rows), batch_size):
        batch = update_rows[i:i + batch_size]
        try:
            resp = supabase.table("network_codes").upsert(batch, on_conflict="network_code").execute()
            upserted += len(resp.data) if resp.data else 0
        except Exception:
            for row in batch:
                try:
                    supabase.table("network_codes").update(row).eq("network_code", row["network_code"]).execute()
                    upserted += 1
                except Exception as e:
                    log.error("Failed to sync network_code %s: %s", row["network_code"], str(e))

    # New children → insert only (no last_synced_at; first report fetch pending)
    for i in range(0, len(insert_rows), batch_size):
        batch = insert_rows[i:i + batch_size]
        try:
            resp = supabase.table("network_codes").upsert(batch, on_conflict="network_code").execute()
            upserted += len(resp.data) if resp.data else 0
        except Exception:
            for row in batch:
                try:
                    supabase.table("network_codes").insert(row).execute()
                    upserted += 1
                except Exception as e:
                    err = str(e)
                    if "not-null" in err:
                        log.error(
                            "Cannot insert MCM code %s — run migration.sql first:\n"
                            "  ALTER TABLE network_codes ALTER COLUMN user_id DROP NOT NULL;\n"
                            "  ALTER TABLE network_codes ALTER COLUMN label DROP NOT NULL;",
                            row["network_code"],
                        )
                    else:
                        log.error("Failed to sync network_code %s: %s", row["network_code"], err)

    return upserted


def _remove_orphaned_network_codes(
    supabase: SupabaseClient,
    existing_codes: dict[str, dict],
    current_gam_codes: set[str],
) -> int:
    """
    Delete MCM-child codes we auto-tracked (source='mcm_child') that are no
    longer present under our parent in GAM (child removed / relationship ended).
    Cascades: adx_daily_stats, network_code_performance, adx_sync_errors, and
    the network_codes row itself. They'll be re-added if the child returns.
    """
    removed = [
        code for code, row in existing_codes.items()
        if (row.get("source") or "") == "mcm_child" and code not in current_gam_codes
    ]
    if not removed:
        return 0

    for table in ("adx_daily_stats", "network_code_performance", "adx_sync_errors", "network_codes"):
        for i in range(0, len(removed), 100):
            chunk = removed[i:i + 100]
            try:
                supabase.table(table).delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Cleanup delete failed on %s: %s", table, str(e)[:150])

    log.info("Removed %d network code(s) no longer present in GAM: %s", len(removed), removed)
    return len(removed)


# ═════════════════════════════════════════════════════════════════
#  MCM EARNINGS (parent-attributed, via mcm_earnings PQL view)
# ═════════════════════════════════════════════════════════════════

def fetch_mcm_earnings(token: str, parent_code: str, month: str) -> list[dict]:
    """
    Query the mcm_earnings PQL view on the PARENT network for a single month.
    This is the authoritative source for how much each child earned WITH US
    (no pre-join history, no other-parent data). Every column is in micros.
    Each query MUST be scoped to exactly one month.
    """
    body = f'''<select xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <selectStatement>
        <query>
          SELECT ChildName, ChildNetworkCode, ChildPaymentCurrencyCode, ChildPaymentMicros,
                 DeductionsCurrencyCode, DeductionsMicros, DelegationType, Month,
                 ParentName, ParentNetworkCode, ParentPaymentCurrencyCode, ParentPaymentMicros,
                 TotalEarningsCurrencyCode, TotalEarningsMicros
          FROM mcm_earnings
          WHERE month = '{month}'
        </query>
      </selectStatement>
    </select>'''
    resp = soap_call(parent_code, "PublisherQueryLanguageService", body, token)
    return parse_pql_result(resp)


def sync_mcm_earnings(supabase: SupabaseClient, token: str) -> int:
    """Fetch MCM earnings for the last N months and upsert into mcm_earnings table."""
    if not GAM_NETWORK_CODE:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    months = []
    now = datetime.now(timezone.utc)
    for i in range(MCM_EARNINGS_MONTHS):
        idx = now.year * 12 + (now.month - 1) - i
        months.append(f"{idx // 12:04d}-{idx % 12 + 1:02d}")

    rows = []
    for month in months:
        try:
            data = fetch_mcm_earnings(token, GAM_NETWORK_CODE, month)
        except Exception as e:
            log.warning("mcm_earnings fetch failed for %s: %s", month, str(e)[:300])
            continue
        for r in data:
            rows.append({
                "month": month,
                "child_network_code": r.get("childnetworkcode", ""),
                "child_name": r.get("childname", ""),
                "delegation_type": r.get("delegationtype", ""),
                "total_earnings_micros": int(float(r.get("totalearningsmicros") or 0)),
                "total_earnings_currency_code": r.get("totalearningscurrencycode", ""),
                "parent_payment_micros": int(float(r.get("parentpaymentmicros") or 0)),
                "parent_payment_currency_code": r.get("parentpaymentcurrencycode", ""),
                "child_payment_micros": int(float(r.get("childpaymentmicros") or 0)),
                "child_payment_currency_code": r.get("childpaymentcurrencycode", ""),
                "deductions_micros": int(float(r.get("deductionsmicros") or 0)),
                "deductions_currency_code": r.get("deductionscurrencycode", ""),
                "updated_at": now_iso,
            })
        log.info("mcm_earnings: fetched %d child rows for %s", len(data), month)

    if not rows:
        return 0

    total = 0
    try:
        for i in range(0, len(rows), 500):
            batch = rows[i:i + 500]
            resp = supabase.table("mcm_earnings").upsert(batch, on_conflict="month,child_network_code").execute()
            total += len(resp.data) if resp.data else 0
        log.info("mcm_earnings: upserted %d rows across %d months", total, len(months))
    except Exception as e:
        log.warning("mcm_earnings upsert failed (run migration.sql first?): %s", str(e)[:300])
    return total


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


def cleanup_prejoin_data(supabase: SupabaseClient) -> int:
    """
    Idempotent startup cleanup: delete adx_daily_stats rows older than the
    child's active_since date (fallback created_at). These rows came from
    querying the child's own network before the parent relationship existed.

    Always uses the per-code REST loop (active_since-aware) rather than the
    cleanup_prejoin_data() Postgres function, so it stays correct even if the
    DB function was created with the old created_at-only logic.
    """
    return _cleanup_prejoin_loop(supabase)


def _cleanup_prejoin_loop(supabase: SupabaseClient) -> int:
    deleted = 0
    for entry in fetch_all_network_codes(supabase, "network_code, created_at, active_since"):
        code = entry.get("network_code", "").strip()
        boundary = entry.get("active_since") or entry.get("created_at") or ""
        join = boundary[:10]
        if not code or not join:
            continue
        try:
            dr = supabase.table("adx_daily_stats").delete().eq("network_code", code).lt("date", join).execute()
            deleted += len(dr.data) if dr.data else 0
        except Exception as e:
            log.warning("Pre-join cleanup failed for %s: %s", code, str(e)[:200])
    if deleted:
        log.info("Pre-join cleanup removed %d rows older than their join date", deleted)
    return deleted


def record_sync_failure(supabase: SupabaseClient, network_code: str, err: BaseException, phase: str = "") -> None:
    """Log a failed sync for a code; mark as NO_ACCESS when the service account
    can't reach the child network so we don't retry it every cycle."""
    err_msg = str(err)
    log.error("Error syncing %s [%s]: %s", network_code, phase or "unknown", err_msg)

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


def upsert_report_rows(supabase: SupabaseClient, network_code: str, rows: list[dict]) -> int:
    """Upsert fetched report rows into adx_daily_stats and pre-compute the
    today / last-7-days summary for the API (no read-time aggregation)."""
    if not rows:
        supabase.table("adx_sync_errors").delete().eq("network_code", network_code).execute()
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    _dates = sorted({r["date"] for r in rows})
    if _dates:
        try:
            supabase.table("adx_daily_stats").delete().eq("network_code", network_code).gte("date", _dates[0]).lte("date", _dates[-1]).execute()
        except Exception as e:
            log.warning("pre-delete adx_daily_stats failed for %s: %s", network_code, str(e)[:200])
    to_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "platform": r["platform"],
            "revenue": r["revenue"],
            "ecpm": r["ecpm"],
            "impressions": r["impressions"],
            "clicks": r["clicks"],
            "ctr": r["ctr"],
            "updated_at": now_iso,
        }
        for r in aggregate_daily_rows(rows)
    ]

    total = 0
    for i in range(0, len(to_upsert), 500):
        batch = to_upsert[i:i + 500]
        resp = supabase.table("adx_daily_stats").upsert(batch, on_conflict="network_code,date,platform").execute()
        total += len(resp.data) if resp.data else 0

    os_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "os": r["os"],
            "impressions": r["impressions"],
            "updated_at": now_iso,
        }
        for r in aggregate_os_rows(rows)
    ]
    if _dates:
        try:
            supabase.table("adx_os_stats").delete().eq("network_code", network_code).gte("date", _dates[0]).lte("date", _dates[-1]).execute()
        except Exception as e:
            log.warning("pre-delete adx_os_stats failed for %s: %s", network_code, str(e)[:200])
    for i in range(0, len(os_upsert), 500):
        batch = os_upsert[i:i + 500]
        try:
            supabase.table("adx_os_stats").upsert(batch, on_conflict="network_code,date,os").execute()
        except Exception as e:
            log.warning("adx_os_stats upsert failed for %s: %s", network_code, str(e)[:200])

    supabase.table("adx_sync_errors").delete().eq("network_code", network_code).execute()

    # Mark last_synced_at so next cycle only fetches incremental data
    supabase.table("network_codes").update({
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": now_iso,
    }).eq("network_code", network_code).execute()

    # Pre-compute today + last-7-days summary so the API never aggregates at read time
    refresh_code_performance(supabase, network_code)

    log.info("Upserted %d rows for %s", total, network_code)
    return total


def sync_single_code(network_code: str, start_date: str, end_date: str, supabase: SupabaseClient, token: str) -> dict:
    try:
        rows = fetch_adx_report(network_code, start_date, end_date, token)
        total = upsert_report_rows(supabase, network_code, rows)
        try:
            supabase.rpc("refresh_dashboard_totals").execute()
        except Exception as e:
            log.warning("refresh_dashboard_totals skipped: %s", str(e)[:200])
        return {"network_code": network_code, "rows": total}
    except Exception as e:
        record_sync_failure(supabase, network_code, e)
        return {"network_code": network_code, "error": str(e)}


def refresh_code_performance(supabase: SupabaseClient, network_code: str) -> None:
    """
    Pre-compute today + last-7-days totals (revenue / impressions / clicks) for one
    network code and upsert them into network_code_performance. The API then only
    does a plain SELECT per code — no aggregation at read time (PGRST123 disabled).
    """
    today = get_date_days_ago(0)
    week_start = get_date_days_ago(6)
    try:
        resp = supabase.table("adx_daily_stats").select(
            "date, platform, revenue, impressions, clicks"
        ).eq("network_code", network_code).gte("date", week_start).lte("date", today).execute()
        rows = resp.data or []

        t_rev = t_imp = t_clk = 0
        t_app = t_na = 0
        w_rev = w_imp = w_clk = 0
        w_app = w_na = 0
        for r in rows:
            rev = float(r.get("revenue") or 0)
            imp = int(r.get("impressions") or 0)
            clk = int(r.get("clicks") or 0)
            d = r.get("date") or ""
            is_na = (r.get("platform") or "").strip() == "(Not applicable)"
            if d == today:
                t_rev += rev
                t_imp += imp
                t_clk += clk
                if is_na:
                    t_na += rev
                else:
                    t_app += rev
            w_rev += rev
            w_imp += imp
            w_clk += clk
            if is_na:
                w_na += rev
            else:
                w_app += rev

        first_date = None
        try:
            fr = supabase.table("v_network_code_first_date").select("first_date").eq("network_code", network_code).execute()
            if fr.data and fr.data[0].get("first_date"):
                first_date = fr.data[0]["first_date"]
        except Exception:
            pass

        t_imp_os = w_imp_os = t_ios = w_ios = 0
        try:
            osr = supabase.table("adx_os_stats").select(
                "date, os, impressions"
            ).eq("network_code", network_code).gte("date", week_start).lte("date", today).execute()
            for o in osr.data or []:
                imp = int(o.get("impressions") or 0)
                d = o.get("date") or ""
                is_ios = is_ios_os(o.get("os") or "")
                w_imp_os += imp
                if is_ios:
                    w_ios += imp
                if d == today:
                    t_imp_os += imp
                    if is_ios:
                        t_ios += imp
        except Exception as e:
            log.warning("adx_os_stats read failed for %s: %s", network_code, str(e)[:200])


        now_iso = datetime.now(timezone.utc).isoformat()
        supabase.table("network_code_performance").upsert({
            "network_code": network_code,
            "today_revenue": round(t_rev, 6),
            "today_impressions": t_imp,
            "today_clicks": t_clk,
            "week_revenue": round(w_rev, 6),
            "week_impressions": w_imp,
            "week_clicks": w_clk,
            "week_applicable_revenue": round(w_app, 6),
            "week_not_applicable_revenue": round(w_na, 6),
            "today_applicable_revenue": round(t_app, 6),
            "today_not_applicable_revenue": round(t_na, 6),
            "today_ios_share_pct": round((t_ios / t_imp_os) * 100, 2) if t_imp_os else 0,
            "week_ios_share_pct": round((w_ios / w_imp_os) * 100, 2) if w_imp_os else 0,
            "first_data_date": first_date,
            "last_synced_at": now_iso,
            "updated_at": now_iso,
        }, on_conflict="network_code").execute()
    except Exception as e:
        log.warning("Performance summary failed for %s: %s", network_code, str(e)[:200])


def submit_all_report_jobs(codes: list[tuple[str, str]], end_date: str, supabase: SupabaseClient, token: str) -> list[dict]:
    """Phase A — submit one report job per ACTIVE code concurrently.
    Returns the successfully-submitted jobs; failures are recorded and excluded."""
    jobs: list[dict] = []
    with ThreadPoolExecutor(max_workers=SUBMIT_CONCURRENCY) as executor:
        futures = {
            executor.submit(
                submit_report_job_staggered, code, start, end_date, token
            ): code
            for code, start in codes
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                jobs.append({"network_code": code, "job_id": future.result()})
            except Exception as e:
                record_sync_failure(supabase, code, e, "phase-a-submit")
    log.info("Submitted %d/%d report jobs", len(jobs), len(codes))
    return jobs


def poll_all_report_jobs(jobs: list[dict], supabase: SupabaseClient, token: str) -> list[dict]:
    """Phase B — poll every job status in parallel until COMPLETED/FAILED or timeout.
    Returns the jobs that completed; failures and timeouts are recorded."""
    pending = list(jobs)
    completed: list[dict] = []
    deadline = time.time() + REPORT_POLL_TIMEOUT
    while pending and time.time() < deadline:
        statuses: dict[int, str] = {}
        errors: dict[int, BaseException] = {}
        poll_workers = min(max(DOWNLOAD_CONCURRENCY * 2, 8), len(pending))
        poll_workers = min(poll_workers, 8)
        with ThreadPoolExecutor(max_workers=poll_workers) as executor:
            futures = {
                executor.submit(get_report_job_status, j["network_code"], j["job_id"], token): j
                for j in pending
            }
            for future in as_completed(futures):
                job = futures[future]
                try:
                    statuses[id(job)] = future.result()
                except Exception as e:
                    statuses[id(job)] = "FAILED"
                    errors[id(job)] = e

        still_pending = []
        for job in pending:
            status = statuses.get(id(job), "IN_PROGRESS")
            if status == "COMPLETED":
                completed.append(job)
            elif status == "FAILED":
                record_sync_failure(
                    supabase, job["network_code"],
                    errors.get(id(job)) or RuntimeError(f"Report job {job['job_id']} failed"),
                    "phase-b-poll",
                )
            else:
                still_pending.append(job)
        pending = still_pending
        if pending:
            log.info("Waiting for %d report job(s)...", len(pending))
            time.sleep(REPORT_POLL_INTERVAL)

    for job in pending:
        record_sync_failure(supabase, job["network_code"], RuntimeError(f"Report job {job['job_id']} timed out"), "phase-b-timeout")
    return completed


def upsert_daily_rows(supabase: SupabaseClient, network_code: str, rows: list[dict], now_iso: str) -> int:
    """Upsert fetched report rows into adx_daily_stats (batched, 500/call).
    Report rows are per (date, app, OS); they're collapsed back to per (date, app)
    for adx_daily_stats, and the per-OS impression split is stored in adx_os_stats
    (used for the dashboard's iOS % column)."""
    if not rows:
        return 0

    # Replace, don't accumulate: every cycle re-fetches [effective_start, today],
    # so first delete this code's rows already stored in the fetched date range.
    # Prevents duplicate/leftover rows from piling up and flipping daily totals.
    _dates = sorted({r["date"] for r in rows})
    if _dates:
        _min, _max = _dates[0], _dates[-1]
        try:
            supabase.table("adx_daily_stats").delete().eq("network_code", network_code).gte("date", _min).lte("date", _max).execute()
        except Exception as e:
            log.warning("pre-delete adx_daily_stats failed for %s: %s", network_code, str(e)[:200])

    to_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "platform": r["platform"],
            "revenue": r["revenue"],
            "ecpm": r["ecpm"],
            "impressions": r["impressions"],
            "clicks": r["clicks"],
            "ctr": r["ctr"],
            "updated_at": now_iso,
        }
        for r in aggregate_daily_rows(rows)
    ]
    total = 0
    for i in range(0, len(to_upsert), 500):
        batch = to_upsert[i:i + 500]
        resp = supabase.table("adx_daily_stats").upsert(batch, on_conflict="network_code,date,platform").execute()
        total += len(resp.data) if resp.data else 0

    os_upsert = [
        {
            "network_code": network_code,
            "date": r["date"],
            "os": r["os"],
            "impressions": r["impressions"],
            "updated_at": now_iso,
        }
        for r in aggregate_os_rows(rows)
    ]
    if _dates:
        try:
            supabase.table("adx_os_stats").delete().eq("network_code", network_code).gte("date", _min).lte("date", _max).execute()
        except Exception as e:
            log.warning("pre-delete adx_os_stats failed for %s: %s", network_code, str(e)[:200])
    for i in range(0, len(os_upsert), 500):
        batch = os_upsert[i:i + 500]
        try:
            supabase.table("adx_os_stats").upsert(batch, on_conflict="network_code,date,os").execute()
        except Exception as e:
            log.warning("adx_os_stats upsert failed for %s: %s", network_code, str(e)[:200])
    return total


def refresh_performance_bulk(supabase: SupabaseClient, downloaded: list[dict], now_iso: str) -> None:
    """Compute today / last-7-days summaries from the just-fetched rows and
    batch-upsert network_code_performance.

    Every cycle re-fetches the FULL [effective_start, today] window, which always
    covers [week_start, today]. Those rows are exactly what the SELECT-based
    per-code refresh reads, so computing from memory is 100% equivalent — but with
    ~2 bulk DB calls instead of 3 per code."""
    if not downloaded:
        return
    today = get_date_days_ago(0)
    week_start = get_date_days_ago(6)  # last 7 days inclusive (matches dashboard "7 days")

    # Existing first_data_date for every code — one paginated query.
    existing: dict[str, str] = {}
    try:
        offset = 0
        while True:
            resp = supabase.table("network_code_performance").select(
                "network_code, first_data_date"
            ).range(offset, offset + 999).execute()
            chunk = resp.data or []
            for r in chunk:
                existing[r.get("network_code")] = r.get("first_data_date")
            if len(chunk) < 1000:
                break
            offset += 1000
    except Exception as e:
        log.warning("Could not read existing performance rows: %s", str(e)[:200])

    upserts: list[dict] = []
    for d in downloaded:
        rows = d["rows"]
        if not rows:
            # Empty report: keep the previously stored summary untouched.
            continue
        t_rev = t_imp = t_clk = t_app = t_na = 0
        w_rev = w_imp = w_clk = w_app = w_na = 0
        t_imp_os = w_imp_os = t_ios = w_ios = 0
        first = existing.get(d["network_code"])
        for r in rows:
            rev = float(r.get("revenue") or 0)
            imp = int(r.get("impressions") or 0)
            clk = int(r.get("clicks") or 0)
            dt = r.get("date") or ""
            if not first or dt < first:
                first = dt
            is_na = (r.get("platform") or "").strip() == "(Not applicable)"
            is_ios = is_ios_os(r.get("os") or "")
            in_week = dt >= week_start
            if in_week:
                w_imp_os += imp
                if is_ios:
                    w_ios += imp
            if dt == today:
                t_rev += rev
                t_imp += imp
                t_clk += clk
                t_imp_os += imp
                if is_ios:
                    t_ios += imp
                if is_na:
                    t_na += rev
                else:
                    t_app += rev
            if in_week:
                w_rev += rev
                w_imp += imp
                w_clk += clk
                if is_na:
                    w_na += rev
                else:
                    w_app += rev
        upserts.append({
            "network_code": d["network_code"],
            "today_revenue": round(t_rev, 6),
            "today_impressions": t_imp,
            "today_clicks": t_clk,
            "week_revenue": round(w_rev, 6),
            "week_impressions": w_imp,
            "week_clicks": w_clk,
            "week_applicable_revenue": round(w_app, 6),
            "week_not_applicable_revenue": round(w_na, 6),
            "today_applicable_revenue": round(t_app, 6),
            "today_not_applicable_revenue": round(t_na, 6),
            "today_ios_share_pct": round((t_ios / t_imp_os) * 100, 2) if t_imp_os else 0,
            "week_ios_share_pct": round((w_ios / w_imp_os) * 100, 2) if w_imp_os else 0,
            "first_data_date": first,
            "last_synced_at": now_iso,
            "updated_at": now_iso,
        })

    for i in range(0, len(upserts), 500):
        supabase.table("network_code_performance").upsert(
            upserts[i:i + 500], on_conflict="network_code"
        ).execute()


def process_completed_jobs(completed_jobs: list[dict], supabase: SupabaseClient, token: str) -> tuple[int, int]:
    """Phase C — download all CSVs concurrently, upsert all rows, then batch
    refresh pre-computed summaries (no per-code DB round-trips).
    Returns (total_rows_upserted, error_count)."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # C1: download + parse all reports concurrently (keeps rows in memory)
    downloaded: list[dict] = []
    error_count = 0
    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as executor:
        def dl(job: dict) -> dict:
            try:
                rows = download_report_rows(job["network_code"], job["job_id"], token)
                return {"network_code": job["network_code"], "rows": rows}
            except Exception as e:
                return {"network_code": job["network_code"], "error": e}

        for res in executor.map(dl, completed_jobs):
            if "error" in res:
                record_sync_failure(supabase, res["network_code"], res["error"], "phase-c1-download")
                error_count += 1
            else:
                downloaded.append(res)

    # C2: upsert rows into adx_daily_stats concurrently
    def up(d: dict) -> tuple[str, bool]:
        try:
            db = _supabase_client()
            upsert_daily_rows(db, d["network_code"], d["rows"], now_iso)
            return (d["network_code"], True)
        except Exception as e:
            record_sync_failure(_supabase_client(), d["network_code"], e, "phase-c2-upsert")
            return ("", False)

    upserted_codes: list[str] = []
    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as executor:
        for code, ok in executor.map(up, downloaded):
            if ok:
                upserted_codes.append(code)
            else:
                error_count += 1

    # C3: batch refresh pre-computed summaries. The SQL RPC is the single source
    # of truth — it recomputes today / last-7 / last-30 day totals server-side
    # from the frozen raw tables, so the dashboard never drifts between cycles.
    # The in-memory fallback (refresh_performance_bulk) only runs when the
    # migration (migration_dashboard_totals.sql) hasn't been applied yet.
    rpc_ok = False
    try:
        supabase.rpc("refresh_dashboard_totals").execute()
        rpc_ok = True
    except Exception as e:
        log.warning("refresh_dashboard_totals skipped (migration not applied?): %s", str(e)[:200])
    if not rpc_ok:
        refresh_performance_bulk(supabase, downloaded, now_iso)

    # C4: advance last_synced_at + clear errors for non-empty reports (bulk)
    if upserted_codes:
        for i in range(0, len(upserted_codes), 100):
            chunk = upserted_codes[i:i + 100]
            try:
                supabase.table("network_codes").update({
                    "last_synced_at": now_iso,
                    "updated_at": now_iso,
                }).in_("network_code", chunk).execute()
                supabase.table("adx_sync_errors").delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Batch last_synced update failed: %s", str(e)[:200])

    # Empty (0-row) reports are a success but must NOT advance last_synced_at
    # (matches legacy per-code path) — only clear any stale error.
    empty_codes = [d["network_code"] for d in downloaded if not d["rows"]]
    if empty_codes:
        for i in range(0, len(empty_codes), 100):
            chunk = empty_codes[i:i + 100]
            try:
                supabase.table("adx_sync_errors").delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Batch error clear failed: %s", str(e)[:200])

    total_rows = sum(len(d["rows"]) for d in downloaded)
    log.info("Processed %d codes (%d rows)", len(upserted_codes), total_rows)
    return total_rows, error_count


def run_sync_cycle() -> dict:
    start_time = time.time()
    end_date = get_date_days_ago(0)
    week_start = get_date_days_ago(6)

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    token = get_gam_token_cached()

    # Step 1: Auto-fetch child publishers from MCM parent (if configured)
    mcm_stats = sync_network_codes(supabase, token)
    if mcm_stats.get("children_found", 0) > 0:
        log.info("MCM sync found %d children", mcm_stats["children_found"])

    # Step 1b: Parent-attributed earnings via mcm_earnings view (monthly)
    # DISABLED for now — will be reimplemented later.
    mcm_earnings_rows = 0

    # Step 2: Fetch all network codes from DB
    all_codes = fetch_all_network_codes(supabase, "*")
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
        # Non-ACTIVE codes are never re-synced, so clear any stale sync errors
        # for them — otherwise old transient/"deleted" errors would linger forever.
        for i in range(0, len(skipped_list), 100):
            chunk = skipped_list[i:i + 100]
            try:
                supabase.table("adx_sync_errors").delete().in_("network_code", chunk).execute()
            except Exception as e:
                log.warning("Stale error clear for non-ACTIVE codes failed: %s", str(e)[:200])

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
        # Join date = when this child first became ACTIVE under our parent
        # (active_since), falling back to created_at (first seen in MCM).
        # We never query earnings earlier than this so pre-join history is excluded.
        boundary = entry.get("active_since") or entry.get("created_at") or ""
        join_date = boundary.split("T")[0] if boundary else week_start
        if not join_date:
            join_date = week_start
        last_synced = entry.get("last_synced_at")
        if last_synced:
            # Incremental: a day is FINAL once it has passed (GAM T+1). Never
            # re-fetch days before yesterday except on the first cycle of a UTC
            # day (which re-pulls the rolling 7-day window to close out the
            # previous day). This freezes completed days — the #1 source of the
            # 7-day total flapping was re-writing finished days every 13 min.
            last_day = str(last_synced).split("T")[0]
            if last_day >= end_date:
                start = get_date_days_ago(1)  # already synced today: yesterday + today only
            else:
                start = effective_start(last_synced, week_start)  # first run of the day: full window
        else:
            # First sync — only fetch from the date the code was added
            start = join_date
        start = max(start, join_date)
        if code not in unique or start < (unique.get(code) or week_start):
            unique[code] = start

    codes = list(unique.items())
    log.info("Syncing %d ACTIVE network codes (submit_conc=%d, download_conc=%d)", len(codes), SUBMIT_CONCURRENCY, DOWNLOAD_CONCURRENCY)

    # Two-phase pipeline so GAM processes all child-network report jobs in
    # parallel: submit everything first, poll all in parallel, then download.
    submitted_jobs = submit_all_report_jobs(codes, end_date, supabase, token)
    completed_jobs = poll_all_report_jobs(submitted_jobs, supabase, token)
    total_rows, error_count = process_completed_jobs(completed_jobs, supabase, token)

    cutoff = get_date_days_ago(RETENTION_DAYS)
    deleted = cleanup_old_data(supabase, cutoff)

    elapsed = round(time.time() - start_time, 1)
    stats = {
        "status": "completed",
        "codes_found": len(all_codes),
        "codes_found_mcm": mcm_stats.get("children_found", 0),
        "mcm_earnings_rows": mcm_earnings_rows,
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
    log.info("Interval: %d min | Submit/Download: %d/%d | Retention: %d days", SYNC_INTERVAL, SUBMIT_CONCURRENCY, DOWNLOAD_CONCURRENCY, RETENTION_DAYS)
    log.info("Supabase: %s", SUPABASE_URL)
    log.info("=" * 60)

    # One-time cleanup: remove any adx_daily_stats rows older than a child's join date.
    # Idempotent — after the first run there is nothing left to remove.
    try:
        startup_supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        cleanup_prejoin_data(startup_supabase)
    except Exception as e:
        log.warning("Pre-join cleanup skipped: %s", str(e)[:200])

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
