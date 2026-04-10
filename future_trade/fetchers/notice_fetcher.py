"""
Notice fetcher for East Money announcements.
Fetches announcement list and full text from East Money API.
"""
import time
import json
from datetime import date
from typing import Optional

import requests

from config.postgres_config import get_settings


BASE_LIST_URL = "https://np-anotice-stock.eastmoney.com/api/security/ann"
BASE_DETAIL_URL = "https://np-cnotice-stock.eastmoney.com/api/content/ann"


def _fetch_json(url: str, params: dict, timeout: int = 30) -> Optional[dict]:
    """Fetch JSON from API with retry."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://data.eastmoney.com/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    return None


def fetch_notice_list(
    stock_code: str,
    start_date: str,
    end_date: str,
    f_node: int = 0,
    s_node: int = 0,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """
    Fetch announcement list for a stock within date range.

    Returns (list of notice metadata, total count).
    """
    all_notices = []
    page_index = 1
    total_hits = 0

    # Convert dates to sort_date range (YYYY-MM-DD format for filtering)
    # East Money API uses sort_date for ordering, format: YYYY-MM-DD
    params = {
        "sr": -1,
        "page_size": page_size,
        "page_index": page_index,
        "ann_type": "A",
        "client_source": "web",
        "stock_list": stock_code,
        "f_node": f_node,
        "s_node": s_node,
    }

    while True:
        params["page_index"] = page_index
        data = _fetch_json(BASE_LIST_URL, params)
        if not data or data.get("success") != 1:
            break

        result = data.get("data", {})
        notices = result.get("list", [])
        total_hits = result.get("total_hits", 0)

        if not notices:
            break

        for n in notices:
            notice_date = n.get("notice_date", "")[:10]  # YYYY-MM-DD
            if notice_date < start_date:
                return all_notices, total_hits
            all_notices.append(n)

        # Stop if we've collected enough or reached the end
        if page_index * page_size >= total_hits:
            break
        page_index += 1
        time.sleep(0.5)

    return all_notices, total_hits


def fetch_notice_detail(art_code: str) -> Optional[dict]:
    """
    Fetch full text content for a single announcement.
    Returns dict with notice_content (HTML) and attach_url.
    """
    params = {
        "art_code": art_code,
        "client_source": "web",
        "page_index": 1,
    }
    data = _fetch_json(BASE_DETAIL_URL, params)
    if not data or data.get("success") != 1:
        return None
    return data.get("data")


def parse_notice(n: dict) -> dict:
    """Parse notice metadata into flat dict."""
    codes = n.get("codes", [{}])
    code_info = codes[0] if codes else {}

    columns = n.get("columns", [])
    f_node_name = columns[0]["column_name"] if len(columns) > 0 else ""
    f_node_code = columns[0]["column_code"] if len(columns) > 0 else ""
    s_node_name = columns[1]["column_name"] if len(columns) > 1 else ""
    s_node_code = columns[1]["column_code"] if len(columns) > 1 else ""

    return {
        "art_code": n.get("art_code", ""),
        "stock_code": code_info.get("stock_code", ""),
        "stock_name": code_info.get("short_name", ""),
        "title": n.get("title", ""),
        "notice_date": n.get("notice_date", "")[:10],
        "f_node": None,
        "f_node_name": f_node_name,
        "s_node": None,
        "s_node_name": s_node_name,
        "attach_url": None,
        "full_text": None,
    }


def upsert_notice(notice: dict, full_text: Optional[str] = None, attach_url: Optional[str] = None) -> int:
    """
    Upsert a single announcement into the database.
    Returns 1 if inserted, 0 if updated.
    """
    from db import get_connection

    sql = """
    INSERT INTO announcements (
        art_code, stock_code, stock_name, title, notice_date,
        f_node, f_node_name, s_node, s_node_name,
        attach_url, full_text
    ) VALUES (
        %(art_code)s, %(stock_code)s, %(stock_name)s, %(title)s, %(notice_date)s,
        %(f_node)s, %(f_node_name)s, %(s_node)s, %(s_node_name)s,
        %(attach_url)s, %(full_text)s
    )
    ON CONFLICT (art_code) DO UPDATE SET
        title = EXCLUDED.title,
        f_node_name = EXCLUDED.f_node_name,
        s_node_name = EXCLUDED.s_node_name,
        full_text = EXCLUDED.full_text,
        attach_url = EXCLUDED.attach_url
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            notice_copy = dict(notice)
            notice_copy["attach_url"] = attach_url
            notice_copy["full_text"] = full_text
            cur.execute(sql, notice_copy)
    return 1
