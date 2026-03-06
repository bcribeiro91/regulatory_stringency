#!/usr/bin/env python3
"""
scrap_ANA_Agencies.py  —  v1 (2026-03)

Collects ALL rows from the ANA ERIs "Serviços" Power BI tab and saves to CSV.

━━  INSTALL ONCE  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Open an Anaconda Prompt (or cmd / PowerShell) and run:

      pip install playwright requests pandas
      playwright install chromium

━━  RUN  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      python scrap_ANA_Agencies.py

  Output CSVs are saved in the same folder as this script.

━━  STRATEGY  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  APPROACH A (primary — bypasses virtual scrolling entirely):
    1. Load the report in a headless browser with network monitoring.
    2. Capture every "querydata" POST request Power BI makes to its backend.
    3. Remove the row-count limit (Top / Count fields) from the request body.
    4. Replay the modified request directly via requests.post().
    5. Parse the Power BI DSR (Data Shape Result) JSON response, which
       includes delta-encoded rows (repeat bitmask R) and value dictionaries.
    6. Keep the result with the most rows across all captured requests.

  APPROACH B (fallback — used only if Approach A returns nothing):
    1. Find the frame containing the table (handles Power BI iframes).
    2. Click the table to give it keyboard focus.
    3. Scroll row-by-row with ArrowDown; deduplicate by full-row content
       signature so virtual-scroll duplicates are silently discarded.
    4. After 20 consecutive steps with no new rows, jump to Ctrl+End to
       capture any tail rows, then stop.
"""

import copy
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime

import pandas as pd
import requests
from playwright.sync_api import sync_playwright

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

PBI_URL = (
    "https://app.powerbi.com/view?r="
    "eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUw"
    "YmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9"
    "&pageName=ReportSectione2a118ad60b5488ab805"
)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Standard column name mapping by detected column count
COL_NAMES = {
    8:  ["Cod_IBGE", "UF", "Municipio", "CNPJ_ERI",
         "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI", "Data_inicio"],
    9:  ["Cod_IBGE", "UF", "Municipio", "CNPJ_ERI",
         "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI", "Data_inicio",
         "Servico"],
    10: ["Cod_IBGE", "UF", "Municipio", "CNPJ_ERI",
         "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI", "Data_inicio",
         "Servico", "Atividade"],
    11: ["Cod_IBGE", "UF", "Municipio", "CNPJ_ERI",
         "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI", "Data_inicio",
         "Servico", "Atividade", "Atribuicao"],
}


# ── APPROACH A HELPERS ────────────────────────────────────────────────────────

def strip_row_limits(obj):
    """Recursively remove Top / Count row-limit fields from a PBI query body."""
    if isinstance(obj, dict):
        obj.pop("Top",   None)
        obj.pop("Count", None)
        return {k: strip_row_limits(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_row_limits(v) for v in obj]
    return obj


def decode_cell(val, col_idx, value_dicts):
    """
    Resolve a Power BI cell value.
    If the value is an integer and a value dictionary exists for that column,
    look up the string; otherwise return the value as a string.
    """
    if val is None:
        return None
    if (
        isinstance(val, (int, float))
        and value_dicts
        and col_idx < len(value_dicts)
        and value_dicts[col_idx]
    ):
        d   = value_dicts[col_idx]
        idx = int(val)
        if 0 <= idx < len(d):
            return str(d[idx])
    return str(val)


def parse_dsr(resp_json):
    """
    Parse a Power BI DSR (Data Shape Result) API response into a DataFrame.

    DSR structure:
      results[] → result → data → dsr → DS[] → {
          SH:          column-name schema
          ValueDicts:  dictionary encoding (list-of-lists, one per column)
          PH:          page header containing row data in PH[].DM0[]
      }

    Each row (DM0 entry) has:
      C  — cell values (only columns that changed from the previous row)
      R  — bitmask: bit j set → column j repeats from previous row

    Returns the DataFrame with the most rows found, or None.
    """
    best_rows = []
    best_cols = []

    for r in resp_json.get("results", []):
        dsr = r.get("result", {}).get("data", {}).get("dsr", {})

        for ds in dsr.get("DS", []):

            # --- Column names from SH (Schema Header) ---
            col_names = []
            for sh in ds.get("SH", []):
                for dm in sh.get("DM0", []):
                    col_names.append(
                        dm.get("N") or dm.get("S") or f"Col{len(col_names)}"
                    )

            value_dicts = ds.get("ValueDicts") or []

            # --- Row data from PH (Page/Data Header) ---
            raw_rows = []
            for ph in ds.get("PH", []):
                raw_rows.extend(ph.get("DM0", []))

            if not raw_rows:
                continue

            n_cols = (
                len(col_names)
                if col_names
                else max((len(row.get("C") or []) for row in raw_rows), default=0)
            )
            if n_cols == 0:
                continue

            schema_preview = ", ".join(col_names[:5]) + ("..." if len(col_names) > 5 else "")
            print(f"    DS block: {len(raw_rows)} rows, {n_cols} cols"
                  + (f"  [{schema_preview}]" if col_names else ""))

            # --- Decode rows (repeat-flag / delta encoding) ---
            prev = [None] * n_cols
            decoded = []

            for row in raw_rows:
                cells  = row.get("C") or []
                R_flag = row.get("R")
                cur    = [None] * n_cols

                if R_flag is not None and R_flag != 0:
                    # Bit j set → column j repeats from previous row
                    R_int    = int(R_flag)
                    cell_idx = 0
                    for j in range(n_cols):
                        if R_int & (1 << j):
                            cur[j] = prev[j]          # repeat
                        else:
                            if cell_idx < len(cells):
                                cur[j] = decode_cell(cells[cell_idx], j, value_dicts)
                                cell_idx += 1
                else:
                    # No repeat flag: each cell maps directly to a column
                    for j, v in enumerate(cells):
                        if j < n_cols:
                            cur[j] = decode_cell(v, j, value_dicts)

                decoded.append(cur)
                prev = cur

            if len(decoded) > len(best_rows):
                best_rows = decoded
                best_cols = col_names if col_names else [f"Col{i}" for i in range(n_cols)]

    if not best_rows:
        return None

    n = len(best_rows[0])
    # Pad / trim col names to match actual row width
    cols = (best_cols + [f"Col{i}" for i in range(len(best_cols), n)])[:n]
    return pd.DataFrame(best_rows, columns=cols)


# ── APPROACH B HELPERS ────────────────────────────────────────────────────────

_GET_ROWS_JS = """() => {
  const cells = [];
  document.querySelectorAll('[role=gridcell]').forEach(el => {
    const r = el.getBoundingClientRect();
    if (r.width > 0 && r.height > 0) {
      const t = (el.getAttribute('title') || el.innerText || '').trim();
      if (t && t !== 'Select Row' && t !== 'Row Selection') {
        cells.push({ text: t, top: Math.round(r.top), left: Math.round(r.left) });
      }
    }
  });
  if (!cells.length) return [];
  cells.sort((a, b) => a.top - b.top);
  const rows = [];
  let cur = [cells[0]], curTop = cells[0].top;
  for (let i = 1; i < cells.length; i++) {
    if (Math.abs(cells[i].top - curTop) <= 10) {
      cur.push(cells[i]);
    } else {
      cur.sort((a, b) => a.left - b.left);
      rows.push(cur.map(c => c.text));
      cur = [cells[i]]; curTop = cells[i].top;
    }
  }
  cur.sort((a, b) => a.left - b.left);
  rows.push(cur.map(c => c.text));
  return rows.filter(r => r.length >= 7 && r.length <= 12);
}"""


def _find_table_frame(page):
    """
    Return the frame that contains the Power BI table gridcells.
    Power BI sometimes renders inside nested iframes; this finds the right one.
    """
    for frame in page.frames:
        try:
            n = frame.evaluate(
                "document.querySelectorAll('[role=gridcell]').length"
            )
            if n > 0:
                print(f"  Table found in frame: {frame.url[:70]}")
                return frame
        except Exception:
            pass
    print("  Table not found in any sub-frame; using main frame")
    return page.main_frame


def scroll_extract(page):
    """
    Scroll the Power BI table row-by-row using ArrowDown.
    Every visible row is deduplicated by its full content signature.
    Returns a DataFrame or None.
    """
    print("\nApproach B: scroll-based extraction (ArrowDown + content dedup)")

    frame = _find_table_frame(page)

    # Give the table keyboard focus
    try:
        frame.locator("[role=gridcell]").first.click(timeout=5_000)
        print("  Table focused via click")
    except Exception as e:
        print(f"  Could not click table: {e}")

    # Go to absolute top
    page.keyboard.press("Control+Home")
    page.wait_for_timeout(2_500)
    try:
        frame.locator("[role=gridcell]").first.click(timeout=3_000)
    except Exception:
        pass
    page.wait_for_timeout(500)

    seen     = set()
    all_rows = []
    stale    = 0

    for it in range(8_000):
        visible = frame.evaluate(_GET_ROWS_JS)
        added   = 0
        for row in visible:
            sig = "|".join(row)
            if sig not in seen:
                seen.add(sig)
                all_rows.append(row)
                added += 1

        if added:
            stale = 0
            if it % 100 == 0 or added > 5:
                print(f"  iter {it:5d} | +{added:3d} new | total {len(all_rows):5d}")
        else:
            stale += 1

        # After 20 stale steps jump to the bottom to catch any remaining rows
        if stale == 20:
            print("  20 stale steps — jumping to Ctrl+End to capture tail rows...")
            page.keyboard.press("Control+End")
            page.wait_for_timeout(3_000)
            try:
                frame.locator("[role=gridcell]").first.click(timeout=3_000)
            except Exception:
                pass
            page.wait_for_timeout(500)
            tail_added = 0
            for row in frame.evaluate(_GET_ROWS_JS):
                sig = "|".join(row)
                if sig not in seen:
                    seen.add(sig)
                    all_rows.append(row)
                    tail_added += 1
            print(f"  +{tail_added} tail rows.  Total unique rows: {len(all_rows)}")
            break

        # 3 × ArrowDown per iteration (fine-grained — never skips a row)
        for _ in range(3):
            page.keyboard.press("ArrowDown")
        page.wait_for_timeout(150)

    if not all_rows:
        return None

    target_len = Counter(len(r) for r in all_rows).most_common(1)[0][0]
    valid = [r for r in all_rows if len(r) == target_len]
    return pd.DataFrame(valid)


# ── POST-PROCESSING ───────────────────────────────────────────────────────────

def clean_dataframe(df):
    """Assign column names, clean CNPJ / Cod_IBGE, remove dupes and NA rows."""
    nc = len(df.columns)
    df.columns = COL_NAMES.get(nc, [f"Col{i}" for i in range(nc)])

    # Replace junk values with None
    df = df.replace({"": None, "########": None})

    # CNPJ → 14-digit zero-padded string
    for col in [c for c in df.columns if "CNPJ" in c.upper()]:
        def _cnpj(x, _col=col):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            digits = re.sub(r"[^0-9]", "", str(x))
            return digits.zfill(14)[:14] if digits else None
        df[col] = df[col].apply(_cnpj)

    # Cod_IBGE → 7-digit zero-padded string
    for col in [c for c in df.columns if "IBGE" in c.upper()][:1]:
        def _ibge(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            digits = re.sub(r"[^0-9]", "", str(x))
            return digits.zfill(7) if digits else None
        df[col] = df[col].apply(_ibge)

    n_before = len(df)
    df = df.drop_duplicates().dropna(how="all").reset_index(drop=True)
    print(f"  {n_before} → {len(df)} rows after dedup / drop-NA rows")
    return df


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    captured = {}   # key → {url, headers, body}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,   # set True once confirmed working
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(viewport={"width": 1600, "height": 900})
        page    = context.new_page()

        # ── Capture every querydata POST (fired once per visual on load) ──────
        def on_request(req):
            if "querydata" in req.url.lower() and req.method == "POST":
                body = req.post_data
                if body:
                    key = f"{req.url}|{len(captured)}"
                    captured[key] = {
                        "url":     req.url,
                        "headers": dict(req.headers),
                        "body":    body,
                    }

        page.on("request", on_request)

        print("=" * 65)
        print("  ANA ERIs — Serviços tab")
        print("=" * 65)
        print("\nLoading Power BI report (30 s wait for all visuals)...\n")
        page.goto(PBI_URL)
        page.wait_for_timeout(30_000)

        print(f"\nCaptured {len(captured)} querydata request(s)\n")

        # ── APPROACH A: replay each captured request without row limits ───────
        df_api = None
        best_n = 0

        for key, req in captured.items():
            print(f"  Request → {req['url'][:68]}")
            try:
                body_obj = json.loads(req["body"])
            except (json.JSONDecodeError, TypeError):
                print("    Body is not JSON — skipping")
                continue

            modified = strip_row_limits(copy.deepcopy(body_obj))

            hdrs = {
                "Content-Type": "application/json",
                "Accept":       "application/json",
            }
            for h in (
                "X-PowerBI-ResourceKey",
                "X-PowerBI-EntitledToken",
                "ActivityId",
                "RequestId",
                "Authorization",
            ):
                if h in req["headers"]:
                    hdrs[h] = req["headers"][h]

            try:
                resp = requests.post(
                    req["url"],
                    data=json.dumps(modified),
                    headers=hdrs,
                    timeout=90,
                )
            except Exception as e:
                print(f"    POST error: {e}")
                continue

            print(f"    HTTP {resp.status_code}")
            if resp.status_code != 200:
                # If row-limit removal failed, retry with a very high Top value
                print("    Retrying with Top=500000...")
                def _set_high(obj):
                    if isinstance(obj, dict):
                        if "Top" in obj:
                            obj["Top"] = 500_000
                        if "Count" in obj:
                            obj["Count"] = 500_000
                        return {k: _set_high(v) for k, v in obj.items()}
                    if isinstance(obj, list):
                        return [_set_high(v) for v in obj]
                    return obj

                retry_body = _set_high(copy.deepcopy(body_obj))
                try:
                    resp = requests.post(
                        req["url"],
                        data=json.dumps(retry_body),
                        headers=hdrs,
                        timeout=90,
                    )
                    print(f"    Retry HTTP {resp.status_code}")
                except Exception:
                    continue

            if resp.status_code != 200:
                continue

            df_try = parse_dsr(resp.json())
            if df_try is not None and len(df_try) > best_n:
                best_n = len(df_try)
                df_api = df_try
                print(f"    Best so far: {best_n} rows × {len(df_try.columns)} cols")

        # ── APPROACH B: fallback if API gave nothing ──────────────────────────
        if df_api is None or len(df_api) == 0:
            print("\nApproach A returned no data — switching to scroll-based fallback\n")
            df_api = scroll_extract(page)

        browser.close()

    # ── POST-PROCESSING ───────────────────────────────────────────────────────
    if df_api is None or len(df_api) == 0:
        print("\nERROR: No data collected by either approach.")
        sys.exit(1)

    print("\n" + "=" * 65)
    print("  Post-processing")
    print("=" * 65)
    print(f"Raw: {len(df_api)} rows × {len(df_api.columns)} cols")
    df = clean_dataframe(df_api)

    # ── VALIDATION ────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  Validation")
    print("=" * 65)
    print(f"Final: {len(df)} rows × {len(df.columns)} cols")
    print(f"Columns: {' | '.join(df.columns.tolist())}\n")

    if "UF" in df.columns:
        counts = df["UF"].value_counts().sort_index()
        print(f"States ({df['UF'].nunique()}):\n{counts.to_string()}\n")
    if "Municipio"  in df.columns:
        print(f"Unique municipalities : {df['Municipio'].nunique()}")
    if "Sigla_ERI"  in df.columns:
        print(f"Unique ERIs (sigla)   : {df['Sigla_ERI'].nunique()}")
    if "CNPJ_ERI"   in df.columns:
        print(f"Unique ERIs (CNPJ)    : {df['CNPJ_ERI'].nunique()}")

    print(f"\nFirst 10 rows:\n{df.head(10).to_string(index=False)}")
    print(f"\nLast  5 rows:\n{df.tail(5).to_string(index=False)}")

    # ── EXPORT ────────────────────────────────────────────────────────────────
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_ts   = os.path.join(OUTPUT_DIR, f"ANA_ERIs_Servicos_{ts}.csv")
    out_main = os.path.join(OUTPUT_DIR, "ANA_ERIs_Servicos.csv")

    # utf-8-sig adds a BOM so Excel opens it correctly without garbled characters
    df.to_csv(out_ts,   index=False, encoding="utf-8-sig")
    df.to_csv(out_main, index=False, encoding="utf-8-sig")

    print(f"\nSaved:\n  {out_ts}\n  {out_main}")
    print("\nDone!")


if __name__ == "__main__":
    # Playwright on Windows needs the ProactorEventLoop (Python 3.8+)
    if sys.platform == "win32":
        import asyncio
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    main()
