#!/usr/bin/env python3
"""
scrap_ANA_Agencies_colab.py  —  Google Colab version

Paste each CELL block into a separate Colab cell and run in order.

━━  CELL 1 — Install  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  !pip install -q playwright requests pandas
  !playwright install --with-deps chromium

━━  CELL 2 onwards — paste the code below  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

# ── CELL 2: imports ────────────────────────────────────────────────────────────
import copy, json, os, re, sys
from collections import Counter
from datetime import datetime

import pandas as pd
import requests
from playwright.sync_api import sync_playwright

PBI_URL = (
    "https://app.powerbi.com/view?r="
    "eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUw"
    "YmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9"
    "&pageName=ReportSectione2a118ad60b5488ab805"
)

OUTPUT_DIR = "/content"   # Colab: saves to Files pane  (change to a Drive path if needed)

COL_NAMES = {
    8:  ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio"],
    9:  ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Servico"],
    10: ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Servico","Atividade"],
    11: ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Servico","Atividade","Atribuicao"],
}

print("Imports OK")


# ── CELL 3: helpers ────────────────────────────────────────────────────────────

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
    """Resolve a Power BI cell value (handles value-dictionary references)."""
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
    Parse a Power BI DSR (Data Shape Result) JSON response into a DataFrame.

    DSR structure:
      results[] → result → data → dsr → DS[] → {
          SH          — column-name schema
          ValueDicts  — string dictionaries (one list per column)
          PH          — page header containing rows in PH[].DM0[]
      }
    Each row (DM0 entry):
      C  — cell values (only changed columns)
      R  — bitmask: bit j set → column j repeats from the previous row

    Returns the DataFrame with the most rows, or None.
    """
    best_rows, best_cols = [], []

    for r in resp_json.get("results", []):
        dsr = r.get("result", {}).get("data", {}).get("dsr", {})

        for ds in dsr.get("DS", []):
            # Column names
            col_names = []
            for sh in ds.get("SH", []):
                for dm in sh.get("DM0", []):
                    col_names.append(dm.get("N") or dm.get("S") or f"Col{len(col_names)}")

            value_dicts = ds.get("ValueDicts") or []

            # Row data
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

            preview = ", ".join(col_names[:5]) + ("..." if len(col_names) > 5 else "")
            print(f"  DS: {len(raw_rows)} rows, {n_cols} cols [{preview}]")

            # Decode rows (repeat-flag / delta encoding)
            prev, decoded = [None] * n_cols, []

            for row in raw_rows:
                cells, R_flag = row.get("C") or [], row.get("R")
                cur = [None] * n_cols

                if R_flag is not None and R_flag != 0:
                    R_int, cell_idx = int(R_flag), 0
                    for j in range(n_cols):
                        if R_int & (1 << j):
                            cur[j] = prev[j]
                        elif cell_idx < len(cells):
                            cur[j] = decode_cell(cells[cell_idx], j, value_dicts)
                            cell_idx += 1
                else:
                    for j, v in enumerate(cells):
                        if j < n_cols:
                            cur[j] = decode_cell(v, j, value_dicts)

                decoded.append(cur)
                prev = cur

            if len(decoded) > len(best_rows):
                best_rows = decoded
                best_cols = col_names or [f"Col{i}" for i in range(n_cols)]

    if not best_rows:
        return None

    n    = len(best_rows[0])
    cols = (best_cols + [f"Col{i}" for i in range(len(best_cols), n)])[:n]
    return pd.DataFrame(best_rows, columns=cols)


# ── Scroll-based fallback ──────────────────────────────────────────────────────

_GET_ROWS_JS = """() => {
  const cells = [];
  document.querySelectorAll('[role=gridcell]').forEach(el => {
    const r = el.getBoundingClientRect();
    if (r.width > 0 && r.height > 0) {
      const t = (el.getAttribute('title') || el.innerText || '').trim();
      if (t && t !== 'Select Row' && t !== 'Row Selection')
        cells.push({ text: t, top: Math.round(r.top), left: Math.round(r.left) });
    }
  });
  if (!cells.length) return [];
  cells.sort((a,b) => a.top - b.top);
  const rows = []; let cur = [cells[0]], curTop = cells[0].top;
  for (let i = 1; i < cells.length; i++) {
    if (Math.abs(cells[i].top - curTop) <= 10) { cur.push(cells[i]); }
    else {
      cur.sort((a,b)=>a.left-b.left); rows.push(cur.map(c=>c.text));
      cur=[cells[i]]; curTop=cells[i].top;
    }
  }
  cur.sort((a,b)=>a.left-b.left); rows.push(cur.map(c=>c.text));
  return rows.filter(r => r.length>=7 && r.length<=12);
}"""


def _find_table_frame(page):
    """Return the frame that contains the Power BI table gridcells."""
    for frame in page.frames:
        try:
            if frame.evaluate("document.querySelectorAll('[role=gridcell]').length") > 0:
                print(f"  Table found in frame: {frame.url[:70]}")
                return frame
        except Exception:
            pass
    print("  Using main frame")
    return page.main_frame


def scroll_extract(page):
    """Row-by-row ArrowDown scroll with full-content deduplication."""
    print("\nApproach B: scroll-based extraction")
    frame = _find_table_frame(page)

    # Focus the table
    try:
        frame.locator("[role=gridcell]").first.click(timeout=5_000)
        print("  Table focused")
    except Exception as e:
        print(f"  Could not click table: {e}")

    page.keyboard.press("Control+Home")
    page.wait_for_timeout(2_500)
    try:
        frame.locator("[role=gridcell]").first.click(timeout=3_000)
    except Exception:
        pass
    page.wait_for_timeout(500)

    seen, all_rows, stale = set(), [], 0

    for it in range(8_000):
        visible = frame.evaluate(_GET_ROWS_JS)
        added   = 0
        for row in visible:
            sig = "|".join(row)
            if sig not in seen:
                seen.add(sig); all_rows.append(row); added += 1

        if added:
            stale = 0
            if it % 100 == 0 or added > 5:
                print(f"  iter {it:5d} | +{added:3d} new | total {len(all_rows):5d}")
        else:
            stale += 1

        if stale == 20:
            print("  20 stale — Ctrl+End to catch tail rows...")
            page.keyboard.press("Control+End")
            page.wait_for_timeout(3_000)
            try:
                frame.locator("[role=gridcell]").first.click(timeout=3_000)
            except Exception:
                pass
            page.wait_for_timeout(500)
            for row in frame.evaluate(_GET_ROWS_JS):
                sig = "|".join(row)
                if sig not in seen:
                    seen.add(sig); all_rows.append(row)
            print(f"  Done. Total unique rows: {len(all_rows)}")
            break

        for _ in range(3):
            page.keyboard.press("ArrowDown")
        page.wait_for_timeout(150)

    if not all_rows:
        return None

    target_len = Counter(len(r) for r in all_rows).most_common(1)[0][0]
    return pd.DataFrame([r for r in all_rows if len(r) == target_len])


def clean_dataframe(df):
    """Assign column names, clean CNPJ / Cod_IBGE, dedup."""
    nc = len(df.columns)
    df.columns = COL_NAMES.get(nc, [f"Col{i}" for i in range(nc)])
    df = df.replace({"": None, "########": None})

    for col in [c for c in df.columns if "CNPJ" in c.upper()]:
        df[col] = df[col].apply(
            lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x)))
                      else re.sub(r"[^0-9]","",str(x)).zfill(14)[:14] or None)

    for col in [c for c in df.columns if "IBGE" in c.upper()][:1]:
        df[col] = df[col].apply(
            lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x)))
                      else re.sub(r"[^0-9]","",str(x)).zfill(7) or None)

    n_before = len(df)
    df = df.drop_duplicates().dropna(how="all").reset_index(drop=True)
    print(f"  {n_before} → {len(df)} rows after dedup / drop-NA")
    return df


print("Helpers defined OK")


# ── CELL 4: main scraping function ────────────────────────────────────────────

def run_scraper():
    captured = {}

    with sync_playwright() as p:
        # ── Launch Chromium (headless + Colab sandbox flags) ─────────────────
        browser = p.chromium.launch(
            headless=True,                          # must be True in Colab
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",          # prevents /dev/shm OOM in Colab
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(viewport={"width": 1600, "height": 900})
        page    = context.new_page()

        # ── Capture querydata POST requests ──────────────────────────────────
        def on_request(req):
            if "querydata" in req.url.lower() and req.method == "POST":
                body = req.post_data
                if body:
                    captured[f"{req.url}|{len(captured)}"] = {
                        "url": req.url, "headers": dict(req.headers), "body": body
                    }

        page.on("request", on_request)

        print("=" * 65)
        print("  ANA ERIs — Serviços tab")
        print("=" * 65)
        print("\nLoading Power BI report (30 s wait)...\n")
        page.goto(PBI_URL)
        page.wait_for_timeout(30_000)
        print(f"\nCaptured {len(captured)} querydata request(s)\n")

        # ── APPROACH A: replay without row limits ─────────────────────────────
        df_api, best_n = None, 0

        for key, req in captured.items():
            print(f"  Request → {req['url'][:68]}")
            try:
                body_obj = json.loads(req["body"])
            except (json.JSONDecodeError, TypeError):
                print("    Not JSON — skip"); continue

            modified = strip_row_limits(copy.deepcopy(body_obj))

            hdrs = {"Content-Type":"application/json","Accept":"application/json"}
            for h in ("X-PowerBI-ResourceKey","X-PowerBI-EntitledToken",
                       "ActivityId","RequestId","Authorization"):
                if h in req["headers"]:
                    hdrs[h] = req["headers"][h]

            try:
                resp = requests.post(req["url"], data=json.dumps(modified),
                                     headers=hdrs, timeout=90)
            except Exception as e:
                print(f"    POST error: {e}"); continue

            print(f"    HTTP {resp.status_code}")

            # If Top removal was rejected, retry with a very high value
            if resp.status_code != 200:
                print("    Retrying with Top=500000...")
                def _high(o):
                    if isinstance(o, dict):
                        if "Top"   in o: o["Top"]   = 500_000
                        if "Count" in o: o["Count"] = 500_000
                        return {k: _high(v) for k, v in o.items()}
                    if isinstance(o, list): return [_high(v) for v in o]
                    return o
                try:
                    resp = requests.post(req["url"],
                                         data=json.dumps(_high(copy.deepcopy(body_obj))),
                                         headers=hdrs, timeout=90)
                    print(f"    Retry HTTP {resp.status_code}")
                except Exception:
                    continue

            if resp.status_code != 200:
                continue

            df_try = parse_dsr(resp.json())
            if df_try is not None and len(df_try) > best_n:
                best_n, df_api = len(df_try), df_try
                print(f"    Best so far: {best_n} rows × {len(df_try.columns)} cols")

        # ── APPROACH B fallback ───────────────────────────────────────────────
        if df_api is None or len(df_api) == 0:
            print("\nApproach A returned no data — scroll-based fallback\n")
            df_api = scroll_extract(page)

        browser.close()

    return df_api


print("run_scraper() defined OK")


# ── CELL 5: run and save ───────────────────────────────────────────────────────

df_raw = run_scraper()

if df_raw is None or len(df_raw) == 0:
    print("ERROR: No data collected.")
else:
    print(f"\nRaw: {len(df_raw)} rows × {len(df_raw.columns)} cols")
    df = clean_dataframe(df_raw)

    # Validation
    print(f"\nFinal: {len(df)} rows × {len(df.columns)} cols")
    print(f"Columns: {' | '.join(df.columns.tolist())}")
    if "UF"         in df.columns: print(f"States:            {df['UF'].nunique()}")
    if "Municipio"  in df.columns: print(f"Municipalities:    {df['Municipio'].nunique()}")
    if "Sigla_ERI"  in df.columns: print(f"ERIs (sigla):      {df['Sigla_ERI'].nunique()}")
    if "CNPJ_ERI"   in df.columns: print(f"ERIs (CNPJ):       {df['CNPJ_ERI'].nunique()}")

    print(f"\nFirst 10 rows:\n{df.head(10).to_string(index=False)}")
    print(f"\nLast  5 rows:\n{df.tail(5).to_string(index=False)}")

    # Save
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_ts   = os.path.join(OUTPUT_DIR, f"ANA_ERIs_Servicos_{ts}.csv")
    out_main = os.path.join(OUTPUT_DIR, "ANA_ERIs_Servicos.csv")

    df.to_csv(out_ts,   index=False, encoding="utf-8-sig")
    df.to_csv(out_main, index=False, encoding="utf-8-sig")
    print(f"\nSaved to:\n  {out_ts}\n  {out_main}")

    # Make it easy to download from Colab's file browser
    try:
        from google.colab import files
        files.download(out_main)
    except Exception:
        print("(Run from Colab Files panel to download manually)")

    print("\nDone!")
