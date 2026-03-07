#!/usr/bin/env python3
"""
scrap_ANA_Agencies_colab.py  —  Google Colab version
Run each CELL block in a separate Colab cell, top to bottom.

━━  CELL 1 — Install (run once per Colab session)  ━━━━━━━━━━━━━━━━━━━━━━━━━━
  !pip install -q playwright nest_asyncio pandas
  !playwright install --with-deps chromium
"""

# ══════════════════════════════════════════════════════════════════════════════
# CELL 2 — Imports & config
# ══════════════════════════════════════════════════════════════════════════════
import copy, json, os, re
from datetime import datetime
from collections import Counter

import pandas as pd
import nest_asyncio, asyncio
from playwright.async_api import async_playwright

nest_asyncio.apply()   # lets asyncio.run() work inside Colab's event loop

PBI_URL = (
    "https://app.powerbi.com/view?r="
    "eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUw"
    "YmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9"
    "&pageName=ReportSectione2a118ad60b5488ab805"
)
OUTPUT_DIR = "/content"

# Column-name mapping by number of columns found in the DSR schema.
# Keys are added dynamically once we learn the real column count.
COL_NAMES = {
    8:  ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio"],
    9:  ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Setorialidade"],
    10: ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Data_validade","Setorialidade"],
    11: ["Cod_IBGE","UF","Municipio","CNPJ_ERI",
         "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Data_validade","Setorialidade","Extra"],
}

print("Imports OK")


# ══════════════════════════════════════════════════════════════════════════════
# CELL 3 — Helpers  (decode_cell, parse_dsr, clean_dataframe)
# ══════════════════════════════════════════════════════════════════════════════

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

    value_dicts can arrive as:
      - None / []        → no dictionary; return raw value
      - list[list]       → value_dicts[col_idx] is the lookup list
      - dict{str: list}  → JSON-serialised object; keys are STRING column indices
      - dict{int: list}  → (rare) integer-keyed dict

    The original code assumed list and did value_dicts[col_idx], which raises
    KeyError:0 when value_dicts is a dict (JSON object keys are always strings).
    """
    if val is None:
        return None
    if isinstance(val, (int, float)) and value_dicts:
        d = None
        if isinstance(value_dicts, list):
            if col_idx < len(value_dicts):
                d = value_dicts[col_idx]
        elif isinstance(value_dicts, dict):
            # JSON keys are strings; try str(col_idx) first, then int key
            d = value_dicts.get(str(col_idx)) or value_dicts.get(col_idx)
        if d:
            idx = int(val)
            if 0 <= idx < len(d):
                return str(d[idx])
    return str(val)


def parse_dsr(resp_json):
    """
    Parse a Power BI DSR (Data Shape Result) JSON into a DataFrame.

    DSR layout:
      results[] → result → data → dsr → DS[] → {
          SH         — column-schema  (DM0[].N or .S = column name)
          ValueDicts — string dictionaries (list OR dict of lists)
          PH         — page header; rows are in PH[].DM0[]
      }

    Row delta-encoding (field R):
      R is a bitmask; bit j set → column j repeats the previous row's value.
      Supplied cells (C) fill only the non-repeating columns, left to right.

    Returns the DataFrame with the most rows, or None.
    """
    best_rows, best_cols = [], []

    for r in resp_json.get("results", []):
        dsr = r.get("result", {}).get("data", {}).get("dsr", {})

        for ds in dsr.get("DS", []):
            # ── Column names from schema ──────────────────────────────────────
            col_names = []
            for sh in ds.get("SH", []):
                for dm in sh.get("DM0", []):
                    col_names.append(dm.get("N") or dm.get("S") or f"Col{len(col_names)}")

            # ── Value dictionaries (may be list or dict) ──────────────────────
            value_dicts = ds.get("ValueDicts")   # None, list, or dict

            # ── Row data ─────────────────────────────────────────────────────
            raw_rows = []
            for ph in ds.get("PH", []):
                raw_rows.extend(ph.get("DM0", []))

            if not raw_rows:
                continue

            n_cols = (
                len(col_names) if col_names
                else max((len(row.get("C") or []) for row in raw_rows), default=0)
            )
            if n_cols == 0:
                continue

            # Debug: print schema column names so we can build the right mapping
            print(f"  DS: {len(raw_rows)} rows, {n_cols} cols")
            print(f"       Schema cols: {col_names}")
            vd_type = (type(value_dicts).__name__ if value_dicts is not None
                       else "None")
            print(f"       ValueDicts type: {vd_type}")

            # ── Decode rows ───────────────────────────────────────────────────
            prev, decoded = [None] * n_cols, []

            for row in raw_rows:
                cells  = row.get("C") or []
                R_flag = row.get("R")
                cur    = [None] * n_cols

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
  return rows.filter(r => r.length>=7 && r.length<=15);
}"""

# JS that scrolls the Power BI virtualised table container
_SCROLL_TABLE_JS = """(delta) => {
  // Selectors that cover PBI table scroll wrapper across different versions
  const sel = [
    '.tableEx-scroll-wrapper',
    '.scroll-wrapper',
    'div[class*="scroll"][role="presentation"]',
    'div[data-testid="visual-scroll-wrapper"]',
    '[role="grid"]',
    '[role="rowgroup"]',
  ];
  for (const s of sel) {
    const el = document.querySelector(s);
    if (el && el.scrollHeight > el.clientHeight) {
      el.scrollTop += delta;
      return `Scrolled ${s}: scrollTop=${el.scrollTop}`;
    }
  }
  // Fallback: scroll the element with the most scrollable height
  let best = null, bestH = 0;
  document.querySelectorAll('*').forEach(el => {
    const sh = el.scrollHeight - el.clientHeight;
    if (sh > bestH) { bestH = sh; best = el; }
  });
  if (best) { best.scrollTop += delta; return `Scrolled fallback (sh=${bestH}): scrollTop=${best.scrollTop}`; }
  return 'No scrollable element found';
}"""


def clean_dataframe(df, raw_col_names=None):
    """
    Assign human-readable column names, clean CNPJ / Cod_IBGE, dedup.

    If raw_col_names (the DSR schema names) are provided and the count matches
    a known mapping, that mapping is used.  Otherwise falls back to COL_NAMES
    by column count.
    """
    nc = len(df.columns)
    df.columns = COL_NAMES.get(nc, [f"Col{i}" for i in range(nc)])
    df = df.replace({"": None, "########": None})

    for col in [c for c in df.columns if "CNPJ" in c.upper()]:
        df[col] = df[col].apply(
            lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x)))
                      else re.sub(r"[^0-9]", "", str(x)).zfill(14)[:14] or None)

    for col in [c for c in df.columns if "IBGE" in c.upper()][:1]:
        df[col] = df[col].apply(
            lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x)))
                      else re.sub(r"[^0-9]", "", str(x)).zfill(7) or None)

    n_before = len(df)
    df = df.drop_duplicates().dropna(how="all").reset_index(drop=True)
    print(f"  {n_before} → {len(df)} rows after dedup/drop-NA")
    return df


print("Helpers defined OK")


# ══════════════════════════════════════════════════════════════════════════════
# CELL 4 — Async scraper  (route interception + scroll pagination)
# ══════════════════════════════════════════════════════════════════════════════

async def run_scraper():
    """
    Strategy:
      1. Intercept all Power BI querydata POST requests via page.route().
         For each, strip Top/Count row limits and re-fetch using the browser's
         own credentials (route.fetch keeps cookies & auth tokens → no 401).
      2. After the initial load, scroll the virtualised table via JS to trigger
         further querydata requests (Power BI lazy-loads rows on scroll).
      3. Collect and parse all intercepted DSR responses.
      4. Fall back to DOM-scraping if the API approach yields nothing.
    """
    captured_data   = []   # list of successfully parsed JSON bodies
    intercept_count = [0]  # mutable counter for use inside the closure

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(viewport={"width": 1600, "height": 900})
        page    = await context.new_page()

        # ── Route interceptor ─────────────────────────────────────────────────
        async def intercept_querydata(route, request):
            if request.method != "POST":
                await route.continue_()
                return
            intercept_count[0] += 1
            tag = intercept_count[0]
            try:
                body     = json.loads(request.post_data or "{}")
                modified = strip_row_limits(copy.deepcopy(body))
                print(f"  [{tag}] Intercepted querydata — row limits stripped")

                # Re-send with the browser's own auth (no external requests.post)
                resp = await route.fetch(post_data=json.dumps(modified))

                try:
                    raw  = await resp.body()
                    data = json.loads(raw)
                    captured_data.append(data)
                    # Quick sanity check (also prints DSR schema for debugging)
                    df_chk = parse_dsr(data)
                    if df_chk is not None:
                        print(f"  [{tag}] → {len(df_chk)} rows × "
                              f"{len(df_chk.columns)} cols parsed OK")
                    else:
                        print(f"  [{tag}] → parse_dsr returned None "
                              f"(response may not contain row data)")
                except Exception as e:
                    import traceback
                    print(f"  [{tag}] Parse error: {e}")
                    traceback.print_exc()

                await route.fulfill(response=resp)

            except Exception as e:
                print(f"  [{tag}] Route error: {e}")
                await route.continue_()

        await page.route("**/*querydata*", intercept_querydata)

        # ── Load the report ───────────────────────────────────────────────────
        print("=" * 65)
        print("  ANA ERIs — Serviços tab")
        print("=" * 65)
        print("\nLoading Power BI report (45 s initial wait)…\n")
        await page.goto(PBI_URL, timeout=90_000)
        await page.wait_for_timeout(45_000)
        print(f"\n{intercept_count[0]} querydata request(s) captured so far.\n")

        # ── Scroll table to trigger pagination ────────────────────────────────
        print("Scrolling table to load all rows…")
        prev_count = intercept_count[0]

        for scroll_pass in range(40):
            scroll_msg = await page.evaluate(_SCROLL_TABLE_JS, 400)
            await page.wait_for_timeout(1_500)

            if scroll_pass % 10 == 0:
                print(f"  scroll pass {scroll_pass:2d}: {scroll_msg}")

            # Stop early if no new requests after 10 consecutive passes
            if scroll_pass > 0 and scroll_pass % 10 == 9:
                if intercept_count[0] == prev_count:
                    print("  No new requests in last 10 passes — table fully loaded.")
                    break
                prev_count = intercept_count[0]

        print(f"\nTotal querydata responses captured: {len(captured_data)}\n")

        # ── Build DataFrame from captured API responses ───────────────────────
        all_dfs = []
        for i, data in enumerate(captured_data):
            try:
                df_try = parse_dsr(data)
                if df_try is not None and len(df_try) > 0:
                    print(f"  Response {i+1}: {len(df_try)} rows × "
                          f"{len(df_try.columns)} cols → added")
                    all_dfs.append(df_try)
            except Exception as e:
                import traceback
                print(f"  Response {i+1}: parse failed — {e}")
                traceback.print_exc()

        df_api = None
        if all_dfs:
            df_api = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
            print(f"\nAPI result: {len(df_api)} rows × {len(df_api.columns)} cols")

        # ── DOM-scroll fallback ───────────────────────────────────────────────
        if df_api is None or len(df_api) == 0:
            print("\nAPI interception returned no data — trying DOM scroll fallback…\n")
            df_api = await _scroll_extract_async(page)

        await browser.close()

    return df_api


async def _scroll_extract_async(page):
    """Async DOM-scroll fallback: read gridcells row by row."""
    print("Approach B: async DOM-scroll extraction")

    # Try to click the first gridcell to focus the table
    try:
        await page.locator("[role=gridcell]").first.click(timeout=8_000)
        print("  Table gridcell focused")
    except Exception as e:
        print(f"  Could not click gridcell: {e}")

    await page.wait_for_timeout(2_000)

    seen, all_rows, stale = set(), [], 0

    for it in range(6_000):
        visible = await page.evaluate(_GET_ROWS_JS)
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

        if stale == 15:
            # Try scrolling the table container instead of arrow keys
            scroll_msg = await page.evaluate(_SCROLL_TABLE_JS, 300)
            if it % 30 == 0:
                print(f"  stale {stale} — {scroll_msg}")

        if stale >= 30:
            print(f"  30 stale iterations — done. Total rows: {len(all_rows)}")
            break

        # Scroll via JS (more reliable in iframes than keyboard events)
        await page.evaluate(_SCROLL_TABLE_JS, 150)
        await page.wait_for_timeout(200)

    if not all_rows:
        return None

    target_len = Counter(len(r) for r in all_rows).most_common(1)[0][0]
    return pd.DataFrame([r for r in all_rows if len(r) == target_len])


print("run_scraper() defined OK")


# ══════════════════════════════════════════════════════════════════════════════
# CELL 5 — Run and save
# ══════════════════════════════════════════════════════════════════════════════

df_raw = asyncio.run(run_scraper())

if df_raw is None or len(df_raw) == 0:
    print("\nERROR: No data collected.")
else:
    print(f"\nRaw: {len(df_raw)} rows × {len(df_raw.columns)} cols")
    print(f"Raw columns: {df_raw.columns.tolist()}")
    print(f"\nFirst 3 raw rows:\n{df_raw.head(3).to_string(index=False)}\n")

    df = clean_dataframe(df_raw)

    print(f"\nFinal: {len(df)} rows × {len(df.columns)} cols")
    print(f"Columns: {' | '.join(df.columns.tolist())}")
    if "UF"        in df.columns: print(f"States:            {df['UF'].nunique()}")
    if "Municipio" in df.columns: print(f"Municipalities:    {df['Municipio'].nunique()}")
    if "Sigla_ERI" in df.columns: print(f"ERIs (sigla):      {df['Sigla_ERI'].nunique()}")
    if "CNPJ_ERI"  in df.columns: print(f"ERIs (CNPJ):       {df['CNPJ_ERI'].nunique()}")

    print(f"\nFirst 10 rows:\n{df.head(10).to_string(index=False)}")
    print(f"\nLast  5 rows:\n{df.tail(5).to_string(index=False)}")

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_ts   = os.path.join(OUTPUT_DIR, f"ANA_ERIs_Servicos_{ts}.csv")
    out_main = os.path.join(OUTPUT_DIR, "ANA_ERIs_Servicos.csv")

    df.to_csv(out_ts,   index=False, encoding="utf-8-sig")
    df.to_csv(out_main, index=False, encoding="utf-8-sig")
    print(f"\nSaved:\n  {out_ts}\n  {out_main}")

    try:
        from google.colab import files
        files.download(out_main)
    except Exception:
        print("(Download from Colab Files panel manually)")

    print("\nDone!")
