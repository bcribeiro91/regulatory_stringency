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
import re, os
from collections import Counter
from datetime import datetime

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

print("Imports OK")


# ══════════════════════════════════════════════════════════════════════════════
# CELL 3 — Helpers
# ══════════════════════════════════════════════════════════════════════════════

# Maps Power BI DOM column headers (Portuguese) → output column names (English)
_DOM_COL_MAP = {
    "Cód. IBGE":                    "Cod_IBGE",
    "UF":                           "UF",
    "Municipio":                    "Municipio",
    "CNPJ ERI":                     "CNPJ_ERI",
    "Nome ERI":                     "Nome_ERI",
    "Sigla ERI":                    "Sigla_ERI",
    "Abrangência ERI":              "Abrangencia_ERI",
    "Data do início da delegação":  "Data_inicio",
    "Data do fim da delegação":     "Data_validade",
    "Setorialidade ERI":            "Setorialidade",
}

# Power BI UI navigation tokens that appear in tab/menu rows, not data rows
_UI_SET = {
    "Panorama", "Serviços", "Municípios sem regulação de serviço",
    "Row Selection", "Select Row",
}


def _is_data_row(row):
    """
    True for real data rows.
    A row is a UI artefact only when ALL its cells are UI navigation tokens.
    (Every data row starts with 'Select Row' in col-0, so using `any` would
     reject all data rows — must use `all`.)
    """
    return not all(cell in _UI_SET for cell in row)


def _clean_date(val):
    """Normalise Power BI date strings → DD/MM/YYYY."""
    if not val or str(val).strip() in ("", "None", "NaN"):
        return None
    s = str(val).strip()
    # Already DD/MM/YYYY
    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        return s
    # US format M/D/YYYY [H:MM:SS AM/PM]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        mo = m.group(1).zfill(2)
        dy = m.group(2).zfill(2)
        yr = m.group(3)
        return f"{dy}/{mo}/{yr}"
    return s


def clean_dataframe(df):
    """
    Rename Portuguese DOM column headers to English, clean CNPJ / Cod_IBGE,
    normalise dates, and deduplicate.
    """
    # Safety: if columns are integers (header matching failed in run_scraper),
    # apply the positional fallback mapping so downstream string ops don't crash.
    if all(isinstance(c, (int, float)) for c in df.columns):
        nc = len(df.columns)
        _POS = {
            10: ["Row_Selection","Cod_IBGE","UF","Municipio","CNPJ_ERI",
                 "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Setorialidade"],
            11: ["Row_Selection","Cod_IBGE","UF","Municipio","CNPJ_ERI",
                 "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio",
                 "Data_validade","Setorialidade"],
        }
        df.columns = _POS.get(nc, [f"Col{i}" for i in range(nc)])
        print(f"  Applied positional column names ({nc} cols)")

    # Drop Power BI row-selection UI column if present
    for col in list(df.columns):
        if col in ("Row Selection", "Select Row", "Row_Selection"):
            df = df.drop(columns=[col])

    # Rename Portuguese headers → English
    df = df.rename(columns={k: v for k, v in _DOM_COL_MAP.items() if k in df.columns})

    df = df.replace({"": None, "########": None, "None": None})

    # Normalise date columns
    for col in ["Data_inicio", "Data_validade"]:
        if col in df.columns:
            df[col] = df[col].apply(_clean_date)

    # Clean CNPJ → 14 digits, zero-padded
    for col in [c for c in df.columns if isinstance(c, str) and "CNPJ" in c.upper()]:
        df[col] = df[col].apply(
            lambda x: None
            if (x is None or str(x).strip() == "" or
                (isinstance(x, float) and pd.isna(x)))
            else re.sub(r"[^0-9]", "", str(x)).zfill(14)[:14] or None
        )

    # Clean Cod_IBGE → 7 digits, zero-padded
    if "Cod_IBGE" in df.columns:
        df["Cod_IBGE"] = df["Cod_IBGE"].apply(
            lambda x: None
            if (x is None or str(x).strip() == "" or
                (isinstance(x, float) and pd.isna(x)))
            else re.sub(r"[^0-9]", "", str(x)).zfill(7) or None
        )

    n_before = len(df)
    df = df.drop_duplicates().dropna(how="all").reset_index(drop=True)
    print(f"  {n_before} → {len(df)} rows after dedup/drop-NA")
    return df


print("Helpers defined OK")


# ══════════════════════════════════════════════════════════════════════════════
# CELL 4 — Async DOM scraper  (JS scrollTop + mouse-wheel, iframe-aware)
# ══════════════════════════════════════════════════════════════════════════════

# ── JavaScript snippets ───────────────────────────────────────────────────────

# Collect all visible gridcell text; group cells into rows by shared top-coord.
_ROWS_JS = """() => {
    const cells = [];
    document.querySelectorAll('[role=gridcell]').forEach(el => {
        const r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0) {
            const t = (el.getAttribute('title') || el.innerText || '').trim();
            cells.push({ text: t, top: Math.round(r.top), left: Math.round(r.left) });
        }
    });
    if (!cells.length) return [];
    cells.sort((a, b) => a.top - b.top);
    const rows = []; let cur = [cells[0]], curTop = cells[0].top;
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
    return rows;
}"""

# PRIMARY scroll: JS scrollTop on PBI table container, or the page element
# with the largest scrollable height (sidebar / report wrapper).
# Confirmed in earlier sessions: scrolling the sidebar fallback (sh≈25801)
# triggers Power BI's virtual table to load new rows, even though the element
# scrolled is not the table itself — PBI listens for page-level scroll events.
_SCROLL_JS = """(delta) => {
    const selectors = [
        '.tableEx-scroll-wrapper',
        '.scroll-wrapper',
        'div[class*="scroll"][role="presentation"]',
        '[data-testid="visual-scroll-wrapper"]',
        '[role="grid"]',
        '[role="rowgroup"]',
    ];
    for (const s of selectors) {
        const el = document.querySelector(s);
        if (el && el.scrollHeight > el.clientHeight) {
            el.scrollTop += delta;
            return 'scrollTop(' + s + '): ' + el.scrollTop;
        }
    }
    // Fallback: element with maximum scrollable height (page sidebar / wrapper).
    let best = null, bestH = 0;
    document.querySelectorAll('*').forEach(el => {
        const sh = el.scrollHeight - el.clientHeight;
        if (sh > bestH) { bestH = sh; best = el; }
    });
    if (best) {
        best.scrollTop += delta;
        return 'scrollTop fallback (sh=' + bestH + '): ' + best.scrollTop;
    }
    return 'no scrollable element';
}"""

# SECONDARY scroll: WheelEvent dispatched inside the frame JS context.
_WHEEL_JS = """(delta) => {
    const targets = [
        document.querySelector('[role=rowgroup]'),
        document.querySelector('[role=grid]'),
        document.querySelector('[role=row]'),
        document.body,
    ];
    for (const el of targets) {
        if (!el) continue;
        el.dispatchEvent(new WheelEvent('wheel', {
            deltaY: delta, deltaMode: 0, bubbles: true, cancelable: true, view: window
        }));
        return 'WheelEvent → [' + (el.getAttribute('role') || el.tagName) + ']';
    }
    return 'no target';
}"""

# Collect column header text from the table.
_HEADER_JS = """() => Array.from(
    document.querySelectorAll('[role=columnheader]'),
    el => (el.getAttribute('title') || el.innerText || '').trim()
).filter(t => t)"""


# ── Helper coroutines ─────────────────────────────────────────────────────────

async def _get_table_frame(page):
    """
    Find the frame (iframe or main page) that contains the Power BI table.

    Returns (frame, iframe_page_x, iframe_page_y).

    IMPORTANT: Playwright's frame.locator().bounding_box() returns coordinates
    in the *frame's own* coordinate space — NOT page coordinates.
    To convert frame coords → page coords we must add the iframe's page offset.
    """
    # 1. Check main page first (no iframe overhead)
    try:
        if await page.locator('[role=gridcell]').count() > 0:
            print("  Table is in the main page (no iframe offset).")
            return page, 0.0, 0.0
    except Exception:
        pass

    # 2. Iterate through all iframes
    for iframe_elem in await page.locator('iframe').all():
        try:
            frame = await iframe_elem.content_frame()
            if frame is None:
                continue
            if await frame.locator('[role=gridcell]').count() > 3:
                bb = await iframe_elem.bounding_box()
                ix = bb['x'] if bb else 0.0
                iy = bb['y'] if bb else 0.0
                print(f"  Table iframe at page ({ix:.0f}, {iy:.0f})  "
                      f"url={frame.url[:70]}")
                return frame, ix, iy
        except Exception:
            continue

    print("  WARNING: no iframe with gridcells found — using main page.")
    return page, 0.0, 0.0


async def _find_scroll_coords(tf, iframe_x, iframe_y):
    """
    Return page-level (x, y) for mouse wheel events targeting the table BODY.

    Tries element selectors in priority order, skipping any element whose
    frame-relative y < min_y (the header row sits near y=0 in the frame).
    Falls back to iframe_x+700, iframe_y+450 if nothing is found.
    """
    candidates = [
        ('[role=rowgroup]',  0),   # the tbody container (starts below header)
        ('[role=row]',      80),   # data rows (skip header which is at y~40-70)
        ('[role=gridcell]', 80),   # individual data cells
    ]
    for sel, min_y in candidates:
        try:
            loc = tf.locator(sel)
            n   = await loc.count()
            for idx in range(min(15, n)):
                try:
                    bb = await loc.nth(idx).bounding_box()
                    if bb and bb['y'] >= min_y and bb['width'] > 80:
                        px = iframe_x + bb['x'] + bb['width']  / 2
                        py = iframe_y + bb['y'] + bb['height'] / 2
                        print(f"  Scroll target: {sel}[{idx}] "
                              f"frame({bb['x']:.0f}, {bb['y']:.0f}) → "
                              f"page({px:.0f}, {py:.0f})")
                        return px, py
                except Exception:
                    continue
        except Exception:
            continue

    # Hard fallback
    px, py = iframe_x + 700.0, iframe_y + 450.0
    print(f"  Scroll target: hard fallback → page({px:.0f}, {py:.0f})")
    return px, py


# ── Main scraper ──────────────────────────────────────────────────────────────

async def run_scraper():
    """
    DOM-based extraction with three layered scroll strategies:

      1. JS scrollTop (_SCROLL_JS) — PRIMARY.
         Scrolls the Power BI table container if found, otherwise falls back to
         the page element with the largest scrollable height (typically the
         sidebar/report wrapper).  In earlier sessions this fallback was the
         *only* method that caused the virtual table to load new rows — Power BI
         appears to refresh its virtual table in response to page-level scroll
         events, even when the scrolled element is not the table itself.

      2. WheelEvent via JS (_WHEEL_JS) — SECONDARY.
         Dispatched on [role=rowgroup] / [role=grid] inside the frame.

      3. Real OS mouse wheel (page.mouse.wheel) — TERTIARY.
         Sent at the correct page-level coordinates (iframe offset + frame bbox).
    """
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

        print("=" * 65)
        print("  ANA ERIs — Serviços tab  (DOM extraction)")
        print("=" * 65)
        print("Loading report (45 s)…")
        await page.goto(PBI_URL, timeout=90_000)
        await page.wait_for_timeout(45_000)

        # ── Locate table frame & iframe page offset ───────────────────────────
        print("\nLocating table frame…")
        tf, iframe_x, iframe_y = await _get_table_frame(page)

        # ── Column headers ────────────────────────────────────────────────────
        headers = await tf.evaluate(_HEADER_JS)
        print(f"Headers ({len(headers)}): {headers}")

        # ── Find scroll target (table BODY, not the header row) ───────────────
        print("Finding scroll target in table body…")
        scroll_x, scroll_y = await _find_scroll_coords(tf, iframe_x, iframe_y)

        # ── Click to give the table keyboard/wheel focus ──────────────────────
        try:
            await page.mouse.move(scroll_x, scroll_y)
            await page.mouse.click(scroll_x, scroll_y)
            print(f"  Clicked at page ({scroll_x:.0f}, {scroll_y:.0f})")
            await page.wait_for_timeout(1_500)
        except Exception as e:
            print(f"  Click warning: {e}")

        # ── Scroll + collect loop ─────────────────────────────────────────────
        seen: set  = set()
        all_rows   = []
        stale      = 0
        MAX_STALE  = 80   # ~20 s gap is safe; prior run had a 55-iter silent gap

        for it in range(600):
            # Read currently rendered gridcell rows from the DOM
            visible = await tf.evaluate(_ROWS_JS)
            added = 0
            for row in visible:
                key = "|".join(row)
                if key not in seen:
                    seen.add(key)
                    all_rows.append(row)
                    added += 1

            if added:
                stale = 0
                if it < 20 or it % 50 == 0 or added > 3:
                    print(f"  it {it:4d} | +{added:3d} | total {len(all_rows)}")
            else:
                stale += 1
                if stale >= MAX_STALE:
                    print(f"\n  {MAX_STALE} consecutive stale — done. "
                          f"Total rows: {len(all_rows)}")
                    break

            # 1. JS scrollTop — primary (works via PBI's page-scroll listener)
            scroll_msg = await tf.evaluate(_SCROLL_JS, 300)
            if it < 5 or (stale > 0 and stale % 20 == 0):
                print(f"  scroll[{it}]: {scroll_msg}")

            # 2. WheelEvent via JS — secondary
            await tf.evaluate(_WHEEL_JS, 300)

            # 3. Real mouse wheel at correct page coords — tertiary
            await page.mouse.move(scroll_x, scroll_y)
            await page.mouse.wheel(0, 300)

            await page.wait_for_timeout(250)

        await browser.close()

    # ── Filter and build DataFrame ────────────────────────────────────────────
    print(f"\nRaw rows collected (all): {len(all_rows)}")

    if not all_rows:
        return None

    data_rows = [r for r in all_rows if _is_data_row(r)]
    print(f"After _is_data_row filter: {len(data_rows)}")

    if not data_rows:
        return None

    # Use the most common row length (handles any rows with odd cell counts)
    target_len = Counter(len(r) for r in data_rows).most_common(1)[0][0]
    data_rows  = [r for r in data_rows if len(r) == target_len]
    print(f"After length filter (len={target_len}): {len(data_rows)} rows")

    # ── Assign column names ───────────────────────────────────────────────────
    col_names = None
    if headers:
        if len(headers) == target_len:
            # Perfect match — use DOM headers directly
            col_names = headers
        elif len(headers) == target_len + 1:
            # One column is absent from most rows (typically 'Data do fim da
            # delegação' when the end-date cell renders as invisible/empty).
            # Drop it so the headers align with the shorter rows.
            for skip in ["Data do fim da delegação", "Data do início da delegação"]:
                trimmed = [h for h in headers if h != skip]
                if len(trimmed) == target_len:
                    col_names = trimmed
                    print(f"  Adjusted headers: dropped '{skip}' "
                          f"to match row length {target_len}")
                    break

    if col_names is None:
        # Positional fallback: well-known layouts by column count
        _FALLBACK = {
            10: ["Row_Selection","Cod_IBGE","UF","Municipio","CNPJ_ERI",
                 "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio","Setorialidade"],
            11: ["Row_Selection","Cod_IBGE","UF","Municipio","CNPJ_ERI",
                 "Nome_ERI","Sigla_ERI","Abrangencia_ERI","Data_inicio",
                 "Data_validade","Setorialidade"],
        }
        col_names = _FALLBACK.get(target_len)
        if col_names:
            print(f"  Applied positional fallback column names ({target_len} cols)")
        else:
            print(f"  WARNING: no column-name mapping for len={target_len}; "
                  f"using positional integers")

    if col_names:
        return pd.DataFrame(data_rows, columns=col_names)
    return pd.DataFrame(data_rows)


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
