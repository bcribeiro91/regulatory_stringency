# scrap_ANA_Agencies.R  —  v3 (2026-03)
# Collects ALL rows from the ANA ERIs "Serviços" Power BI tab.
#
# Root cause of missing rows in previous versions:
#   The Power BI table uses virtual scrolling — only ~10-15 rows are rendered
#   at a time.  Scrolling by PageDown can skip rows, and duplicate detection
#   based on the first visible cell is unreliable.
#
# This script uses two strategies (in order of preference):
#
#   APPROACH A (primary): Intercept Power BI's own querydata API calls,
#     strip the row-count limit (Top clause), and replay the request directly
#     from R using httr.  This bypasses all DOM virtualization and retrieves
#     every row in a single HTTP round-trip.  All captured querydata requests
#     are tried; the one returning the most rows is kept.
#
#   APPROACH B (fallback): Navigate to the Serviços tab and scroll the table
#     row-by-row using ArrowDown keypresses.  Every visible row is deduplicated
#     by its full content signature (paste of all cells), so duplicates from
#     the virtualised DOM are silently discarded.  After 20 consecutive
#     scroll steps with no new rows the script jumps to the bottom (Ctrl+End)
#     to capture any tail rows that may have been skipped, then stops.
#
# Expected output columns (Serviços tab):
#   Cod_IBGE | UF | Municipio | CNPJ_ERI | Nome_ERI | Sigla_ERI |
#   Abrangencia_ERI | Data_inicio  [+ Servico / Atividade / Atribuicao if present]
#
# Requirements: chromote, httr, jsonlite, dplyr, stringr

library(chromote)
library(httr)
library(jsonlite)
library(stringr)
library(dplyr)

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

PBI_URL_SERVICOS <- paste0(
  "https://app.powerbi.com/view?r=",
  "eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUw",
  "YmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9",
  "&pageName=ReportSectione2a118ad60b5488ab805"
)

OUTPUT_DIR <- getwd()

# ── COLUMN NAME MAPPING ───────────────────────────────────────────────────────

COL_8  <- c("Cod_IBGE", "UF", "Municipio", "CNPJ_ERI",
            "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI", "Data_inicio")
COL_9  <- c(COL_8, "Servico")
COL_10 <- c(COL_8, "Servico", "Atividade")
COL_11 <- c(COL_8, "Servico", "Atividade", "Atribuicao")

# ── SHARED HELPERS ────────────────────────────────────────────────────────────

# Recursively remove Top / Count row-limit fields from a PBI query body
strip_row_limit <- function(obj) {
  if (!is.list(obj)) return(obj)
  obj[["Top"]]   <- NULL
  obj[["Count"]] <- NULL
  lapply(obj, strip_row_limit)
}

# Decode a single Power BI cell value (resolve value-dictionary references)
decode_cell <- function(val, col_idx, value_dicts) {
  if (is.null(val)) return(NA_character_)
  # Dictionary encoding: integer index → look up string
  if (is.numeric(val) &&
      !is.null(value_dicts) &&
      col_idx <= length(value_dicts) &&
      !is.null(value_dicts[[col_idx]]) &&
      length(value_dicts[[col_idx]]) > 0) {
    dict <- value_dicts[[col_idx]]
    idx  <- as.integer(val) + 1L      # PBI indices are 0-based
    if (idx >= 1L && idx <= length(dict)) return(as.character(dict[[idx]]))
  }
  as.character(val)
}

# Parse a Power BI DSR (Data Shape Result) JSON response into a data.frame.
# Handles: column schema (SH), value dictionaries, repeat bitmasks (R).
# Returns the data.frame with the most rows found across all result entries,
# or NULL if nothing could be parsed.
parse_dsr <- function(resp_json) {
  results <- resp_json$results
  if (is.null(results) || length(results) == 0) {
    cat("    Response contains no 'results'\n")
    return(NULL)
  }

  best_df <- NULL
  best_n  <- 0L

  for (r in results) {
    dsr <- r$result$data$dsr
    if (is.null(dsr) || is.null(dsr$DS) || length(dsr$DS) == 0) next

    for (ds in dsr$DS) {

      # --- Column names from SH (Shape Header) ---
      col_names   <- NULL
      value_dicts <- list()

      if (!is.null(ds$SH) && length(ds$SH) > 0) {
        for (sh in ds$SH) {
          if (!is.null(sh$DM0) && length(sh$DM0) > 0) {
            col_names <- sapply(sh$DM0, function(dm) {
              if (!is.null(dm$N)) return(dm$N)
              if (!is.null(dm$S)) return(dm$S)
              NA_character_
            })
          }
        }
      }

      # --- Value dictionaries (dictionary-encoded columns) ---
      if (!is.null(ds$ValueDicts)) value_dicts <- ds$ValueDicts

      # --- Row data from PH (Page Header) ---
      raw_rows <- list()
      if (!is.null(ds$PH) && length(ds$PH) > 0) {
        for (ph in ds$PH) {
          if (!is.null(ph$DM0) && length(ph$DM0) > 0) {
            raw_rows <- c(raw_rows, ph$DM0)
          }
        }
      }
      if (length(raw_rows) == 0) next

      # Infer column count
      n_cols <- if (!is.null(col_names) && !all(is.na(col_names))) {
        length(col_names)
      } else {
        max(sapply(raw_rows, function(x) length(x$C)), na.rm = TRUE)
      }
      if (n_cols == 0) next

      cat(sprintf("    DS block: %d raw rows, %d columns\n",
                  length(raw_rows), n_cols))
      if (!is.null(col_names))
        cat("    Schema:", paste(col_names, collapse = ", "), "\n")

      # --- Decode rows ---
      # Power BI uses a bitmask R where bit (j-1) being set means column j
      # repeats its value from the previous row (delta encoding).
      prev_row     <- rep(NA_character_, n_cols)
      decoded_rows <- vector("list", length(raw_rows))

      for (i in seq_along(raw_rows)) {
        row_data <- raw_rows[[i]]
        cells    <- row_data$C
        R_flag   <- row_data$R   # May be NULL, NA, or integer bitmask

        current_row <- rep(NA_character_, n_cols)

        R_int <- if (!is.null(R_flag) && length(R_flag) == 1L) {
          tryCatch(as.integer(R_flag),
                   warning = function(w) 0L,
                   error   = function(e) 0L)
        } else {
          0L
        }

        if (!is.na(R_int) && R_int != 0L) {
          # Repeat-flag decoding: consume cells only for non-repeating columns
          cell_idx <- 1L
          for (j in seq_len(n_cols)) {
            # Bit (j-1): set → column j repeats from previous row
            if (bitwAnd(R_int, bitwShiftL(1L, j - 1L)) > 0L) {
              current_row[j] <- prev_row[j]
            } else {
              if (cell_idx <= length(cells)) {
                current_row[j] <- decode_cell(cells[[cell_idx]], j, value_dicts)
                cell_idx <- cell_idx + 1L
              }
            }
          }
        } else {
          # No repeat flag: each cell position maps directly to a column
          for (j in seq_along(cells)) {
            if (j <= n_cols)
              current_row[j] <- decode_cell(cells[[j]], j, value_dicts)
          }
        }

        decoded_rows[[i]] <- current_row
        prev_row <- current_row
      }

      mat <- do.call(rbind, decoded_rows)
      if (is.null(mat) || nrow(mat) == 0) next

      df_ds <- as.data.frame(mat, stringsAsFactors = FALSE)

      # Apply schema column names if they match
      if (!is.null(col_names) && length(col_names) == ncol(df_ds))
        colnames(df_ds) <- col_names

      if (nrow(df_ds) > best_n) {
        best_df <- df_ds
        best_n  <- nrow(df_ds)
      }
    }
  }

  best_df
}

# Fix UTF-8 mojibake for Portuguese characters
fix_pt <- function(x) {
  if (is.na(x) || !nchar(x)) return(x)
  tbl <- c(
    "Ã¡"="á",  "Ã©"="é",  "Ã­"="í",  "Ã³"="ó",  "Ãº"="ú",
    "Ã£"="ã",  "Ãµ"="õ",  "Ã§"="ç",  "Ã¢"="â",  "Ãª"="ê",  "Ã´"="ô",
    "Ã\u0081"="Á", "Ã‰"="É", "Ã\u008d"="Í",
    "Ã\u0093"="Ó", "Ãš"="Ú", "Ãƒ"="Ã",
    "Ã•"="Õ",  "Ã‡"="Ç",  "Ã‚"="Â",  "ÃŠ"="Ê",  "Ã\u0094"="Ô"
  )
  for (i in seq_along(tbl)) x <- gsub(names(tbl)[i], tbl[i], x, fixed = TRUE)
  x
}

# Assign standard column names based on column count, then clean values
clean_df <- function(df) {
  # Fix encoding and trim
  for (j in seq_len(ncol(df))) {
    if (is.character(df[[j]])) {
      df[[j]] <- sapply(df[[j]], fix_pt, USE.NAMES = FALSE)
      df[[j]] <- str_trim(df[[j]])
      df[[j]] <- ifelse(df[[j]] %in% c("", "########"), NA_character_, df[[j]])
    }
  }

  # Assign column names by count
  nc <- ncol(df)
  if      (nc ==  8) colnames(df) <- COL_8
  else if (nc ==  9) colnames(df) <- COL_9
  else if (nc == 10) colnames(df) <- COL_10
  else if (nc == 11) colnames(df) <- COL_11
  else colnames(df) <- paste0("Col", seq_len(nc))

  # CNPJ → 14-digit zero-padded string
  cnpj_col <- grep("CNPJ", names(df), ignore.case = TRUE, value = TRUE)
  if (length(cnpj_col) > 0) {
    df[[cnpj_col[1]]] <- sapply(df[[cnpj_col[1]]], function(x) {
      if (is.na(x)) return(NA_character_)
      x <- gsub("[^0-9]", "", as.character(x))
      if (!nchar(x)) return(NA_character_)
      str_pad(x, 14L, "left", "0")
    }, USE.NAMES = FALSE)
  }

  # Cod_IBGE → 7-digit zero-padded string
  ibge_col <- grep("Cod_IBGE|IBGE", names(df), ignore.case = TRUE, value = TRUE)
  if (length(ibge_col) > 0) {
    df[[ibge_col[1]]] <- sapply(df[[ibge_col[1]]], function(x) {
      if (is.na(x)) return(NA_character_)
      x <- gsub("[^0-9]", "", as.character(x))
      if (!nchar(x)) return(NA_character_)
      str_pad(x, 7L, "left", "0")
    }, USE.NAMES = FALSE)
  }

  # Deduplicate and drop all-NA rows
  n_before <- nrow(df)
  df <- distinct(df)
  df <- df[rowSums(is.na(df)) < ncol(df), , drop = FALSE]
  cat(sprintf("  Rows: %d → %d after dedup/NA-drop\n", n_before, nrow(df)))
  df
}


# ══════════════════════════════════════════════════════════════════════════════
#  APPROACH A: API INTERCEPTION
# ══════════════════════════════════════════════════════════════════════════════

cat("═══════════════════════════════════════════════════════════════\n")
cat("  ANA ERIs — Serviços tab  |  API interception approach\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

# Close any leftover browser session from a previous run
if (exists("b") && inherits(b, "ChromoteSession")) {
  tryCatch(b$close(), error = function(e) NULL)
  rm(b)
  Sys.sleep(1L)
}

cat("Step 1: Launching headless browser and enabling network monitoring...\n")
b <- ChromoteSession$new()
b$Network$enable()

# Collect every querydata POST request Power BI makes
request_bodies <- list()

b$Network$requestWillBeSent(callback = function(params) {
  url <- params$request$url
  if (grepl("querydata", url, ignore.case = TRUE) &&
      !is.null(params$request$postData)) {
    req_id <- params$requestId
    request_bodies[[req_id]] <<- list(
      url     = url,
      headers = params$request$headers,
      body    = params$request$postData
    )
  }
})

cat("Step 2: Navigating to Serviços tab — waiting 30 s for Power BI to load...\n")
b$Page$navigate(PBI_URL_SERVICOS)
Sys.sleep(30)

n_captured <- length(request_bodies)
cat(sprintf("Step 3: %d querydata request(s) captured\n\n", n_captured))

# ── Try every captured request; keep the one with the most rows ───────────────
df_api <- NULL

for (req_id in names(request_bodies)) {
  req <- request_bodies[[req_id]]

  body_parsed <- tryCatch(
    fromJSON(req$body, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.null(body_parsed)) next

  cat(sprintf("  Request %s → %s\n",
              substr(req_id, 1L, 8L), substr(req$url, 1L, 70L)))

  # Remove all row-limit clauses so the backend returns everything
  modified_body <- strip_row_limit(body_parsed)

  # Build minimal headers, copying PBI authentication headers from the capture
  hdrs <- c("Content-Type" = "application/json",
             "Accept"       = "application/json")
  for (h in c("X-PowerBI-ResourceKey", "X-PowerBI-EntitledToken",
               "ActivityId", "RequestId", "Authorization")) {
    val <- req$headers[[h]]
    if (!is.null(val)) hdrs[h] <- val
  }

  resp <- tryCatch(
    POST(
      url    = req$url,
      body   = toJSON(modified_body, auto_unbox = TRUE),
      encode = "raw",
      add_headers(.headers = hdrs),
      content_type_json()
    ),
    error = function(e) {
      cat("    POST error:", conditionMessage(e), "\n")
      NULL
    }
  )

  if (is.null(resp) || status_code(resp) != 200L) {
    cat(sprintf("    HTTP %s — skipping\n",
                if (is.null(resp)) "error" else status_code(resp)))
    next
  }

  cat("    HTTP 200 — parsing DSR response...\n")
  resp_json   <- content(resp, as = "parsed", simplifyVector = FALSE)
  df_attempt  <- parse_dsr(resp_json)

  if (!is.null(df_attempt) && nrow(df_attempt) > 0) {
    cat(sprintf("    Result: %d rows × %d cols\n",
                nrow(df_attempt), ncol(df_attempt)))
    if (nrow(df_attempt) > (if (is.null(df_api)) 0L else nrow(df_api))) {
      df_api <- df_attempt
    }
  } else {
    cat("    Parsed 0 rows\n")
  }
}


# ══════════════════════════════════════════════════════════════════════════════
#  APPROACH B: SCROLL-BASED FALLBACK
#  Used only if Approach A produced no data.
# ══════════════════════════════════════════════════════════════════════════════

if (is.null(df_api) || nrow(df_api) == 0) {

  cat("\n═══════════════════════════════════════════════════════════════\n")
  cat("  Fallback: scroll-based extraction (row-by-row dedup)\n")
  cat("═══════════════════════════════════════════════════════════════\n\n")

  b$Page$navigate(PBI_URL_SERVICOS)
  Sys.sleep(20L)

  # ── Read all visible gridcell rows ──────────────────────────────────────────
  get_visible_rows <- function() {
    res <- b$Runtime$evaluate(expression = '
      (function() {
        const cells = [];
        document.querySelectorAll("[role=gridcell]").forEach(el => {
          const r = el.getBoundingClientRect();
          if (r.width > 0 && r.height > 0) {
            const t = (el.getAttribute("title") || el.innerText || "").trim();
            if (t && t !== "Select Row" && t !== "Row Selection") {
              cells.push({ text: t, top: Math.round(r.top), left: Math.round(r.left) });
            }
          }
        });
        if (!cells.length) return "[]";
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
        return JSON.stringify(rows.filter(r => r.length >= 7 && r.length <= 12));
      })()
    ')
    if (is.null(res$result$value) || res$result$value == "[]") return(list())
    parsed <- tryCatch(fromJSON(res$result$value), error = function(e) list())
    if (length(parsed) == 0) return(list())
    if (is.matrix(parsed))
      return(lapply(seq_len(nrow(parsed)), function(i) as.character(parsed[i, ])))
    if (is.list(parsed)) return(lapply(parsed, as.character))
    list(as.character(parsed))
  }

  # ── Click first gridcell to give the table keyboard focus ──────────────────
  click_table <- function() {
    res <- b$Runtime$evaluate(expression = '
      (function() {
        const el = document.querySelector("[role=gridcell]");
        if (!el) return null;
        const r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0)
          return JSON.stringify({x: Math.round((r.left+r.right)/2),
                                 y: Math.round((r.top+r.bottom)/2)});
        return null;
      })()
    ')
    if (!is.null(res$result$value) && res$result$value != "null") {
      coords <- fromJSON(res$result$value)
      b$Input$dispatchMouseEvent(type="mousePressed", x=coords$x, y=coords$y,
                                 button="left", clickCount=1L)
      b$Input$dispatchMouseEvent(type="mouseReleased", x=coords$x, y=coords$y,
                                 button="left", clickCount=1L)
      return(TRUE)
    }
    FALSE
  }

  # ── Send n ArrowDown keypresses (row-by-row, avoids skipping rows) ──────────
  arrow_down <- function(n = 3L) {
    for (i in seq_len(n)) {
      b$Input$dispatchKeyEvent(type="rawKeyDown", windowsVirtualKeyCode=40L,
                               key="ArrowDown")
      b$Input$dispatchKeyEvent(type="keyUp",      windowsVirtualKeyCode=40L,
                               key="ArrowDown")
      Sys.sleep(0.05)
    }
    Sys.sleep(0.3)
  }

  # ── Ctrl+Home / Ctrl+End ───────────────────────────────────────────────────
  ctrl_key <- function(vk, key) {
    b$Input$dispatchKeyEvent(type="rawKeyDown", windowsVirtualKeyCode=vk,
                             key=key, modifiers=2L)
    b$Input$dispatchKeyEvent(type="keyUp",      windowsVirtualKeyCode=vk,
                             key=key, modifiers=2L)
  }

  # ── Focus table and jump to top ────────────────────────────────────────────
  click_table(); Sys.sleep(1L)
  ctrl_key(36L, "Home")    # Ctrl+Home
  Sys.sleep(3L)
  click_table(); Sys.sleep(1L)

  seen_sigs <- character(0)   # Full-row content signatures for dedup
  all_rows  <- list()
  stale     <- 0L             # Consecutive scroll steps with no new rows
  iter      <- 1L
  max_iter  <- 8000L

  cat("Scrolling row-by-row and collecting unique rows...\n")

  while (iter <= max_iter) {
    visible <- get_visible_rows()
    added   <- 0L

    for (row in visible) {
      sig <- paste(row, collapse = "|")
      if (!(sig %in% seen_sigs)) {
        seen_sigs <- c(seen_sigs, sig)
        all_rows  <- c(all_rows, list(row))
        added     <- added + 1L
      }
    }

    if (added > 0L) {
      stale <- 0L
      if (iter %% 100L == 0L || added > 5L)
        cat(sprintf("  Iter %5d | +%3d new | Total: %5d\n",
                    iter, added, length(all_rows)))
    } else {
      stale <- stale + 1L
    }

    # After 20 stale steps: jump to the absolute bottom to capture any
    # tail rows that may not have been reached by incremental scrolling.
    if (stale == 20L) {
      cat("  20 stale steps — jumping to bottom (Ctrl+End)...\n")
      ctrl_key(35L, "End")   # Ctrl+End
      Sys.sleep(3L)
      click_table(); Sys.sleep(1L)

      bottom_visible <- get_visible_rows()
      tail_added <- 0L
      for (row in bottom_visible) {
        sig <- paste(row, collapse = "|")
        if (!(sig %in% seen_sigs)) {
          seen_sigs  <- c(seen_sigs, sig)
          all_rows   <- c(all_rows, list(row))
          tail_added <- tail_added + 1L
        }
      }
      cat(sprintf("  +%d tail rows from bottom\n", tail_added))
      cat("  Stopping — no further new rows.\n")
      break
    }

    click_table()
    arrow_down(3L)
    iter <- iter + 1L
  }

  cat(sprintf("\nScroll collection done: %d unique rows\n", length(all_rows)))

  if (length(all_rows) == 0)
    stop("No data collected from either Approach A or Approach B.")

  col_counts  <- sapply(all_rows, length)
  target_cols <- as.numeric(names(sort(table(col_counts), decreasing = TRUE))[1])
  valid_rows  <- all_rows[col_counts == target_cols]
  df_api      <- as.data.frame(do.call(rbind, valid_rows), stringsAsFactors = FALSE)
}


# ══════════════════════════════════════════════════════════════════════════════
#  POST-PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

cat("\n═══════════════════════════════════════════════════════════════\n")
cat("  Post-processing\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

cat(sprintf("Raw data: %d rows × %d cols\n", nrow(df_api), ncol(df_api)))
df <- clean_df(df_api)


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

cat("\n═══════════════════════════════════════════════════════════════\n")
cat("  Validation\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

cat(sprintf("Final dataset: %d rows × %d cols\n", nrow(df), ncol(df)))
cat("Columns:", paste(names(df), collapse = " | "), "\n")

if ("UF" %in% names(df)) {
  uf_tab <- sort(table(df$UF), decreasing = TRUE)
  cat(sprintf("\nStates (%d):\n", length(uf_tab)))
  print(uf_tab)
}
if ("Municipio"  %in% names(df)) cat("Unique municipalities:", n_distinct(df$Municipio),  "\n")
if ("Sigla_ERI"  %in% names(df)) cat("Unique ERIs (sigla):  ", n_distinct(df$Sigla_ERI),  "\n")
if ("CNPJ_ERI"   %in% names(df)) cat("Unique ERIs (CNPJ):   ", n_distinct(df$CNPJ_ERI),   "\n")

cat("\nFirst 10 rows:\n");  print(head(df, 10L))
cat("\nLast  5 rows:\n");   print(tail(df,  5L))


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════

ts       <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_ts   <- file.path(OUTPUT_DIR, paste0("ANA_ERIs_Servicos_", ts, ".csv"))
out_main <- file.path(OUTPUT_DIR, "ANA_ERIs_Servicos.csv")

write.csv(df, out_ts,   row.names = FALSE, fileEncoding = "UTF-8")
write.csv(df, out_main, row.names = FALSE, fileEncoding = "UTF-8")

cat(sprintf("\nSaved:\n  %s\n  %s\n", out_ts, out_main))


# ══════════════════════════════════════════════════════════════════════════════
#  CLOSE BROWSER
# ══════════════════════════════════════════════════════════════════════════════

tryCatch(b$close(), error = function(e) NULL)
cat("\nDone!\n")
