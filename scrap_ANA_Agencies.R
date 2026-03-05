# scrap_ANA_Agencies.R
# Scrapes regulatory agency data from ANA's Power BI dashboard
# URL: https://app.powerbi.com/view?r=eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUwYmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9

library(chromote)
library(jsonlite)

# ── 1. CONNECT TO BROWSER ─────────────────────────────────────────────────────
PBI_URL <- "https://app.powerbi.com/view?r=eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUwYmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9"

if (!exists("b") || !inherits(b, "ChromoteSession")) {
  b <- ChromoteSession$new()
  b$Page$navigate(PBI_URL)
  cat("Aguardando Power BI carregar...\n")
  Sys.sleep(12)
} else {
  # Try to reuse existing session; respawn if closed
  tryCatch(
    b$Runtime$evaluate(expression = "1"),
    error = function(e) {
      message("Sessão fechada — reconectando...")
      b <<- b$respawn()
      b$Page$navigate(PBI_URL)
      Sys.sleep(12)
    }
  )
}

# ── 2. HELPER FUNCTIONS ───────────────────────────────────────────────────────

# Extract column headers from the table.
# Filters by proximity to data rows to avoid picking up navigation tab headers.
get_headers <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
      // Find the y-position of the first visible data row
      var cellTops = [];
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var r = el.getBoundingClientRect();
        var t = (el.innerText || "").trim();
        if (r.width > 0 && r.height > 0 && t !== "Select Row") cellTops.push(r.top);
      });
      if (cellTops.length === 0) return JSON.stringify([]);
      var firstDataY = Math.min.apply(null, cellTops);

      // Only keep columnheaders that sit just above the first data row (within 120px)
      var headers = [];
      document.querySelectorAll("[role=columnheader]").forEach(function(el) {
        var r = el.getBoundingClientRect();
        var t = (el.innerText || "").trim();
        if (r.width > 0 && r.height > 0 && t.length > 0 && t !== "Select Row" &&
            r.top < firstDataY && r.top > firstDataY - 120) {
          headers.push({text: t, left: r.left});
        }
      });
      headers.sort(function(a, b) { return a.left - b.left; });
      return JSON.stringify(headers.map(function(h) { return h.text; }));
    })()
  ')
  fromJSON(res$result$value)
}

# Click the first valid data cell to establish keyboard focus in the table
ensure_focus <- function(b) {
  b$Runtime$evaluate(expression = '
    (function() {
      var cells = document.querySelectorAll("[role=gridcell]");
      for (var i = 0; i < cells.length; i++) {
        var t = (cells[i].innerText || "").trim();
        if (t.length > 3 && t !== "Select Row") {
          cells[i].scrollIntoView({block: "nearest"});
          cells[i].focus();
          cells[i].click();
          return "ok: " + t.substring(0, 30);
        }
      }
      return "no cell found";
    })()
  ')
  Sys.sleep(0.4)
}

# Extract all visible rows as a character matrix (n_rows x n_cols).
# n_cols: expected number of data columns (from get_headers).
# Uses bounding-box positions to reconstruct row/column order robustly.
get_visible_rows <- function(b, n_cols) {
  js <- sprintf('
    (function() {
      var ncols = %d;
      var cells = [];
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var t = (el.innerText || "").trim();
        var rect = el.getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0 && t !== "Select Row") {
          cells.push({text: t, left: Math.round(rect.left), top: Math.round(rect.top)});
        }
      });
      if (cells.length === 0) return JSON.stringify([]);
      cells.sort(function(a, b) { return a.top - b.top || a.left - b.left; });
      var rows = [], cur = [cells[0]];
      for (var i = 1; i < cells.length; i++) {
        if (Math.abs(cells[i].top - cur[0].top) <= 5) {
          cur.push(cells[i]);
        } else {
          cur.sort(function(a, b) { return a.left - b.left; });
          rows.push(cur.map(function(c) { return c.text; }));
          cur = [cells[i]];
        }
      }
      cur.sort(function(a, b) { return a.left - b.left; });
      rows.push(cur.map(function(c) { return c.text; }));
      return JSON.stringify(rows.filter(function(r) { return r.length === ncols; }));
    })()
  ', n_cols)

  res    <- b$Runtime$evaluate(expression = js)
  parsed <- fromJSON(res$result$value)

  # fromJSON returns a character matrix when all rows are the same length;
  # otherwise it returns a list. Normalise to matrix either way.
  if (length(parsed) == 0) {
    return(matrix(character(0), nrow = 0, ncol = n_cols))
  }
  if (is.matrix(parsed)) {
    return(parsed)
  }
  do.call(rbind, lapply(parsed, function(r) matrix(r, nrow = 1)))
}

# Press ArrowDown n times to advance the virtual-scroll table
press_arrow_down <- function(b, n = 18) {
  for (i in seq_len(n)) {
    b$Input$dispatchKeyEvent(
      type = "rawKeyDown",
      windowsVirtualKeyCode = 40L, nativeVirtualKeyCode = 40L,
      key = "ArrowDown", code = "ArrowDown"
    )
    b$Input$dispatchKeyEvent(
      type = "keyUp",
      windowsVirtualKeyCode = 40L, nativeVirtualKeyCode = 40L,
      key = "ArrowDown", code = "ArrowDown"
    )
  }
  Sys.sleep(1)
}

# ── 3. DETECT COLUMN COUNT ───────────────────────────────────────────────────
# Retry up to 5x — Power BI may still be rendering
headers <- character(0)
for (.attempt in 1:5) {
  headers <- get_headers(b)
  if (length(headers) > 0) break
  cat("Tentativa", .attempt, "— aguardando headers...\n")
  Sys.sleep(3)
}

# Fallback: infer column count from the most common gridcell count per row
if (length(headers) == 0) {
  cat("Headers não detectados via [role=columnheader]. Inferindo via gridcells...\n")
  res_nc <- b$Runtime$evaluate(expression = '
    (function() {
      var tops = {};
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var t = (el.innerText || "").trim();
        var r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0 && t !== "Select Row") {
          var k = Math.round(r.top);
          tops[k] = (tops[k] || 0) + 1;
        }
      });
      var vals = Object.values(tops).sort(function(a,b){return b-a;});
      return vals.length > 0 ? String(vals[0]) : "0";
    })()
  ')
  n_cols  <- as.integer(res_nc$result$value)
  headers <- paste0("V", seq_len(n_cols))
  cat("Colunas inferidas:", n_cols, "\n")
} else {
  # Validate: infer n_cols from most common gridcell count per row
  res_nc <- b$Runtime$evaluate(expression = '
    (function() {
      var tops = {};
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var t = (el.innerText || "").trim();
        var r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0 && t !== "Select Row") {
          var k = Math.round(r.top);
          tops[k] = (tops[k] || 0) + 1;
        }
      });
      var vals = Object.values(tops).sort(function(a,b){return b-a;});
      return vals.length > 0 ? String(vals[0]) : "0";
    })()
  ')
  inferred_cols <- as.integer(res_nc$result$value)

  if (length(headers) != inferred_cols && inferred_cols > 0) {
    cat("Aviso: headers detectados (", length(headers), ") != colunas visíveis (", inferred_cols, "). Usando colunas visíveis.\n")
    n_cols  <- inferred_cols
    headers <- paste0("V", seq_len(n_cols))
  } else {
    n_cols <- length(headers)
  }
  cat("Colunas:", n_cols, "| Headers:", paste(headers, collapse = " | "), "\n")
}

if (n_cols == 0) stop("Não foi possível detectar colunas — verifique se a tabela está carregada no browser.")

# ── 4. SCROLL TO TOP AND SET INITIAL FOCUS ───────────────────────────────────
b$Runtime$evaluate(expression = '
  (function() {
    var grid = document.querySelector("[role=grid]") ||
               document.querySelector(".scrollRegion") ||
               document.querySelector(".bodyCells");
    if (grid) grid.scrollTop = 0;
    window.scrollTo(0, 0);
  })()
')
Sys.sleep(0.5)
ensure_focus(b)
Sys.sleep(1)

# ── 5. MAIN SCRAPING LOOP ────────────────────────────────────────────────────
all_chunks  <- list()
prev_sig    <- ""   # fingerprint of the last visible row from the previous page
stall_count <- 0
max_pages   <- 1000

for (page in seq_len(max_pages)) {

  mat <- get_visible_rows(b, n_cols = n_cols)

  if (nrow(mat) == 0) {
    cat("Sem linhas visíveis na página", page, "— encerrando.\n")
    break
  }

  # Fingerprint = all cells of the last visible row concatenated
  cur_sig <- paste(mat[nrow(mat), ], collapse = "|")

  cat(sprintf("Pág %4d | %2d linhas | Col1 início: %-22s | Col1 fim: %-22s\n",
              page, nrow(mat),
              substr(mat[1,      1], 1, 22),
              substr(mat[nrow(mat), 1], 1, 22)))

  all_chunks <- c(all_chunks, list(mat))

  # End-of-table detection: last row unchanged for 2 consecutive pages
  if (cur_sig == prev_sig) {
    stall_count <- stall_count + 1
    if (stall_count >= 2) {
      cat("Fim da tabela confirmado na página", page, "\n")
      break
    }
  } else {
    stall_count <- 0
  }
  prev_sig <- cur_sig

  # Re-focus before every scroll to prevent ArrowDown from firing into void
  ensure_focus(b)
  press_arrow_down(b, n = 18)   # 18-row advance; 2-row overlap handled by unique()
}

# ── 6. CONSOLIDATE ───────────────────────────────────────────────────────────
if (length(all_chunks) == 0) {
  stop("Nenhum dado coletado. Chunks esperados com ", n_cols, " colunas. ",
       "Verifique: (1) tabela visível no browser, (2) foco estabelecido na célula.")
}

df_raw <- do.call(rbind, all_chunks)
df     <- as.data.frame(unique(df_raw), stringsAsFactors = FALSE)

cat("\nDimensões após consolidação:", nrow(df), "x", ncol(df), "\n")

if (ncol(df) == 0) {
  stop("Data frame sem colunas após consolidação — n_cols usado: ", n_cols)
}

colnames(df) <- if (length(headers) == ncol(df)) headers else paste0("V", seq_len(ncol(df)))

cat("Total de linhas únicas:", nrow(df), "\n")
print(head(df, 10))

# ── 7. EXPORT ─────────────────────────────────────────────────────────────────
out_file <- file.path(dirname(rstudioapi::getSourceEditorContext()$path),
                      "ANA_Agencies.csv")
write.csv(df, out_file, row.names = FALSE, fileEncoding = "UTF-8")
cat("Arquivo salvo em:", out_file, "\n")
