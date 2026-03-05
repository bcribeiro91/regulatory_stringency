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

# Extract column headers (used for naming only; not critical for row detection).
# Looks for [role=columnheader] elements that are just above the LARGEST cluster
# of gridcells (= the data table), ignoring summary cards and nav tabs.
get_headers <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
      // Count gridcells per y-position, excluding "Select Row"
      var rowCounts = {};
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var t = (el.innerText || "").trim();
        var r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0 && t !== "Select Row") {
          var k = Math.round(r.top);
          rowCounts[k] = (rowCounts[k] || 0) + 1;
        }
      });

      // Find the most common cell count per row (= data columns)
      var freq = {};
      Object.values(rowCounts).forEach(function(n) { freq[n] = (freq[n] || 0) + 1; });
      var dataCols = parseInt(Object.keys(freq).sort(function(a,b){
        return freq[b]-freq[a];
      })[0]);

      // First y-position where a full data row appears
      var dataYs = Object.keys(rowCounts)
        .filter(function(y) { return rowCounts[y] === dataCols; })
        .map(Number).sort(function(a,b){return a-b;});
      if (dataYs.length === 0) return JSON.stringify([]);
      var firstDataY = dataYs[0];

      // Columnheaders just above the first data row (within 120px)
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

# Click the first cell of an actual DATA ROW (not summary cards or nav tabs).
# Summary cards / nav tabs have fewer cells per row than the data table, so we
# identify data rows as those matching the most common cells-per-row count.
ensure_focus <- function(b) {
  b$Runtime$evaluate(expression = '
    (function() {
      // Count cells per y-position
      var rowCounts = {};
      var allCells = [];
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var t = (el.innerText || "").trim();
        var r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0 && t !== "Select Row") {
          var k = Math.round(r.top);
          rowCounts[k] = (rowCounts[k] || 0) + 1;
          allCells.push({el: el, top: k, text: t});
        }
      });
      // Most common count per row = data columns
      var freq = {};
      Object.values(rowCounts).forEach(function(n) { freq[n] = (freq[n]||0)+1; });
      var dataCols = parseInt(
        Object.keys(freq).sort(function(a,b){ return freq[b]-freq[a]; })[0]
      );
      // Click the first cell that belongs to a full data row
      for (var i = 0; i < allCells.length; i++) {
        if (rowCounts[allCells[i].top] === dataCols) {
          allCells[i].el.scrollIntoView({block: "nearest"});
          allCells[i].el.focus();
          allCells[i].el.click();
          return "ok [" + dataCols + " cols]: " + allCells[i].text.substring(0,25);
        }
      }
      return "no data cell found";
    })()
  ')
  Sys.sleep(0.4)
}

# Extract visible data rows as a character matrix.
# Does NOT filter by a fixed column count in JS — instead returns all rows and
# lets R pick the most common row length (= true data columns).
get_visible_rows <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
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
      return JSON.stringify(rows);
    })()
  ')

  parsed <- fromJSON(res$result$value)
  if (length(parsed) == 0) return(matrix(character(0), nrow = 0, ncol = 0))

  # fromJSON returns a matrix when all rows have the same length — accept directly
  if (is.matrix(parsed)) return(parsed)

  # Mixed lengths (summary cards, nav tabs, data rows): keep only the most
  # common row length, which corresponds to the actual data columns
  row_lens   <- sapply(parsed, length)
  target_len <- as.integer(names(sort(table(row_lens), decreasing = TRUE))[1])
  data_rows  <- parsed[row_lens == target_len]
  do.call(rbind, lapply(data_rows, function(r) matrix(r, nrow = 1)))
}

# Fix UTF-8 mojibake: Power BI serves text with UTF-8 bytes rendered as Latin-1.
# "RegulaÃ§Ã£o" → "Regulação": take each char's code-point as a raw byte, then
# declare those bytes as UTF-8.
fix_encoding <- function(x) {
  if (is.na(x) || nchar(x) == 0L) return(x)
  tryCatch({
    z <- rawToChar(as.raw(utf8ToInt(x) %% 256L))
    Encoding(z) <- "UTF-8"
    z
  }, error = function(e) x)
}

# Press ArrowDown n times to advance the virtual-scroll table.
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

# ── 3. NAVIGATE TO THE "Serviços" TAB ────────────────────────────────────────
# The target table (Cód. IBGE | UF | Município | CNPJ ERI | ...) lives on the
# "Serviços" tab. We click it by matching its exact text content.
res_nav <- b$Runtime$evaluate(expression = '
  (function() {
    var cells = document.querySelectorAll("[role=gridcell]");
    for (var i = 0; i < cells.length; i++) {
      var t = (cells[i].innerText || "").trim();
      if (t === "Serviços") {
        cells[i].click();
        return "clicked: Serviços";
      }
    }
    return "tab Serviços not found";
  })()
')
cat(res_nav$result$value, "\n")
Sys.sleep(4)   # wait for the tab and its table to render

# ── 4. GET COLUMN HEADERS (for naming only) ───────────────────────────────────
headers <- character(0)
for (.attempt in 1:5) {
  headers <- get_headers(b)
  if (length(headers) > 0) break
  cat("Tentativa", .attempt, "— aguardando headers...\n")
  Sys.sleep(3)
}
cat("Headers encontrados:", length(headers),
    if (length(headers) > 0) paste0("| ", paste(headers, collapse = " | ")) else "", "\n")

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
prev_sig    <- ""
stall_count <- 0
max_pages   <- 1000

for (page in seq_len(max_pages)) {

  mat <- get_visible_rows(b)   # auto-detects column count each call

  if (nrow(mat) == 0) {
    cat("Sem linhas visíveis na página", page, "— encerrando.\n")
    break
  }

  cur_sig <- paste(mat[nrow(mat), ], collapse = "|")

  cat(sprintf("Pág %4d | %2d linhas | %d cols | Início: %-20s | Fim: %-20s\n",
              page, nrow(mat), ncol(mat),
              substr(mat[1,           1], 1, 20),
              substr(mat[nrow(mat),   1], 1, 20)))

  all_chunks <- c(all_chunks, list(mat))

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

  ensure_focus(b)
  press_arrow_down(b, n = 18)
}

# ── 6. CONSOLIDATE ───────────────────────────────────────────────────────────
if (length(all_chunks) == 0) {
  stop("Nenhum dado coletado. Verifique: (1) tabela visível no browser, (2) foco na célula.")
}

df_raw <- do.call(rbind, all_chunks)
df     <- as.data.frame(unique(df_raw), stringsAsFactors = FALSE)

cat("\nDimensões após consolidação:", nrow(df), "x", ncol(df), "\n")

# ── Column names ─────────────────────────────────────────────────────────────
col_names_known <- c("Cod_IBGE", "UF", "Municipio", "CNPJ_ERI",
                     "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI", "Data_inicio")
if (length(headers) == ncol(df)) {
  colnames(df) <- headers
} else if (ncol(df) == length(col_names_known)) {
  colnames(df) <- col_names_known
} else {
  colnames(df) <- paste0("V", seq_len(ncol(df)))
  warning("Número de colunas (", ncol(df), ") não esperado. Renomear manualmente.")
}

# ── Fix encoding (UTF-8 mojibake) ────────────────────────────────────────────
df[] <- lapply(df, function(col) sapply(col, fix_encoding, USE.NAMES = FALSE))

# ── Fix CNPJ scientific notation (e.g. "2.07691E+13" → "20769100000000") ────
# CNPJ: fix scientific notation and pad to 14 digits
if ("CNPJ_ERI" %in% colnames(df)) {
  df$CNPJ_ERI <- sapply(df$CNPJ_ERI, function(x) {
    if (grepl("[Ee]\\+", x)) {
      formatC(as.numeric(x), format = "f", digits = 0, big.mark = "")
    } else {
      formatC(x, width = 14, flag = "0")
    }
  }, USE.NAMES = FALSE)
}
# Cód. IBGE: pad to 7 digits
if ("Cod_IBGE" %in% colnames(df)) {
  df$Cod_IBGE <- formatC(df$Cod_IBGE, width = 7, flag = "0")
}

cat("Total de linhas únicas:", nrow(df), "\n")
print(head(df, 10))

# ── 7. EXPORT ─────────────────────────────────────────────────────────────────
out_file <- file.path(dirname(rstudioapi::getSourceEditorContext()$path),
                      "ANA_Agencies.csv")
write.csv(df, out_file, row.names = FALSE, fileEncoding = "UTF-8")
cat("Arquivo salvo em:", out_file, "\n")
