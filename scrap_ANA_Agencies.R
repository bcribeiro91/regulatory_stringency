library(jsonlite)

# Função para extrair as linhas visíveis
get_visible_rows <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
      var cells = [];
      document.querySelectorAll("[role=gridcell]").forEach(function(el) {
        var t = (el.innerText || "").trim();
        var rect = el.getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0 && t !== "Select Row") {
          cells.push({text: t, left: rect.left, top: rect.top});
        }
      });
      cells.sort(function(a,b) { return a.top - b.top || a.left - b.left; });
      var rows = [];
      if (cells.length === 0) return JSON.stringify([]);
      var cur = [cells[0]];
      for (var i = 1; i < cells.length; i++) {
        if (Math.abs(cells[i].top - cur[0].top) <= 3) {
          cur.push(cells[i]);
        } else {
          cur.sort(function(a,b) { return a.left - b.left; });
          rows.push(cur.map(function(c) { return c.text; }));
          cur = [cells[i]];
        }
      }
      cur.sort(function(a,b) { return a.left - b.left; });
      rows.push(cur.map(function(c) { return c.text; }));
      return JSON.stringify(rows.filter(function(r) { return r.length === 7; }));
    })()
  ')
  fromJSON(res$result$value)
}

# Função para pressionar ArrowDown N vezes
press_arrow_down <- function(b, n = 20) {
  for (i in 1:n) {
    b$Input$dispatchKeyEvent(type = "rawKeyDown",
                             windowsVirtualKeyCode = 40L, nativeVirtualKeyCode = 40L,
                             key = "ArrowDown", code = "ArrowDown")
    b$Input$dispatchKeyEvent(type = "keyUp",
                             windowsVirtualKeyCode = 40L, nativeVirtualKeyCode = 40L,
                             key = "ArrowDown", code = "ArrowDown")
  }
  Sys.sleep(1)
}

# ---- Loop principal ----
all_data <- list()
seen_first <- c()
max_pages  <- 200  # limite de segurança

for (page in 1:max_pages) {
  rows <- get_visible_rows(b)
  
  if (length(rows) == 0) {
    cat("Sem linhas na página", page, "- encerrando.\n")
    break
  }
  
  first_cnpj <- rows[[1]][1]
  
  # Parar se voltamos ao início (scroll chegou ao fim)
  if (first_cnpj %in% seen_first && page > 1) {
    cat("CNPJ repetido detectado na página", page, "- fim da tabela.\n")
    break
  }
  
  seen_first <- c(seen_first, first_cnpj)
  all_data   <- c(all_data, list(rows))
  
  cat(sprintf("Página %d | Primeiro: %s | Último: %s\n",
              page, rows[[1]][1], rows[[length(rows)]][1]))
  
  # Avançar 20 linhas
  press_arrow_down(b, n = 20)
}

# ---- Consolidar ----
df <- do.call(rbind, lapply(all_data, function(chunk) {
  do.call(rbind, lapply(chunk, function(r) as.data.frame(t(r), stringsAsFactors = FALSE)))
}))

# Remover duplicatas (sobreposição entre páginas)
df <- unique(df)
colnames(df) <- paste0("V", 1:7)  # renomear depois que souber os headers

cat("Total de linhas únicas:", nrow(df), "\n")
