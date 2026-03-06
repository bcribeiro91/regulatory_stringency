# scrap_ANA_Agencias_final_clean.R
# Final version with proper column handling

library(chromote)
library(jsonlite)
library(stringr)
library(dplyr)

# ── 1. CONNECT TO BROWSER ─────────────────────────────────────────────────────
PBI_URL <- "https://app.powerbi.com/view?r=eyJrIjoiYWY2NDlhZjktNjZlYy00ZjE3LThmZGYtODUyNjA4OGUwYzU2IiwidCI6ImUwYmI0MDEyLTgxMGItNDY5YS04YjRkLTY2N2ZjZDFiYWY4OCJ9&pageName=ReportSectione2a118ad60b5488ab805"

if (!exists("b") || !inherits(b, "ChromoteSession")) {
  b <- ChromoteSession$new()
  b$Page$navigate(PBI_URL)
  cat("Aguardando Power BI carregar... (20 segundos)\n")
  Sys.sleep(20)
}

# ── 2. PORTUGUESE CHARACTER FIX ────────────────────────────────────────────────
fix_portuguese_chars <- function(text) {
  if (is.na(text) || is.null(text) || text == "") return(text)
  
  replacements <- c(
    "Ã¡" = "á", "Ã©" = "é", "Ã­" = "í", "Ã³" = "ó", "Ãº" = "ú",
    "Ã£" = "ã", "Ãµ" = "õ", "Ã§" = "ç",
    "Ã¢" = "â", "Ãª" = "ê", "Ã´" = "ô",
    "Ã" = "Á", "Ã‰" = "É", "Ã" = "Í", "Ã“" = "Ó", "Ãš" = "Ú",
    "Ãƒ" = "Ã", "Ã•" = "Õ", "Ã‡" = "Ç",
    "Ã‚" = "Â", "ÃŠ" = "Ê", "Ã”" = "Ô",
    "AgÃªncia" = "Agência", "agÃªncia" = "agência",
    "ServiÃ§os" = "Serviços", "serviÃ§os" = "serviços",
    "PÃºblicos" = "Públicos", "pÃºblicos" = "públicos",
    "CearÃ¡" = "Ceará", "cearÃ¡" = "ceará",
    "RegulaÃ§Ã£o" = "Regulação", "regulaÃ§Ã£o" = "regulação",
    "MunicÃ­pio" = "Município", "municÃ­pio" = "município",
    "AbrangÃªncia" = "Abrangência", "abrangÃªncia" = "abrangência"
  )
  
  for (i in seq_along(replacements)) {
    text <- gsub(names(replacements)[i], replacements[i], text, fixed = TRUE)
  }
  
  return(text)
}

# ── 3. ROW EXTRACTION ────────────────────────────────────────────────────────
get_rows <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
      const cells = [];
      document.querySelectorAll("[role=gridcell]").forEach(el => {
        const rect = el.getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0) {
          let text = "";
          if (el.getAttribute && el.getAttribute("title")) {
            text = el.getAttribute("title");
          } else if (el.innerText) {
            text = el.innerText;
          } else if (el.textContent) {
            text = el.textContent;
          }
          
          text = text.trim();
          
          if (text && text !== "Select Row" && text !== "Row Selection") {
            cells.push({
              text: text,
              top: Math.round(rect.top),
              left: Math.round(rect.left)
            });
          }
        }
      });
      
      if (cells.length === 0) return JSON.stringify([]);
      
      cells.sort((a, b) => a.top - b.top);
      
      const rows = [];
      let currentRow = [cells[0]];
      let currentTop = cells[0].top;
      
      for (let i = 1; i < cells.length; i++) {
        if (Math.abs(cells[i].top - currentTop) <= 10) {
          currentRow.push(cells[i]);
        } else {
          currentRow.sort((a, b) => a.left - b.left);
          rows.push(currentRow.map(c => c.text));
          currentRow = [cells[i]];
          currentTop = cells[i].top;
        }
      }
      
      if (currentRow.length > 0) {
        currentRow.sort((a, b) => a.left - b.left);
        rows.push(currentRow.map(c => c.text));
      }
      
      const dataRows = rows.filter(row => row.length >= 8 && row.length <= 12);
      
      return JSON.stringify(dataRows);
    })()
  ')
  
  if (is.null(res$result$value)) return(list())
  
  parsed <- fromJSON(res$result$value)
  
  if (length(parsed) == 0) return(list())
  
  if (is.matrix(parsed)) {
    result <- list()
    for (i in 1:nrow(parsed)) {
      result[[i]] <- as.character(parsed[i, ])
    }
    return(result)
  }
  
  if (is.list(parsed)) {
    return(lapply(parsed, as.character))
  }
  
  return(list(as.character(parsed)))
}

# ── 4. FOCUS AND SCROLL ───────────────────────────────────────────────────────
ensure_focus <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
      const firstCell = document.querySelector("[role=gridcell]");
      if (!firstCell) return null;
      
      const rect = firstCell.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0) {
        return {
          x: Math.round((rect.left + rect.right) / 2),
          y: Math.round((rect.top + rect.bottom) / 2)
        };
      }
      return null;
    })()
  ')
  
  if (!is.null(res$result$value)) {
    coords <- fromJSON(res$result$value)
    if (!is.null(coords) && !is.null(coords$x) && !is.null(coords$y)) {
      b$Input$dispatchMouseEvent(type = "mousePressed", x = coords$x, y = coords$y,
                                 button = "left", clickCount = 1)
      b$Input$dispatchMouseEvent(type = "mouseReleased", x = coords$x, y = coords$y,
                                 button = "left", clickCount = 1)
      return(TRUE)
    }
  }
  return(FALSE)
}

scroll_down <- function(b, n_times = 1) {
  for (i in seq_len(n_times)) {
    b$Input$dispatchKeyEvent(
      type = "rawKeyDown",
      windowsVirtualKeyCode = 34L,
      key = "PageDown"
    )
    b$Input$dispatchKeyEvent(
      type = "keyUp",
      windowsVirtualKeyCode = 34L,
      key = "PageDown"
    )
    Sys.sleep(0.5)
  }
  Sys.sleep(1)
}

# ── 5. END-OF-TABLE DETECTION ─────────────────────────────────────────────────
check_end_of_table <- function(b) {
  res <- b$Runtime$evaluate(expression = '
    (function() {
      const grid = document.querySelector("[role=grid]") || 
                   document.querySelector(".scrollRegion") ||
                   document.querySelector(".pivotTable");
      
      if (grid) {
        const atBottom = grid.scrollTop + grid.clientHeight >= grid.scrollHeight - 50;
        if (atBottom) return JSON.stringify({end: true, reason: "scroll_bottom"});
      }
      
      const cells = document.querySelectorAll("[role=gridcell]");
      let lastText = "";
      let repeatCount = 0;
      
      for (let i = cells.length - 1; i >= Math.max(0, cells.length - 10); i--) {
        const el = cells[i];
        const rect = el.getBoundingClientRect();
        if (rect.width > 0 && rect.height > 0) {
          let text = "";
          if (el.getAttribute && el.getAttribute("title")) {
            text = el.getAttribute("title");
          } else if (el.innerText) {
            text = el.innerText;
          }
          text = text.trim();
          
          if (text && text !== "Select Row" && text !== "Row Selection") {
            if (text === lastText) {
              repeatCount++;
            } else {
              repeatCount = 0;
            }
            lastText = text;
            
            if (repeatCount > 5) {
              return JSON.stringify({end: true, reason: "repeated_rows"});
            }
          }
        }
      }
      
      return JSON.stringify({end: false});
    })()
  ')
  
  if (!is.null(res$result$value)) {
    result <- fromJSON(res$result$value)
    return(result$end)
  }
  
  return(FALSE)
}

# ── 6. MAIN SCRAPING LOOP ──────────────────────────────────────────────────────
cat("Preparando para coleta...\n")
b$Runtime$evaluate(expression = 'window.scrollTo(0, 0);')
Sys.sleep(3)

cat("Focando na tabela...\n")
ensure_focus(b)
Sys.sleep(2)

all_rows <- list()
last_first_cell <- NULL
empty_count <- 0
page <- 1
max_pages <- 5000
end_detected <- FALSE
unique_first_cells <- c()

cat("\n🚀 Iniciando coleta COMPLETA dos dados...\n")
cat("Pressione Ctrl+C para interromper manualmente se necessário\n\n")

while (!end_detected && page <= max_pages) {
  
  current_rows <- get_rows(b)
  
  if (length(current_rows) == 0) {
    empty_count <- empty_count + 1
    cat("Página", page, ": sem dados visíveis (", empty_count, ")\n")
    
    if (empty_count >= 5) {
      cat("⚠️  Muitas páginas sem dados - provavelmente chegamos ao fim\n")
      end_detected <- TRUE
      break
    }
    
    ensure_focus(b)
    scroll_down(b, n_times = 1)
    page <- page + 1
    next
  }
  
  empty_count <- 0
  before_count <- length(all_rows)
  all_rows <- c(all_rows, current_rows)
  new_rows <- length(all_rows) - before_count
  
  first_row_preview <- if (length(current_rows) > 0 && length(current_rows[[1]]) > 0) {
    paste(current_rows[[1]][1:min(3, length(current_rows[[1]]))], collapse = " | ")
  } else {
    "Unknown"
  }
  
  current_first_cell <- if (length(current_rows) > 0 && length(current_rows[[1]]) > 0) {
    current_rows[[1]][1]
  } else {
    NULL
  }
  
  if (!is.null(current_first_cell) && !(current_first_cell %in% unique_first_cells)) {
    unique_first_cells <- c(unique_first_cells, current_first_cell)
  }
  
  cat(sprintf("Página %4d | +%3d novas | Total: %5d linhas | Primeiro: %s\n", 
              page, new_rows, length(all_rows), substr(first_row_preview, 1, 50)))
  
  if (!is.null(last_first_cell) && !is.null(current_first_cell)) {
    if (current_first_cell == last_first_cell) {
      cat("   ⚠️  Primeira célula repetida - verificando fim da tabela...\n")
      
      if (check_end_of_table(b)) {
        cat("   ✅ Fim da tabela detectado!\n")
        end_detected <- TRUE
        break
      }
    }
  }
  
  last_first_cell <- current_first_cell
  
  if (page %% 10 == 0) {
    cat("   Verificando se chegou ao fim...\n")
    if (check_end_of_table(b)) {
      cat("   ✅ Fim da tabela detectado na página", page, "\n")
      end_detected <- TRUE
      break
    }
  }
  
  ensure_focus(b)
  scroll_down(b, n_times = 1)
  page <- page + 1
}

# ── 7. FINAL SUMMARY ─────────────────────────────────────────────────────────
cat("\n", rep("=", 60), "\n", sep="")
cat("🎉 COLETA FINALIZADA!\n")
cat(rep("=", 60), "\n", sep="")
cat("Total de páginas processadas:", page, "\n")
cat("Total de linhas coletadas:", length(all_rows), "\n")
cat("Motivo da parada:", ifelse(end_detected, "Fim da tabela detectado", "Limite máximo de páginas"), "\n")

# ── 8. PROCESS DATA ──────────────────────────────────────────────────────────
cat("\nProcessando dados...\n")

col_counts <- sapply(all_rows, length)
cat("Distribuição de colunas:\n")
print(table(col_counts))

# Use the most common column count
target_cols <- as.numeric(names(sort(table(col_counts), decreasing = TRUE))[1])
cat("Usando", target_cols, "colunas como padrão\n")

# Keep only rows with target columns
valid_rows <- all_rows[col_counts == target_cols]
cat("Linhas válidas:", length(valid_rows), "\n")

# Convert to data frame
df <- as.data.frame(do.call(rbind, valid_rows), stringsAsFactors = FALSE)

# ── 9. APPLY PORTUGUESE FIXES ────────────────────────────────────────────────
cat("Corrigindo caracteres portugueses...\n")

for (j in 1:ncol(df)) {
  if (is.character(df[[j]])) {
    df[[j]] <- sapply(df[[j]], function(x) {
      x <- fix_portuguese_chars(x)
      if (!is.na(x)) {
        x <- gsub('^"|"$', '', x)
        x <- gsub('"', '', x)
        x <- str_trim(x)
        if (x == "" || x == "########") x <- NA
      }
      return(x)
    })
  }
}

# ── 10. ASSIGN COLUMN NAMES ──────────────────────────────────────────────────
# Based on your sample, we have 9 columns most commonly
if (ncol(df) == 9) {
  colnames(df) <- c("Cod_IBGE", "UF", "Municipio", "CNPJ_ERI", 
                    "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI",
                    "Data_inicio", "Setorialidade")
} else if (ncol(df) == 10) {
  colnames(df) <- c("Cod_IBGE", "UF", "Municipio", "CNPJ_ERI", 
                    "Nome_ERI", "Sigla_ERI", "Abrangencia_ERI",
                    "Data_inicio", "Data_validade", "Setorialidade")
} else {
  colnames(df) <- paste0("Col", 1:ncol(df))
}

# ── 11. CLEAN CNPJ (with safety check) ───────────────────────────────────────
cnpj_col <- which(grepl("CNPJ", names(df), ignore.case = TRUE))
if (length(cnpj_col) > 0) {
  # Check if column exists and has data
  if (nrow(df) > 0 && cnpj_col[1] <= ncol(df)) {
    cat("Limpando CNPJ...\n")
    
    # Process CNPJ safely
    cnpj_values <- df[[cnpj_col[1]]]
    
    # Only process if there are non-NA values
    if (any(!is.na(cnpj_values))) {
      df[[cnpj_col[1]]] <- sapply(cnpj_values, function(x) {
        if (is.na(x)) return(NA)
        x <- gsub("[^0-9]", "", as.character(x))
        if (nchar(x) == 0) return(NA)
        if (grepl("e", x, ignore.case = TRUE)) {
          x <- formatC(as.numeric(x), format = "f", digits = 0, big.mark = "")
        }
        x <- gsub("[^0-9]", "", x)
        if (nchar(x) < 14) {
          x <- paste0(paste(rep("0", 14 - nchar(x)), collapse = ""), x)
        } else if (nchar(x) > 14) {
          x <- substr(x, 1, 14)
        }
        return(x)
      })
    } else {
      cat("⚠️  CNPJ column has no valid data\n")
    }
  }
}

# ── 12. FIX Cod_IBGE ─────────────────────────────────────────────────────────
ibge_col <- which(grepl("Cod|IBGE", names(df), ignore.case = TRUE))[1]
if (length(ibge_col) > 0 && ibge_col <= ncol(df)) {
  cat("Limpando Código IBGE...\n")
  
  ibge_values <- df[[ibge_col]]
  if (any(!is.na(ibge_values))) {
    df[[ibge_col]] <- sapply(ibge_values, function(x) {
      if (is.na(x)) return(NA)
      x <- gsub("[^0-9]", "", as.character(x))
      if (nchar(x) == 0) return(NA)
      formatC(as.numeric(x), width = 7, format = "d", flag = "0")
    })
  }
}

# ── 13. REMOVE DUPLICATES AND NA ROWS ────────────────────────────────────────
df <- unique(df)
df <- df[rowSums(is.na(df)) < ncol(df), ]

cat("\n✅ Dados finais:", nrow(df), "linhas,", ncol(df), "colunas\n")

# ── 14. SHOW SAMPLE ──────────────────────────────────────────────────────────
cat("\n📋 Amostra dos dados coletados:\n")
print(head(df, 20))

# Check for specific cities
if ("Municipio" %in% names(df)) {
  natal_rows <- which(df$Municipio == "Natal")
  if (length(natal_rows) > 0) {
    cat("\n✅ Natal encontrado na linha", natal_rows[1], "\n")
  }
  
  # Show first few municipalities to verify order
  cat("\nPrimeiros municípios (deve começar com Natal):\n")
  print(head(df$Municipio, 10))
}

# ── 15. EXPORT ────────────────────────────────────────────────────────────────
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_file <- file.path(getwd(), paste0("ANA_Agencias_COMPLETA_", timestamp, ".csv"))
write.csv(df, out_file, row.names = FALSE, fileEncoding = "UTF-8")
cat("\n📁 CSV salvo:", out_file, "\n")

rds_file <- file.path(getwd(), paste0("ANA_Agencias_COMPLETA_", timestamp, ".rds"))
saveRDS(df, rds_file)
cat("📁 RDS salvo:", rds_file, "\n")

# ── 16. STATISTICS ────────────────────────────────────────────────────────────
cat("\n📊 Estatísticas:\n")
if ("UF" %in% names(df)) {
  uf_counts <- table(df$UF)
  cat("Estados presentes (", length(uf_counts), "):\n", sep="")
  print(uf_counts)
}