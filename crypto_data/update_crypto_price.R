suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(DBI)
  library(duckdb)
  library(lubridate)
})

db_path <- "crypto_data.duckdb"
con <- dbConnect(duckdb::duckdb(), db_path)

# ---- Get command-line argument ----
args <- commandArgs(trailingOnly = TRUE)
coin_codes <- if (length(args) >= 1 && nzchar(args[1])) c(toupper(args[1])) else "all"
if (coin_codes =="all"){
  coin_codes <- dbReadTable(con, "CRYPTO_COIN")$coin_code
}

get_binance_klines <- function(symbol = "BTCUSDT",
                               interval = "1h",
                               rows = 2000,
                               base_url = "https://api.binance.com") {
  
  to_fetch <- rows
  end_time_ms <- NULL  # NULL = latest window
  all_df <- list()
  
  col_names <- c(
    "open_time_ms","open","high","low","close","volume",
    "close_time_ms","quote_asset_volume","number_of_trades",
    "taker_buy_base_volume","taker_buy_quote_volume","ignore"
  )
  
  ms_str <- function(x) format(floor(x), scientific = FALSE, trim = TRUE)
  
  parse_batch <- function(lst) {
    if (length(lst) == 0) return(tibble())
    df <- as.data.frame(do.call(rbind, lst), stringsAsFactors = FALSE)
    names(df) <- col_names
    df |>
      mutate(
        open_time_ms = as.numeric(open_time_ms),
        close_time_ms = as.numeric(close_time_ms),
        open  = as.numeric(open),
        high  = as.numeric(high),
        low   = as.numeric(low),
        close = as.numeric(close),
        volume = as.numeric(volume),
        open_time  = as_datetime(open_time_ms/1000, tz = "UTC"),
        close_time = as_datetime(close_time_ms/1000, tz = "UTC")
      )
  }
  
  while (to_fetch > 0) {
    limit <- min(1000, to_fetch)
    url <- paste0(base_url, "/api/v3/klines")
    query <- list(symbol = symbol, interval = interval, limit = limit)
    if (!is.null(end_time_ms)) query$endTime <- ms_str(end_time_ms)  # <-- no as.integer()
    
    resp <- GET(url, query = query, timeout(20))
    if (http_error(resp)) {
      stop(sprintf("HTTP error %s: %s",
                   status_code(resp),
                   content(resp, as = "text", encoding = "UTF-8")))
    }
    
    payload <- content(resp, as = "text", encoding = "UTF-8")
    batch_list <- tryCatch(fromJSON(payload, simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(batch_list)) stop("Failed to parse JSON from Binance response.")
    if (length(batch_list) == 0) break
    
    batch_df <- parse_batch(batch_list)
    if (nrow(batch_df) == 0) break
    
    all_df[[length(all_df) + 1]] <- batch_df
    to_fetch <- to_fetch - nrow(batch_df)
    
    earliest_open_ms <- suppressWarnings(min(batch_df$open_time_ms, na.rm = TRUE))
    if (!is.finite(earliest_open_ms)) break
    end_time_ms <- earliest_open_ms - 1 
    
    Sys.sleep(0.12)
  }
  
  out <- bind_rows(all_df) |>
    arrange(open_time) |>
    select(open_time, open, high, low, close, volume)
  
  if (nrow(out) > rows) out <- tail(out, rows)
  out
}

save_data_in_db <- function(coin_code, data, table){
  dbExecute(
    con,
    paste0("DELETE FROM ", table, " WHERE coin_code = ?"),
    params = list(coin_code)
  )
  dbWriteTable(con, table, data, append = TRUE)
}

for (coin_code in coin_codes){
  df_1h <- get_binance_klines(symbol = coin_code, interval = "1h") |> 
    mutate(coin_code = coin_code)
  save_data_in_db(coin_code, df_1h, "CRYPTO_PRICE_1H")
  df_1d <- get_binance_klines(symbol = coin_code, interval = "1d") |> 
    mutate(coin_code = coin_code)
  save_data_in_db(coin_code, df_1d, "CRYPTO_PRICE_1D")
  df_1w <- get_binance_klines(symbol = coin_code, interval = "1w") |> 
    mutate(coin_code = coin_code)
  save_data_in_db(coin_code, df_1w, "CRYPTO_PRICE_1W")
}

message("Done updating price")
dbDisconnect(con)

