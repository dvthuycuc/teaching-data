suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

db_path <- "crypto_data.duckdb"

if (!file.exists(db_path)) {
  con <- dbConnect(duckdb::duckdb(), db_path)
  
  ddl <- "
  -- 1) Master table
  CREATE TABLE IF NOT EXISTS CRYPTO_COIN (
    coin_code                 VARCHAR PRIMARY KEY
  );
  
  -- 3) CRYPTO_PRICE_1H
  CREATE TABLE IF NOT EXISTS CRYPTO_PRICE_1H (
    coin_code                 VARCHAR REFERENCES CRYPTO_COIN(coin_code),
    open_time                 DATETIME,
    open                      FLOAT,
    close                     FLOAT,
    high                      FLOAT,
    low                       FLOAT,
    volume                    FLOAT
  );
  
  -- 4) CRYPTO_PRICE_1D
  CREATE TABLE IF NOT EXISTS CRYPTO_PRICE_1D (
    coin_code                 VARCHAR REFERENCES CRYPTO_COIN(coin_code),
    open_time                 DATETIME,
    open                      FLOAT,
    close                     FLOAT,
    high                      FLOAT,
    low                       FLOAT,
    volume                    FLOAT
  );
  
  -- 4) CRYPTO_PRICE_1M
  CREATE TABLE IF NOT EXISTS CRYPTO_PRICE_1W (
    coin_code                 VARCHAR REFERENCES CRYPTO_COIN(coin_code),
    open_time                 DATETIME,
    open                      FLOAT,
    close                     FLOAT,
    high                      FLOAT,
    low                       FLOAT,
    volume                    FLOAT
  );
  "
  
  dbExecute(con, ddl)
} else {
  con <- dbConnect(duckdb::duckdb(), db_path)
}

# ---- Get command-line argument ----
args <- commandArgs(trailingOnly = TRUE)
coin_code <- if (length(args) >= 1 && nzchar(args[1])) toupper(args[1]) else "BTCUSDT"
action    <- if (length(args) >= 2 && nzchar(args[2])) tolower(args[2]) else "add" # add or delete


add_coin <- function(con, code) {
  existing <- dbGetQuery(con, "SELECT coin_code FROM CRYPTO_COIN WHERE coin_code = ?", params = list(code))
  if (nrow(existing) > 0) {
    message("Coin already exists: ", code)
  } else {
    dbExecute(con, "INSERT INTO CRYPTO_COIN (coin_code) VALUES (?)", params = list(code))
    message("Added coin: ", code)
  }
}

delete_coin <- function(con, code) {
  dbExecute(con, "DELETE FROM CRYPTO_PRICE_1H WHERE coin_code = ?", params = list(code))
  dbExecute(con, "DELETE FROM CRYPTO_PRICE_1D WHERE coin_code = ?", params = list(code))
  dbExecute(con, "DELETE FROM CRYPTO_PRICE_1M WHERE coin_code = ?", params = list(code))
  dbExecute(con, "DELETE FROM CRYPTO_COIN WHERE coin_code = ?", params = list(code))
  
  message("Deleted coin: ", code)
}

if (action == "add") {
  add_coin(con, coin_code)
} else if (action == "delete") {
  delete_coin(con, coin_code)
} else {
  message("Invalid action. Use 'add' or 'delete'.")
}

dbDisconnect(con)