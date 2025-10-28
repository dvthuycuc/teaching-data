suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(lubridate)
})

db_path <- "steam_data.duckdb"
con <- dbConnect(duckdb::duckdb(), db_path)

# ---- Get command-line argument ----
args <- commandArgs(trailingOnly = TRUE)
x_days <- ifelse(length(args) >= 1, as.numeric(args[1]), 730)
appid <- ifelse(length(args) >= 2, as.numeric(args[2]), NA)

cutoff_date <- Sys.Date() - x_days
print(paste0("Deleting records older than ", x_days, " days (before ", cutoff_date, ")"))

if (is.na(appid)) {
  query <- sprintf("DELETE FROM STEAM_NUM_PLAYERS WHERE record_date < '%s';", cutoff_date)
} else {
  query <- sprintf("DELETE FROM STEAM_NUM_PLAYERS WHERE record_date < '%s' AND appid = %d;", cutoff_date, appid)
}

deleted <- tryCatch({
  dbExecute(con, query)
}, error = function(e) {
  message("Error deleting records: ", e$message)
  return(0)
})

print("Done deleted ")
dbDisconnect(con, shutdown = TRUE)