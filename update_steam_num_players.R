suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(DBI)
  library(duckdb)
  library(lubridate)
})

db_path <- "steam_data.duckdb"
con <- dbConnect(duckdb::duckdb(), db_path)

# ---- Get command-line argument ----
args <- commandArgs(trailingOnly = TRUE)
appIds <- ifelse(length(args) == 0, "all", c(args[1]))
appId_list <- appIds
if (appIds == "all"){
  df_appdetails_db <- dbReadTable(con, "STEAM_APPDETAILS")
  appId_list = df_appdetails_db$appid
}

get_now_app_players <- function(appid) {
  url <- paste0("https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=", appid)
  raw <- fromJSON(url)
  if (raw$response$result == 1){
    return (raw$response$player_count)
  }
  else if (raw$response$result != 0){
    return (raw$result)
  }
  return (0)
}



for (appid in appId_list){
  result <- try({
    players <- get_now_app_players(appid)
    if (players>0){
      today <- Sys.Date()
      dbExecute(
        con,
        "
        INSERT INTO STEAM_NUM_PLAYERS (appid, record_date, num_players)
        VALUES (?, ?, ?)
        ON CONFLICT (appid, record_date)
        DO UPDATE SET num_players = EXCLUDED.num_players;
        ",
        params = list(appid, today, players)
      )
    }, silent=TRUE})
    
}

print(paste0("Update today players for app ", appIds))

dbDisconnect(con)
