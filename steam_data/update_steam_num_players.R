suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(DBI)
  library(duckdb)
  library(lubridate)
})

db_path <- Sys.getenv("STEAM_DB_PATH", "steam_data.duckdb")
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

get_historical_app_players <- function(appid){
  url <- paste0("https://steamcharts.com/app/", appid, "/chart-data.json")
  response <- httr::GET(url)
  data_json <- jsonlite::fromJSON(rawToChar(response$content))
  df_players <- as.data.frame(data_json)
  names(df_players) <- c("timestamp", "num_players")
  df_players <- df_players |> 
    mutate(
      appid = appid,
      record_date = as_datetime(timestamp / 1000) %>% as_date()
    ) |> 
    select(appid, record_date, num_players)
  df_players <- df_players %>%
    group_by(record_date) %>%
    summarise(num_players = as.integer(mean(num_players, na.rm = TRUE))) |> 
    mutate(appid = appid)
  return (df_players)
}

upsert_historical_app_players <- function(con, appid) {
  df_hist <- get_historical_app_players(appid)
  if (nrow(df_hist) == 0) return(invisible(NULL))
  
  existing_dates <- dbGetQuery(
    con,
    "SELECT record_date FROM STEAM_NUM_PLAYERS WHERE appid = ?",
    params = list(appid)
  )$record_date
  if (length(existing_dates) > 0) {
    df_hist <- df_hist %>% filter(!(record_date %in% as.Date(existing_dates)))
  }
  if (nrow(df_hist) == 0) return(invisible(NULL))
  
  for (i in seq_len(nrow(df_hist))) {
    dbExecute(
      con,
      "
      INSERT INTO STEAM_NUM_PLAYERS (appid, record_date, num_players)
      VALUES (?, ?, ?)
      ON CONFLICT (appid, record_date)
      DO UPDATE SET num_players = EXCLUDED.num_players;
      ",
      params = list(df_hist$appid[i], df_hist$record_date[i], df_hist$num_players[i])
    )
  }
  
  invisible(NULL)
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
    }
    }, silent=FALSE)
  upsert_historical_app_players(con, appid)
}

cat("Update today players for app ", appIds)
dbDisconnect(con)
