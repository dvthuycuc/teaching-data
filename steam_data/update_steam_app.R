suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(DBI)
  library(duckdb)
  library(lubridate)
})
db_path <- "steam_data.duckdb"

if (!file.exists(db_path)) {
  con <- dbConnect(duckdb::duckdb(), db_path)
  
  ddl <- "
  -- 1) Master table
  CREATE TABLE IF NOT EXISTS STEAM_APPDETAILS (
    appid                 INTEGER PRIMARY KEY,
    type                  VARCHAR,
    name                  VARCHAR,
    is_free               BOOLEAN,
    detailed_description  TEXT,
    about_the_game        TEXT,
    short_description     TEXT,
    supported_languages   TEXT,
    reviews               TEXT,
    header_image          VARCHAR,
    capsule_image         VARCHAR,
    capsule_imagev5       VARCHAR,
    website               VARCHAR,
    developers            VARCHAR,
    publishers            VARCHAR,
    metacritic_score      INTEGER,
    metacritic_url        VARCHAR,
    price                 VARCHAR,
    total_recommendations INTEGER,
    release_date          DATE,
    content_descriptors   TEXT,
    rating_age            INTEGER
  );

  -- 2) Platforms
  CREATE TABLE IF NOT EXISTS STEAM_PLATFORMS (
    appid                      INTEGER PRIMARY KEY REFERENCES STEAM_APPDETAILS(appid),
    windows                    BOOLEAN,
    mac                        BOOLEAN,
    linux                      BOOLEAN,
    pc_requirements_minimum    TEXT,
    mac_requirements_minimum   TEXT,
    linux_requirements_minimum TEXT
  );

  -- 3) Categories
  CREATE TABLE IF NOT EXISTS STEAM_CATEGORIES (
    appid     INTEGER NOT NULL REFERENCES STEAM_APPDETAILS(appid),
    categories VARCHAR NOT NULL
  );

  -- 4) Genres
  CREATE TABLE IF NOT EXISTS STEAM_GENRES (
    appid  INTEGER NOT NULL REFERENCES STEAM_APPDETAILS(appid),
    genres VARCHAR NOT NULL
  );

  -- 5) Player counts
  CREATE TABLE IF NOT EXISTS STEAM_NUM_PLAYERS (
    appid       INTEGER NOT NULL REFERENCES STEAM_APPDETAILS(appid),
    record_date    DATE NOT NULL,
    num_players INTEGER,
    CONSTRAINT pk_players PRIMARY KEY (appid, record_date)
  );
  "
  
  # execute DDL
  dbExecute(con, ddl)
} else {
  con <- dbConnect(duckdb::duckdb(), db_path)
}

df_appdetails_db <- dbReadTable(con, "STEAM_APPDETAILS") 
df_player_db <- dbReadTable(con, "STEAM_NUM_PLAYERS") 

#get game Info:
fun_get_game_info <- function(appid){
  url <- paste0("https://store.steampowered.com/api/appdetails?appids=", appid)
  raw <- fromJSON(url)
  return (raw)
}
 
parse_steam_date <- function(x) {
  if (is.null(x) || is.na(x)) return(as.Date(NA))
  x <- trimws(x)
  if (x == "" || grepl("coming", x, ignore.case = TRUE)) return(as.Date(NA))
  
  fmts <- c("%d %b, %Y", "%b %d, %Y", "%b %Y", "%Y")
  for (f in fmts) {
    dt <- as.Date(strptime(x, f, tz = "UTC"))
    if (!is.na(dt)) return(dt)
  }
  as.Date(NA)
}

# get all game ID:
url <- "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
raw <- fromJSON(url)
df_game_id <- as.data.frame(raw$applist$apps, stringsAsFactors = FALSE)
df_game_id <- df_game_id |>
  filter(nzchar(name)) |>
  filter(!appid %in% df_appdetails_db$appid) |> 
  arrange(appid)


for (i in 1:nrow(df_game_id)) {
  appid <- df_game_id$appid[i]
  app_result <- try({
    app_data <- fun_get_game_info(appid)
    if (!isTRUE(app_data[[as.character(appid)]]$success)) {
      next
    }
  }, silent=TRUE)
  if (inherits(app_result, "try-error")) {
    next
  }
  
  result <- try({
    df_categories <- data.frame()
    df_genres <- data.frame()
    # Add row into appdetails
    df_appdetails <- tibble(
        appid = appid,
        name = df_game_id$name[i],
        type = app_data[[as.character(appid)]]$data$type,
        is_free = app_data[[as.character(appid)]]$data$is_free,
        detailed_description = app_data[[as.character(appid)]]$data$detailed_description,
        about_the_game = app_data[[as.character(appid)]]$data$about_the_game,
        short_description = app_data[[as.character(appid)]]$data$short_description,
        supported_languages = app_data[[as.character(appid)]]$data$supported_languages,
        reviews = app_data[[as.character(appid)]]$data$reviews %||% NA_character_,
        header_image = app_data[[as.character(appid)]]$data$header_image,
        capsule_image = app_data[[as.character(appid)]]$data$capsule_image,
        capsule_imagev5 = app_data[[as.character(appid)]]$data$capsule_imagev5,
        website = app_data[[as.character(appid)]]$data$website %||% NA_character_,
        developers = app_data[[as.character(appid)]]$data$developers,
        publishers = app_data[[as.character(appid)]]$data$publishers,
        metacritic_score = app_data[[as.character(appid)]]$data$metacritic$score %||% 0,
        metacritic_url = app_data[[as.character(appid)]]$data$metacritic$url %||% NA_character_,
        price = app_data[[as.character(appid)]]$data$price_overview$final_formatted %||% NA_character_,
        total_recommendations = app_data[[as.character(appid)]]$data$recommendations$total,
        release_date = parse_steam_date(app_data[[as.character(appid)]]$data$release_date$date),
        content_descriptors = app_data[[as.character(appid)]]$data$content_descriptors$notes,
        rating_age = as.numeric(app_data[[as.character(appid)]]$data$ratings$usk$rating %||% 
                                  app_data[[as.character(appid)]]$data$ratings$steam_germany$required_age %||%
                                  app_data[[as.character(appid)]]$data$required_age)
      )
    
    df_platforms <- tibble(
        appid = appid,
        windows = app_data[[as.character(appid)]]$data$platforms$windows,
        mac = app_data[[as.character(appid)]]$data$platforms$mac,
        linux = app_data[[as.character(appid)]]$data$platforms$linux,
        pc_requirements_minimum = app_data[[as.character(appid)]]$data$pc_requirements$minimum,
        mac_requirements_minimum = app_data[[as.character(appid)]]$data$mac_requirements$minimum,
        linux_requirements_minimum = app_data[[as.character(appid)]]$data$linux_requirements$minimum
      )
    
    categories <- app_data[[as.character(appid)]]$data$categories$description
    for (c in categories){
      df_categories <- df_categories %>%
        bind_rows(tibble(
          appid = appid,
          categories = c,
        ))
    }
    
    genres <- app_data[[as.character(appid)]]$data$genres$description
    for (g in genres){
      df_genres <- df_genres %>%
        bind_rows(tibble(
          appid = appid,
          genres = g,
        ))
    }
    
    #get historical data
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

    dbWriteTable(con, "STEAM_APPDETAILS", df_appdetails, append = TRUE)
    dbWriteTable(con, "STEAM_PLATFORMS", df_platforms, append = TRUE)
    dbWriteTable(con, "STEAM_CATEGORIES", df_categories, append = TRUE)
    dbWriteTable(con, "STEAM_GENRES", df_genres, append = TRUE)
    dbWriteTable(con, "STEAM_NUM_PLAYERS", df_players, append = TRUE)
    print(paste0("Updated appid ", appid))
  }, silent=TRUE)
  if (inherits(result, "try-error")) {
    cat(appid, "Error:", conditionMessage(attr(result, "condition")), "\n")
  }
}

dbDisconnect(con)





