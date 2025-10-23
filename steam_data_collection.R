library(jsonlite)
library(dplyr)

# get game ID:
url <- "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
raw <- fromJSON(url)
df_game_id <- as.data.frame(raw$applist$apps, stringsAsFactors = FALSE)
df_game_id <- df_game_id |> 
  filter(nzchar(name)) |> 
  arrange(appid)
head(df_game_id)
dim(df_game_id)

# #get game Info:
# fun_get_game_info <- function(appid, primary_tbl, foreign_tbl, tbl_list){
#   url <- paste0("https://store.steampowered.com/api/appdetails?appids=", appid)
#   raw <- fromJSON(url)
# }
url <- paste0("https://store.steampowered.com/api/appdetails?appids=", 570)
raw <- fromJSON(url)
data <- raw[["5"]]$data
data