# TEACHING DATA REPO

This repository collects and structures multiple public datasets for study, teaching, and analysis purposes.
Each dataset is organized in a separate folder and includes scripts for automatic data collection and updates.

Each dataset folder contains:

- A local database (.duckdb)
- Data collection or update scripts (R )

#### Clone the project:

```r
git clone https://github.com/dvthuycuc/teaching-data.git
```

## Steam Data

The Steam dataset consists of five main tables, linked by the appid field (the unique identifier of each game on Steam).

<p align="center"> <img src="figures/steam_db_schema.png" alt="Steam Database Schema" width="600"/> </p>

| Table                 | Description                                                                        |
| --------------------- | ---------------------------------------------------------------------------------- |
| **STEAM_APPDETAILS**  | Core game information (name, description, developer, publisher, Metacritic score). |
| **STEAM_PLATFORMS**   | Supported operating systems and minimum system requirements.                       |
| **STEAM_CATEGORIES**  | Game categories such as "Single-player", "Multiplayer", etc.                       |
| **STEAM_GENRES**      | Game genres such as "Action", "Adventure", "Strategy", etc.                        |
| **STEAM_NUM_PLAYERS** | Historical player count data with record date and number of players.               |

### How to Use

#### Copy or Download Database
(Optional) Download the pre-collected Steam database and place it into the folder:

```r
steam_data/
```

#### Update Game Information

```r
cd steam_data
Rscript update_steam_app.R
```

#### Update Today Number of Players:

Single Game
```r
Rscript update_steam_num_players.R <appid>
# Example:
Rscript update_steam_num_players.R 150
```

All games
```r
Rscript update_steam_num_players.R
```


Page for collecting data: https://archive.ics.uci.edu/

https://disease.sh/docs/#/COVID-19%3A%20JHUCSSE/get_v3_covid_19_historical


No history number of players => require run every day to get it


https://steamspy.com/api.php

# history of number players
https://steamcharts.com/app/570/chart-data.json


# current number of players:
https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=570

# game information
https://store.steampowered.com/api/appdetails?appids=570


https://disease.sh/docs/#/COVID-19%3A%20JHUCSSE/get_v3_covid_19_historical
https://dbdiagram.io/d/68f774832e68d21b41824dde
https://dbdiagram.io/d/68f774832e68d21b41824dde